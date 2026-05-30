"""SparkSession lifecycle management with Delta Lake and SAS token support."""

import logging
from typing import Optional

from pyspark.sql import SparkSession

from app.config import (
    SPARK_APP_NAME,
    SPARK_MASTER,
    SPARK_LOG_LEVEL,
    SPARK_SQL_EXTENSIONS,
    SPARK_CATALOG,
    DFS_HOST_AZURE,
    DFS_HOST_ONELAKE,
    ABFSS_AUTH_TYPE_KEY,
    ABFSS_SAS_TOKEN_KEY,
    ABFSS_AUTH_TYPE_VALUE,
    ABFSS_HNS_ENABLED_KEY,
)

logger = logging.getLogger(__name__)


class SparkManager:
    """Manages a singleton SparkSession configured for Delta Lake operations."""

    def __init__(self) -> None:
        self._spark = None  # type: Optional[SparkSession]

    @property
    def spark(self) -> SparkSession:
        """Returns the active SparkSession, creating it if necessary."""
        if self._spark is None:
            self._spark = self._create_session()
        return self._spark

    def configure_storage(
        self,
        storage_account: str,
        sas_token: str,
        container: Optional[str] = None,
        evict_fs_cache: bool = True,
    ) -> None:
        """Configure ABFSS SAS token authentication for a given storage account.

        This sets Hadoop configuration on the active SparkSession so that
        subsequent reads/writes to
        ``abfss://<container>@<storage_account>.<dfs_host>/...``
        are authenticated via the provided SAS token.

        When the *storage_account* name contains ``"onelake"``
        (case-insensitive) the DFS host suffix is automatically set to
        ``dfs.fabric.microsoft.com`` (Microsoft Fabric / OneLake).
        Otherwise the standard Azure Storage suffix
        ``dfs.core.windows.net`` is used.

        Args:
            storage_account: The storage account name (e.g. 'mystorageacct'
                or 'msit-onelake' for Fabric OneLake).
            sas_token: The SAS token string (including the leading '?').
            container: Optional ABFSS container name (e.g. workspace GUID
                for OneLake).  When provided, the cached Hadoop FileSystem
                instance for ``abfss://{container}@{authority}/`` is evicted
                so the new SAS token takes effect.  Without this, a stale
                cached instance (keyed by a different container) would
                continue using the old SAS token.
            evict_fs_cache: Whether to evict the cached Hadoop FileSystem
                instance before reconfiguring.  Defaults to ``True``
                (current behaviour preserved).  Set to ``False`` when the
                same table is read repeatedly with unchanged credentials
                (e.g. benchmarks) to avoid the JVM overhead of tearing
                down and recreating the FileSystem instance.
        """
        # Choose the DFS host suffix that matches the ABFSS URI authority.
        dfs_host = (
            DFS_HOST_ONELAKE
            if "onelake" in storage_account.lower()
            else DFS_HOST_AZURE
        )

        spark = self.spark
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

        # Evict any cached AzureBlobFileSystem instance for this storage
        # account so the new SAS token takes effect immediately.  Hadoop's
        # FileSystem.get() caches instances by scheme + authority; for ABFSS
        # the authority is "{container}@{account}.{dfs_host}".  If an
        # earlier operation already created one (e.g. a read test with a
        # different SAS scope), the cached instance still uses the old
        # SAS token provider and will ignore any hadoop_conf changes.
        #
        # The *container* parameter (e.g. the OneLake workspace GUID) is
        # required to construct the correct cache key.  Without it the
        # eviction URI would not match and the stale instance would persist.
        #
        # When *evict_fs_cache* is False the eviction is skipped entirely.
        # This is useful for benchmarks where the same table is read
        # repeatedly with unchanged credentials — the JVM overhead of
        # tearing down and recreating the FileSystem instance is avoided.
        if container and evict_fs_cache:
            jvm = spark.sparkContext._jvm
            try:
                authority = f"{storage_account}.{dfs_host}"
                fs_uri = jvm.java.net.URI(
                    f"abfss://{container}@{authority}/"
                )
                cached_fs = jvm.org.apache.hadoop.fs.FileSystem.get(
                    fs_uri, hadoop_conf
                )
                cached_fs.close()
                logger.info(
                    "Evicted cached FileSystem for abfss://%s@%s/",
                    container,
                    authority,
                )
            except Exception:
                # No cached instance or close failed — safe to ignore;
                # the next FileSystem.get() will create a fresh one.
                logger.debug(
                    "No cached FileSystem to evict for abfss://%s@%s.%s/",
                    container,
                    storage_account,
                    dfs_host,
                    exc_info=True,
                )

        hadoop_conf.set(
            ABFSS_AUTH_TYPE_KEY.format(
                storage_account=storage_account, dfs_host=dfs_host
            ),
            ABFSS_AUTH_TYPE_VALUE,
        )
        hadoop_conf.set(
            ABFSS_SAS_TOKEN_KEY.format(
                storage_account=storage_account, dfs_host=dfs_host
            ),
            sas_token,
        )

        # Tell the ABFSS driver that the storage account supports HNS
        # (Hierarchical Namespace) so it skips the auto-detection probe.
        # Without this, Spark calls getAclStatus on the filesystem root
        # during write operations, which a directory-scoped SAS cannot
        # authorize — causing 401/500 errors on OneLake write paths.
        hadoop_conf.set(
            ABFSS_HNS_ENABLED_KEY.format(
                storage_account=storage_account, dfs_host=dfs_host
            ),
            "true",
        )

        # Limit ABFSS I/O retries so that 401/500 errors surface quickly
        # instead of causing the request to hang with exponential backoff.
        hadoop_conf.set("fs.azure.io.retry.max.retries", "1")

        logger.info(
            "Configured SAS token auth for storage account: %s (dfs_host: %s)",
            storage_account,
            dfs_host,
        )

    def stop(self) -> None:
        """Stop the SparkSession and release resources."""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
            logger.info("SparkSession stopped.")

    def _create_session(self) -> SparkSession:
        """Create a new SparkSession with Delta Lake extensions."""
        logger.info("Creating SparkSession with Delta Lake extensions...")

        spark = (
            SparkSession.builder.appName(SPARK_APP_NAME)
            .master(SPARK_MASTER)
            .config("spark.sql.extensions", SPARK_SQL_EXTENSIONS)
            .config("spark.sql.catalog.spark_catalog", SPARK_CATALOG)
            .config(
                "spark.sql.execution.arrow.pyspark.enabled", "true"
            )
            # Pin session timezone to UTC so that Spark's TimestampType
            # (tz-aware) consistently interprets and emits UTC values
            # regardless of the JVM's default system timezone.
            .config("spark.sql.session.timeZone", "UTC")
            # Suppress .crc sidecar files on local filesystem writes.
            #
            # Hadoop has two filesystem APIs that resolve implementations
            # independently:
            #   1. FileSystem API (used by Spark's data writers)
            #      - controlled by fs.file.impl
            #      - default: LocalFileSystem (wraps RawLocalFileSystem
            #        in ChecksumFileSystem → produces .crc files)
            #   2. FileContext API (used by Delta Lake's HDFSLogStore
            #      for writing _delta_log/ commit files)
            #      - controlled by fs.AbstractFileSystem.file.impl
            #      - default: LocalFs (wraps ChecksumFs → produces .crc)
            #
            # Both must be overridden to fully eliminate .crc files.
            # This only affects the file:// scheme — ABFSS and other
            # cloud filesystem writes are completely unaffected.
            .config(
                "spark.hadoop.fs.file.impl",
                "org.apache.hadoop.fs.RawLocalFileSystem",
            )
            .config(
                "spark.hadoop.fs.AbstractFileSystem.file.impl",
                "org.apache.hadoop.fs.local.RawLocalFs",
            )
            # --- OneLake / ABFSS write-path fixes ---
            # The Hadoop ABFSS driver probes the filesystem root via
            # getAclStatus to auto-detect HNS (Hierarchical Namespace)
            # support the first time a FileSystem instance is created.
            # With a directory-scoped SAS token the probe fails because
            # the token cannot authorise access to the workspace root.
            #
            # Setting these properties at session-creation time ensures
            # they are present *before* any AzureBlobFileSystem instance
            # is cached by Hadoop's FileSystem.get().  Once cached, later
            # hadoop_conf.set() calls in configure_storage() do NOT
            # affect the already-initialised instance.
            #
            # 1. Tell the driver every ABFSS account has HNS enabled so
            #    it never needs to probe.  (OneLake always has HNS;
            #    local file:// paths are unaffected.)
            .config(
                "spark.hadoop.fs.azure.account.hns.enabled", "true"
            )
            # 2. Disable the up-front access check that also triggers an
            #    authorisation call outside the SAS-scoped directory.
            .config(
                "spark.hadoop.fs.azure.enable.check.access", "false"
            )
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
        logger.info("SparkSession created successfully.")
        return spark
