"""Delta table operations using DataFusion (Rust engine) and delta-rs.

This module provides the same logical operations as ``delta_operations.py``
(the V1 Spark-based backend) but uses the lightweight DataFusion query
engine and the ``deltalake`` (delta-rs) Python package.  No JVM or Spark
dependency is required.

Known Limitations (delta-rs 1.4.x):
    Column mapping (delta.columnMapping.mode):
        - WRITE: The ``configuration`` parameter is accepted and stored in
          table metadata, but column mapping is NOT implemented. The protocol
          remains at (1, 2) and no column mapping annotations (physical names,
          column IDs) are added to the schema. Tables created this way are
          standard Delta tables, not true column-mapped tables.
        - READ: delta-rs cannot read tables with column mapping created by
          Spark. Fails with "minimum reader version is 2 but deltalake only
          supports version 1 or 3 with reader features: {'timestampNtz'}"
          or "reader features: {'columnMapping'} not yet supported".

    Deletion vectors (delta.enableDeletionVectors):
        - WRITE: Configuration is stored, protocol upgraded to (3, 7) with
          ``deletionVectors`` reader/writer feature, but deletion vectors are
          NOT implemented. DELETE operations still use copy-on-write (rewrite
          entire parquet files). No ``.bin`` DV files are created, and no
          ``deletionVector`` field appears in transaction log add actions.
        - READ: After any DELETE on a DV-configured table, delta-rs cannot
          read it because the protocol declares ``deletionVectors`` feature
          but the reader doesn't support it. Fails with "reader features:
          {'deletionVectors'} not yet supported by the deltalake reader".
        - WARNING: This is worse than column mapping! The config makes tables
          unreadable after DELETE without providing any benefit. Do NOT use
          ``delta.enableDeletionVectors=true`` with V2 backend.

    For full Delta Lake feature support, use V1 (PySpark) backend.
"""

from __future__ import annotations

import logging
from typing import Any

import pyarrow as pa
from datafusion import SessionContext
from deltalake import DeltaTable, write_deltalake
from deltalake._internal import TableFeatures

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  Arrow type mapping (simple name → standard PyArrow type)
# ------------------------------------------------------------------ #

_ARROW_TYPE_MAP: dict[str, pa.DataType] = {
    "string": pa.utf8(),
    "utf8": pa.utf8(),
    "int": pa.int32(),
    "int32": pa.int32(),
    "integer": pa.int32(),
    "long": pa.int64(),
    "int64": pa.int64(),
    "bigint": pa.int64(),
    "short": pa.int16(),
    "int16": pa.int16(),
    "smallint": pa.int16(),
    "byte": pa.int8(),
    "int8": pa.int8(),
    "tinyint": pa.int8(),
    "float": pa.float32(),
    "float32": pa.float32(),
    "double": pa.float64(),
    "float64": pa.float64(),
    "boolean": pa.bool_(),
    "bool": pa.bool_(),
    "date": pa.date32(),
    "date32": pa.date32(),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "timestamp_ntz": pa.timestamp("us"),
    "binary": pa.binary(),
}


def _resolve_storage(
    path: str,
    storage_account: str | None,
    sas_token: str | None,
) -> tuple[str, dict[str, str] | None]:
    """Resolve storage options **and** path for delta-rs Azure access.

    Returns ``(effective_path, storage_options)`` where *effective_path*
    may differ from the input *path* for non-production OneLake
    environments (see below).

    For Microsoft Fabric / OneLake the SAS token is always signed with
    account name ``"onelake"`` (canonicalized resource
    ``/blob/onelake/{workspace}/{path}`` regardless of environment).

    The Rust ``object_store`` crate's ``use_fabric_endpoint`` option
    constructs the endpoint as ``{account_name}.dfs.fabric.microsoft.com``
    and — critically — switches to DFS-compatible API operations.
    This works for **Production** (``account_name="onelake"``).

    For **non-production environments** (e.g. MSIT with DNS account
    ``"msit-onelake"``):

    * ``account_name`` must be ``"onelake"`` to match the SAS signing
      account.
    * ``use_fabric_endpoint`` enables DFS-compatible API operations.
    * An explicit ``endpoint`` overrides the hostname to the correct
      non-prod DFS endpoint.
    * The ``abfss://`` URI embeds the host, so the path is rewritten to
      ``az://`` scheme which does **not** carry a host component.

    **Known limitation (deltalake 1.4.x / object_store):**
    ``use_fabric_endpoint=true`` constructs the URL as
    ``{account_name}.dfs.fabric.microsoft.com``, *ignoring* any explicit
    ``endpoint`` option.  Since ``account_name`` must be ``"onelake"``
    (for SAS validation), this always routes to production.  There is
    currently no valid option combination in ``object_store`` that
    simultaneously routes to a non-prod endpoint, uses DFS-compatible
    API paths, and validates the SAS token.  V1 (Spark/Hadoop) does not
    have this limitation.
    """
    if not (storage_account and sas_token):
        return path, None

    lower = storage_account.lower()

    if "onelake" in lower:
        # OneLake (any environment — Production or MSIT).
        # SAS token is always signed against account "onelake".
        if lower == "onelake":
            # Production: use_fabric_endpoint builds
            # onelake.dfs.fabric.microsoft.com from account_name.
            opts: dict[str, str] = {
                "account_name": "onelake",
                "sas_token": sas_token,
                "use_fabric_endpoint": "true",
            }
        else:
            # Non-production (e.g. "msit-onelake"):
            endpoint = f"https://{storage_account}.dfs.fabric.microsoft.com"
            opts = {
                "account_name": "onelake",
                "sas_token": sas_token,
                "use_fabric_endpoint": "true",
                "endpoint": endpoint,
            }
            # Rewrite abfss:// → az:// so object_store uses `endpoint`
            # config instead of the host embedded in the URI.
            path = _abfss_to_az(path)
            logger.info(
                "OneLake non-prod: account_name=onelake, endpoint=%s, "
                "path=%s",
                endpoint, path,
            )
    else:
        # Regular Azure Storage (non-OneLake)
        opts = {
            "account_name": storage_account,
            "sas_token": sas_token,
        }

    logger.debug(
        "storage_options: %s",
        {k: (v[:20] + "...") if k == "sas_token" else v for k, v in opts.items()},
    )
    return path, opts


def _abfss_to_az(uri: str) -> str:
    """Convert ``abfss://container@host/path`` → ``az://container/path``.

    If the URI does not start with ``abfss://`` it is returned unchanged.
    """
    if not uri.lower().startswith("abfss://"):
        return uri
    # abfss://container@account.dfs.../rest/of/path
    without_scheme = uri[len("abfss://"):]
    at_idx = without_scheme.find("@")
    if at_idx < 0:
        return uri  # malformed — return as-is
    container = without_scheme[:at_idx]
    after_host = without_scheme[at_idx + 1:]
    # after_host = "account.dfs.fabric.microsoft.com/rest/of/path"
    slash_idx = after_host.find("/")
    if slash_idx < 0:
        return f"az://{container}"
    rest = after_host[slash_idx + 1:]
    return f"az://{container}/{rest}"


def _to_pyarrow_batch(batch) -> pa.RecordBatch:
    """Ensure *batch* is a standard ``pa.RecordBatch``.

    DataFusion's ``execute_stream()`` may yield its own RecordBatch
    wrapper objects that lack the ``.schema`` attribute.  If the object
    exposes ``to_pyarrow()`` we call it first; otherwise we assume it
    is already a native PyArrow RecordBatch.
    """
    if hasattr(batch, "to_pyarrow"):
        batch = batch.to_pyarrow()
    return batch


def _batches_to_rows(stream) -> list[dict[str, Any]]:
    """Convert an iterable of RecordBatches to a list-of-dicts.

    Works with both ``RecordBatchStream`` (from ``execute_stream()``)
    and a plain ``list[RecordBatch]``.
    """
    rows: list[dict[str, Any]] = []
    for batch in stream:
        batch = _to_pyarrow_batch(batch)
        tbl = pa.Table.from_batches([batch]) if isinstance(batch, pa.RecordBatch) else batch
        for row_dict in tbl.to_pylist():
            rows.append(row_dict)
    return rows


def _stream_to_reader(df, stream) -> pa.RecordBatchReader:
    """Consume a DataFusion execute stream and return a ``RecordBatchReader``.

    Peeks at the first batch to derive the schema, then wraps the
    remainder in a ``pa.RecordBatchReader`` that yields batches lazily.

    delta-rs natively produces ``large_utf8`` / ``large_binary`` Arrow
    types (64-bit offsets).  These are passed through unchanged — the C#
    ``ArrowConverter`` already handles ``LargeStringArray`` and
    ``LargeBinaryArray``.

    Args:
        df: The DataFusion ``DataFrame`` — used to derive the schema from
            the query plan when the result set is empty.
        stream: The iterator returned by ``df.execute_stream()``.
    """
    first_batch = next(stream, None)

    if first_batch is None:
        # Empty result — derive schema from the DataFusion plan (works
        # for both modes: plan-based schema handles SQL projections too).
        arrow_schema = df.schema()
        if hasattr(arrow_schema, "to_pyarrow"):
            arrow_schema = arrow_schema.to_pyarrow()
        return pa.RecordBatchReader.from_batches(arrow_schema, [])

    first_batch = _to_pyarrow_batch(first_batch)
    schema = first_batch.schema

    def _generate():
        yield first_batch
        for batch in stream:
            yield _to_pyarrow_batch(batch)

    return pa.RecordBatchReader.from_batches(schema, _generate())


# ------------------------------------------------------------------ #
#  Public operations
# ------------------------------------------------------------------ #


def health_check() -> dict[str, Any]:
    """Return health status with engine version info."""
    import datafusion as _df

    return {
        "status": "healthy",
        "engine": "datafusion",
        "datafusion_version": _df.__version__,
    }


def read_table(
    path: str | None = None,
    num_rows: int | None = None,
    storage_account: str | None = None,
    sas_token: str | None = None,
    *,
    sql: str | None = None,
    table_name: str | None = None,
) -> pa.RecordBatchReader:
    """Read a Delta table or execute SQL and return a ``RecordBatchReader``.

    Two modes of operation:

    1. **Read mode** (default) — *path* is required.  Registers the
       Delta table at *path* as ``_tbl`` and executes
       ``SELECT * FROM _tbl``.  When *num_rows* is provided, a ``LIMIT``
       clause is appended.

    2. **SQL mode** — when *sql* is provided, executes the caller-
       supplied SQL statement.  If *path* and *table_name* are also
       given, the Delta table is opened and registered under
       *table_name* before the query runs.  When *path* is ``None`` the
       query executes directly (useful for ``SELECT 1``, etc.).
       *num_rows* is ignored in SQL mode.

    Returns:
        A ``pa.RecordBatchReader`` whose ``.schema`` is the normalised
        (C#-compatible) Arrow schema.  Iterating the reader yields
        normalised ``pa.RecordBatch`` objects one at a time.
        **No ``pa.Table`` is ever materialised in memory.**  Callers can
        pass this directly to ``flight.RecordBatchStream`` for efficient
        GIL-free Flight serialization.
    """
    ctx = SessionContext()

    if sql is not None:
        # --- SQL mode ---------------------------------------------------
        if path is not None and table_name is not None:
            path, opts = _resolve_storage(path, storage_account, sas_token)
            logger.info(
                "read_table (SQL mode): table=%s as '%s', sql=%s",
                path,
                table_name,
                sql[:200],
            )
            dt = DeltaTable(path, storage_options=opts)
            ctx.register_dataset(table_name, dt.to_pyarrow_dataset())
        else:
            logger.info("read_table (SQL mode, no table): sql=%s", sql[:200])
        df = ctx.sql(sql)
    else:
        # --- Read mode ---------------------------------------------------
        if path is None:
            raise ValueError("path is required in read mode (no sql provided)")
        path, opts = _resolve_storage(path, storage_account, sas_token)
        dt = DeltaTable(path, storage_options=opts)
        ctx.register_dataset("_tbl", dt.to_pyarrow_dataset())
        if num_rows is not None:
            logger.info("read_table (read mode): %s (limit=%d)", path, num_rows)
            df = ctx.sql(f"SELECT * FROM _tbl LIMIT {int(num_rows)}")
        else:
            logger.info("read_table (read mode): %s (all rows)", path)
            df = ctx.sql("SELECT * FROM _tbl")

    stream = df.execute_stream()
    reader = _stream_to_reader(df, stream)
    logger.info(
        "read_table: streaming results (schema: %d columns)", len(reader.schema)
    )
    return reader


def get_schema(
    path: str,
    storage_account: str | None = None,
    sas_token: str | None = None,
) -> pa.Schema:
    """Return the Arrow schema of a Delta table.

    Reads the schema directly from the Delta log via delta-rs — no data
    is scanned.
    """
    path, opts = _resolve_storage(path, storage_account, sas_token)
    logger.info("Getting schema for Delta table at: %s", path)

    dt = DeltaTable(path, storage_options=opts)
    schema = dt.to_pyarrow_dataset().schema

    logger.info(
        "Schema for %s: %s",
        path,
        [(f.name, str(f.type)) for f in schema],
    )
    return schema


def create_table(
    path: str,
    schema_fields: list[dict[str, str]],
    storage_account: str | None = None,
    sas_token: str | None = None,
    configuration: dict[str, str] | None = None,
    partition_by: list[str] | None = None,
) -> dict[str, Any]:
    """Create an empty Delta table with the given schema.

    Uses ``write_deltalake`` with an empty ``pyarrow.Table``.

    Args:
        path: The path to the Delta table.
        schema_fields: List of field definitions with 'name' and 'type'.
        storage_account: Optional Azure storage account name.
        sas_token: Optional SAS token for Azure storage.
        configuration: Optional Delta table configuration properties,
            e.g. {"delta.columnMapping.mode": "name"}.
        partition_by: Optional list of column names to partition by.

    Note:
        The ``configuration`` parameter is stored in Delta table metadata
        but advanced features like column mapping are NOT implemented by
        delta-rs. See module docstring for known limitations.
    """
    path, opts = _resolve_storage(path, storage_account, sas_token)
    logger.info("Creating empty Delta table at: %s (config=%s, partition_by=%s)", path, configuration, partition_by)
    try:
        fields = []
        for f in schema_fields:
            arrow_type = _ARROW_TYPE_MAP.get(f["type"].lower(), pa.utf8())
            fields.append(pa.field(f["name"], arrow_type, nullable=True))

        schema = pa.schema(fields)
        empty_table = pa.table(
            {f.name: pa.array([], type=f.type) for f in schema},
        )

        write_deltalake(
            path,
            empty_table,
            mode="overwrite",
            storage_options=opts,
            configuration=configuration,
            partition_by=partition_by or None,
        )

        return {
            "success": True,
            "message": f"Delta table created at {path}.",
        }
    except Exception as e:
        logger.error("Failed to create Delta table: %s", e)
        return {
            "success": False,
            "message": str(e),
        }


def write_arrow_batches(
    path: str,
    batch_reader: pa.RecordBatchReader,
    mode: str = "overwrite",
    storage_account: str | None = None,
    sas_token: str | None = None,
    configuration: dict[str, str] | None = None,
    partition_by: list[str] | None = None,
) -> dict[str, Any]:
    """Write Arrow RecordBatches as a Delta table — streaming, no full materialisation.

    Accepts a ``pa.RecordBatchReader`` (an iterator of ``pa.RecordBatch``
    objects) and passes it directly to ``write_deltalake``.  The data is
    never fully materialised into a single ``pa.Table`` in memory;
    delta-rs consumes the batches one at a time.

    The Flight server's DoPut path reads incoming batches from the
    client, applies per-batch IPC buffer alignment, wraps them in a
    ``RecordBatchReader``, and hands the reader to this function.

    Args:
        path: The path to the Delta table.
        batch_reader: A ``pa.RecordBatchReader`` yielding the data to
            write.  Can be created via
            ``pa.RecordBatchReader.from_batches(schema, generator)``.
        mode: Write mode ('overwrite' or 'append').
        storage_account: Optional Azure storage account name.
        sas_token: Optional SAS token for Azure storage.
        configuration: Optional Delta table configuration properties.
        partition_by: Optional list of column names to partition by.
    """
    path, opts = _resolve_storage(path, storage_account, sas_token)
    logger.info(
        "Writing batches (streaming) to Delta table at: %s (mode=%s, config=%s)",
        path,
        mode,
        configuration,
    )
    try:
        write_deltalake(
            path,
            batch_reader,
            mode=mode,
            storage_options=opts,
            configuration=configuration,
            partition_by=partition_by or None,
        )
        return {
            "success": True,
            "message": f"Wrote batches to {path}.",
        }
    except Exception as e:
        logger.error("Failed to write Delta table (streaming): %s", e)
        return {
            "success": False,
            "message": str(e),
        }


def execute_dml(
    sql: str,
    table_path: str,
    table_name: str,
    storage_account: str | None = None,
    sas_token: str | None = None,
) -> dict[str, Any]:
    """Execute a DML statement (DELETE, UPDATE, MERGE) against a Delta table.

    This function:

    1. Opens the Delta table at *table_path* via delta-rs.
    2. Creates a DataFusion ``SessionContext`` and registers the table
       under *table_name*.
    3. Executes the DML SQL statement.

    Known limitations (delta-rs / DataFusion):
        - DELETE works via copy-on-write (full parquet file rewrite).
        - UPDATE and MERGE are NOT supported by DataFusion's delta-rs
          integration and will raise errors.
        - If deletion vectors are enabled on the table, DELETE will break
          the table (protocol declares DV support but delta-rs doesn't
          implement it).

    Args:
        sql: The DML SQL statement (DELETE, UPDATE, or MERGE).
        table_path: Physical path to the Delta table.
        table_name: Logical table name referenced in *sql*.
        storage_account: Optional Azure storage account name.
        sas_token: Optional SAS token for Azure storage.

    Returns:
        A dict with 'success' (bool), 'message' (str), and optionally
        'result' (list of row dicts).
    """
    table_path, opts = _resolve_storage(table_path, storage_account, sas_token)
    logger.info(
        "Executing DML: table_path=%s, table_name=%s, sql=%s",
        table_path,
        table_name,
        sql[:200],
    )
    try:
        # Open the Delta table and register it in a DataFusion context.
        dt = DeltaTable(table_path, storage_options=opts)
        ctx = SessionContext()
        ctx.register_dataset(table_name, dt.to_pyarrow_dataset())

        # Execute the DML statement.
        df = ctx.sql(sql)
        stream = df.execute_stream()
        rows = _batches_to_rows(stream)

        return {
            "success": True,
            "message": "DML executed successfully.",
            "result": rows,
        }
    except Exception as e:
        logger.error("DML execution failed: %s", e)
        return {
            "success": False,
            "message": str(e),
        }


def merge_arrow_stream(
    path: str,
    batch_reader: pa.RecordBatchReader,
    cmd: dict[str, Any],
    storage_account: str | None = None,
    sas_token: str | None = None,
) -> dict[str, Any]:
    """Merge streamed Arrow data into an existing Delta table.

    Uses the delta-rs programmatic ``DeltaTable.merge()`` API with
    ``streamed_exec=True`` (the default) so that the source data is
    consumed lazily from the ``RecordBatchReader`` — **no full
    materialisation** into a ``pa.Table`` is required.

    The merge behaviour is controlled by keys in *cmd*:

    =============================================  ===============================
    Key                                            Effect
    =============================================  ===============================
    ``predicate`` *(required)*                     Join predicate, e.g.
                                                   ``"target.id = source.id"``
    ``source_alias``                               Alias for source (default
                                                   ``"source"``)
    ``target_alias``                               Alias for target (default
                                                   ``"target"``)
    ``when_matched_update_all``                    ``True`` → update all columns
    ``when_matched_update_set``                    ``dict`` of column assignments
    ``when_matched_delete_predicate``              Predicate string for matched
                                                   delete
    ``when_not_matched_insert_all``                ``True`` → insert all columns
    ``when_not_matched_insert_set``                ``dict`` of column assignments
    ``when_not_matched_by_source_delete_predicate``  Predicate string
    ``when_not_matched_by_source_update_set``      ``dict`` of column assignments
    ``when_not_matched_by_source_update_predicate``  Predicate string (required
                                                   with above)
    =============================================  ===============================

    Args:
        path: The path to the target Delta table.
        batch_reader: A ``pa.RecordBatchReader`` yielding the source data
            to merge.
        cmd: Parsed command dict with merge parameters (see table above).
        storage_account: Optional Azure storage account name.
        sas_token: Optional SAS token for Azure storage.

    Returns:
        A dict with ``success``, ``message``, and ``result`` (list
        containing the merge metrics dict from delta-rs).
    """
    path, opts = _resolve_storage(path, storage_account, sas_token)
    predicate = cmd["predicate"]
    source_alias = cmd.get("source_alias", "source")
    target_alias = cmd.get("target_alias", "target")

    logger.info(
        "Merging streamed data into Delta table at: %s "
        "(predicate=%s, source_alias=%s, target_alias=%s)",
        path, predicate, source_alias, target_alias,
    )
    try:
        dt = DeltaTable(path, storage_options=opts)

        merger = dt.merge(
            source=batch_reader,
            predicate=predicate,
            source_alias=source_alias,
            target_alias=target_alias,
        )

        # -- WHEN MATCHED clauses ----------------------------------------
        if cmd.get("when_matched_update_all"):
            merger = merger.when_matched_update_all()
        elif cmd.get("when_matched_update_set"):
            merger = merger.when_matched_update(
                updates=cmd["when_matched_update_set"],
            )

        if cmd.get("when_matched_delete_predicate"):
            merger = merger.when_matched_delete(
                predicate=cmd["when_matched_delete_predicate"],
            )

        # -- WHEN NOT MATCHED clauses ------------------------------------
        if cmd.get("when_not_matched_insert_all"):
            merger = merger.when_not_matched_insert_all()
        elif cmd.get("when_not_matched_insert_set"):
            merger = merger.when_not_matched_insert(
                updates=cmd["when_not_matched_insert_set"],
            )

        # -- WHEN NOT MATCHED BY SOURCE clauses --------------------------
        if cmd.get("when_not_matched_by_source_delete_predicate"):
            merger = merger.when_not_matched_by_source_delete(
                predicate=cmd["when_not_matched_by_source_delete_predicate"],
            )

        if cmd.get("when_not_matched_by_source_update_set"):
            merger = merger.when_not_matched_by_source_update(
                updates=cmd["when_not_matched_by_source_update_set"],
                predicate=cmd.get(
                    "when_not_matched_by_source_update_predicate"
                ),
            )

        metrics = merger.execute()

        logger.info("Merge completed: %s", metrics)
        return {
            "success": True,
            "message": "Merge completed.",
            "result": [metrics],
        }
    except Exception as e:
        logger.error("Merge failed: %s", e)
        return {
            "success": False,
            "message": str(e),
        }


# Mapping from Delta protocol feature name strings (camelCase) to the
# ``TableFeatures`` enum used by ``DeltaTable.alter.add_feature()``.
# Feature names come from the gRPC command JSON (e.g. "appendOnly",
# "timestampNtz") and must be resolved to enum values for delta-rs.
_TABLE_FEATURE_MAP: dict[str, TableFeatures] = {
    "appendOnly": TableFeatures.AppendOnly,
    "changeDataFeed": TableFeatures.ChangeDataFeed,
    "checkConstraints": TableFeatures.CheckConstraints,
    "columnMapping": TableFeatures.ColumnMapping,
    "deletionVectors": TableFeatures.DeletionVectors,
    "domainMetadata": TableFeatures.DomainMetadata,
    "generatedColumns": TableFeatures.GeneratedColumns,
    "icebergCompatV1": TableFeatures.IcebergCompatV1,
    "identityColumns": TableFeatures.IdentityColumns,
    "invariants": TableFeatures.Invariants,
    "rowTracking": TableFeatures.RowTracking,
    "timestampNtz": TableFeatures.TimestampWithoutTimezone,
    "v2Checkpoint": TableFeatures.V2Checkpoint,
}

# Mapping from Delta protocol feature names to the table property that
# *activates* the feature.  Features listed here have a companion property
# that must be set via ``set_table_properties()`` — merely calling
# ``add_feature()`` marks the feature as *supported* in the protocol but
# does NOT make it *active*.
#
# Features NOT in this map (e.g. ``timestampNtz``, ``invariants``,
# ``domainMetadata``) have no companion property and are correctly enabled
# via ``add_feature()`` alone.
_FEATURE_COMPANION_PROPS: dict[str, dict[str, str]] = {
    "appendOnly":      {"delta.appendOnly": "true"},
    "changeDataFeed":  {"delta.enableChangeDataFeed": "true"},
    "columnMapping":   {"delta.columnMapping.mode": "name"},
    "deletionVectors": {"delta.enableDeletionVectors": "true"},
    "rowTracking":     {"delta.enableRowTracking": "true"},
}


def upgrade_table_protocol(
    path: str,
    reader_version: int,
    writer_version: int,
    reader_features: list[str] | None = None,
    writer_features: list[str] | None = None,
    storage_account: str | None = None,
    sas_token: str | None = None,
) -> dict[str, Any]:
    """Upgrade the Delta table protocol version using delta-rs.

    Version-only upgrades use ``DeltaTable.alter.set_table_properties()``
    with ``delta.minReaderVersion`` / ``delta.minWriterVersion``.

    When individual table features are requested, they are first
    registered at the protocol level via
    ``DeltaTable.alter.add_feature()`` with
    ``allow_protocol_versions_increase=True`` so that delta-rs
    automatically bumps the protocol to the required minimum (reader 3 /
    writer 7).  Features that have a companion property (e.g.
    ``delta.enableChangeDataFeed`` for *changeDataFeed*,
    ``delta.columnMapping.mode`` for *columnMapping*) are then
    *activated* via a follow-up ``set_table_properties()`` call —
    ``add_feature()`` alone only marks them as *supported*, not *active*.

    If the caller also requested a *higher* reader version than the
    auto-bumped value, a follow-up ``set_table_properties`` call is made.

    After the upgrade the current protocol is read back via
    ``DeltaTable.protocol()`` and returned in the result.

    Args:
        path: Physical path to the Delta table (local or abfss://).
        reader_version: Target minimum reader version (>= 1).
        writer_version: Target minimum writer version (>= 1).
        reader_features: Optional list of reader features to enable
            (camelCase Delta protocol names, e.g. ``"timestampNtz"``).
        writer_features: Optional list of writer features to enable
            (camelCase Delta protocol names, e.g. ``"appendOnly"``).
        storage_account: Optional Azure storage account name.
        sas_token: Optional SAS token for Azure storage.

    Returns:
        A dict with 'success', 'message', and 'result' containing the
        protocol versions and features after upgrade.
    """
    path, opts = _resolve_storage(path, storage_account, sas_token)
    logger.info(
        "Upgrading table protocol: path=%s, reader=%d, writer=%d, "
        "reader_features=%s, writer_features=%s",
        path, reader_version, writer_version,
        reader_features, writer_features,
    )
    try:
        dt = DeltaTable(path, storage_options=opts)

        # Collect all requested features (reader + writer deduplicated).
        all_features: list[TableFeatures] = []
        for name in (reader_features or []) + (writer_features or []):
            feat = _TABLE_FEATURE_MAP.get(name)
            if feat is None:
                return {
                    "success": False,
                    "message": f"Unknown table feature: '{name}'",
                }
            if feat not in all_features:
                all_features.append(feat)

        if all_features:
            # add_feature() automatically bumps protocol to the minimum
            # required versions (reader 3 for reader features, writer 7
            # for any features).
            dt.alter.add_feature(
                all_features,
                allow_protocol_versions_increase=True,
            )

            # Activate features that have companion properties.
            # add_feature() only marks the feature as *supported* at the
            # protocol level; the companion property is what actually
            # makes the feature *active* in the table.
            companion_props: dict[str, str] = {}
            seen: set[str] = set()
            for name in (reader_features or []) + (writer_features or []):
                if name in seen:
                    continue
                seen.add(name)
                props = _FEATURE_COMPANION_PROPS.get(name)
                if props:
                    companion_props.update(props)
            if companion_props:
                dt = DeltaTable(path, storage_options=opts)
                dt.alter.set_table_properties(companion_props)

            # Reload to pick up the new protocol state.
            dt = DeltaTable(path, storage_options=opts)

            # If the caller requested a higher reader version than what
            # add_feature set, bump it.  (Writer is already at 7 which
            # is the maximum for delta-rs.)
            current_proto = dt.protocol()
            if reader_version > current_proto.min_reader_version:
                dt.alter.set_table_properties(
                    {"delta.minReaderVersion": str(reader_version)}
                )
                dt = DeltaTable(path, storage_options=opts)
        else:
            # No features requested — simple version bump.
            dt.alter.set_table_properties({
                "delta.minReaderVersion": str(reader_version),
                "delta.minWriterVersion": str(writer_version),
            })
            dt = DeltaTable(path, storage_options=opts)

        # Read back the current protocol.
        proto = dt.protocol()
        result_info: dict[str, Any] = {
            "minReaderVersion": proto.min_reader_version,
            "minWriterVersion": proto.min_writer_version,
        }
        if proto.reader_features is not None:
            result_info["readerFeatures"] = list(proto.reader_features)
        if proto.writer_features is not None:
            result_info["writerFeatures"] = list(proto.writer_features)

        # configuration — table metadata properties.
        metadata = dt.metadata()
        if metadata.configuration:
            result_info["metadata.configuration"] = dict(metadata.configuration)

        logger.info("Protocol upgraded for table at: %s", path)
        return {
            "success": True,
            "message": "Protocol upgraded.",
            "result": [result_info],
        }
    except Exception as e:
        logger.error("Protocol upgrade failed: %s", e)
        return {
            "success": False,
            "message": str(e),
        }
