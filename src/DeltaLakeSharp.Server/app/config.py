# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
"""Configuration constants for DeltaLakeSharp server backends."""

# Arrow Flight server settings
DEFAULT_FLIGHT_PORT = 8815
DEFAULT_FLIGHT_HOST = "0.0.0.0"

# Spark settings
SPARK_APP_NAME = "DeltaLakeSharp.V1"
SPARK_MASTER = "local[*]"
SPARK_LOG_LEVEL = "WARN"

# Delta Lake Spark extensions
SPARK_SQL_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension"
SPARK_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

# DFS host suffixes — Hadoop config keys must include the *full* hostname that
# appears in the ABFSS URI's authority component.
# Standard Azure Storage: <account>.dfs.core.windows.net
# Microsoft Fabric / OneLake: <account>.dfs.fabric.microsoft.com
DFS_HOST_AZURE = "dfs.core.windows.net"
DFS_HOST_ONELAKE = "dfs.fabric.microsoft.com"

# ABFSS SAS token auth configuration keys — templated by storage account *and*
# DFS host suffix so they match the hostname that Hadoop extracts from the
# abfss:// URI.  Example key for OneLake MSIT:
#   fs.azure.account.auth.type.msit-onelake.dfs.fabric.microsoft.com
ABFSS_AUTH_TYPE_KEY = "fs.azure.account.auth.type.{storage_account}.{dfs_host}"
ABFSS_SAS_TOKEN_KEY = "fs.azure.sas.fixed.token.{storage_account}.{dfs_host}"

ABFSS_AUTH_TYPE_VALUE = "SAS"

# Hadoop ABFSS HNS (Hierarchical Namespace) configuration key.
# When set to "true", the ABFSS driver skips the auto-detection probe
# (getAclStatus on the filesystem root) that would otherwise require
# authorization beyond a directory-scoped SAS token.
ABFSS_HNS_ENABLED_KEY = "fs.azure.account.hns.enabled.{storage_account}.{dfs_host}"
