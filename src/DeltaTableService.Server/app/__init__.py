# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
"""Delta Table Service - Arrow Flight server backed by PySpark and Delta Lake.

This package provides shared utilities and configuration for the Delta Table
Service backends (V1, V2).

Exports:
    - parse_json: Decode bytes to a JSON dict.
    - to_bytes: Encode a Python object as JSON bytes.
    - Configuration constants from config module.
"""

from app.config import (
    ABFSS_AUTH_TYPE_KEY,
    ABFSS_AUTH_TYPE_VALUE,
    ABFSS_SAS_TOKEN_KEY,
    DEFAULT_FLIGHT_HOST,
    DEFAULT_FLIGHT_PORT,
    SPARK_APP_NAME,
    SPARK_CATALOG,
    SPARK_LOG_LEVEL,
    SPARK_MASTER,
    SPARK_SQL_EXTENSIONS,
)
from app.utils import parse_json, to_bytes

__all__ = [
    # Utilities
    "parse_json",
    "to_bytes",
    # Configuration
    "DEFAULT_FLIGHT_PORT",
    "DEFAULT_FLIGHT_HOST",
    "SPARK_APP_NAME",
    "SPARK_MASTER",
    "SPARK_LOG_LEVEL",
    "SPARK_SQL_EXTENSIONS",
    "SPARK_CATALOG",
    "ABFSS_AUTH_TYPE_KEY",
    "ABFSS_SAS_TOKEN_KEY",
    "ABFSS_AUTH_TYPE_VALUE",
]
