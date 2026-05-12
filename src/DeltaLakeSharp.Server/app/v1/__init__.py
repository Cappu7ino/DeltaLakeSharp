# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
"""V1 backend: PySpark + Delta Lake with Arrow Flight transport.

This module provides the V1 implementation of the Delta Table Service,
using PySpark for Delta Lake operations and Arrow Flight for data transport.

Exports:
    - DeltaFlightServer: The Arrow Flight server class.
    - SparkManager: Manages the Spark session lifecycle.
    - delta_operations: Module containing Delta table operations.
"""

from app.v1 import delta_operations
from app.v1.flight_server import DeltaFlightServer
from app.v1.spark_manager import SparkManager

__all__ = [
    "DeltaFlightServer",
    "SparkManager",
    "delta_operations",
]
