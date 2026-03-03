# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
"""V2 backend: DataFusion + delta-rs with Arrow Flight transport.

This module provides the V2 implementation of the Delta Table Service,
using DataFusion (via delta-rs Python bindings) for Delta Lake operations
and Arrow Flight for data transport. No JVM required.

Exports:
    - DeltaFlightServerV2: The Arrow Flight server class.
    - datafusion_operations: Module containing Delta table operations via DataFusion.
"""

from app.v2 import datafusion_operations
from app.v2.flight_server import DeltaFlightServerV2

__all__ = [
    "DeltaFlightServerV2",
    "datafusion_operations",
]
