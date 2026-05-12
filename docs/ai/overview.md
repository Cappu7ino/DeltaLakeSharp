# DeltaTableService AI Overview

## Repository Purpose

DeltaTableService is a .NET SDK repository for interacting with Delta Lake tables from C# consumers. The primary deliverables are NuGet packages that expose:

- `Microsoft.DI.DeltaTableService.Client`: a high-level Delta table client for reads, SQL queries, writes, DML, schema operations, CDF, partitioned reads, and native Rust execution.
- `Microsoft.DI.DeltaTableService.Adbc`: a read-only ADBC driver that exposes one Delta table through Arrow-native APIs.
- `Microsoft.DI.DeltaTableService.Testing`: test infrastructure used by the repository's integration tests.

The repository also contains backend implementations used by the client:

- V1 Spark backend: PySpark/Delta Lake service over Arrow Flight.
- V2 DataFusion backend: Python DataFusion/delta-rs service over Arrow Flight.
- V3 Rust backend: in-process native Rust engine exposed through C ABI and Arrow C Data/C Stream interfaces.

V3 Rust is the preferred and de-facto runtime for external client SDK consumption and the required runtime for the ADBC offering. V1 Spark and V2 DataFusion remain public `ServiceMode` values for service-backed compatibility, but they are primarily exercised through the testing harness and integration infrastructure rather than being the recommended external NuGet consumption model.

## Core Abstractions

| Abstraction | Role | Source |
| --- | --- | --- |
| `DeltaTableServiceClient` | Primary SDK entry point and backend selector. | [../../src/DeltaTableService.Client/DeltaTableServiceClient.cs](../../src/DeltaTableService.Client/DeltaTableServiceClient.cs) |
| `ServiceMode` | Selects V1 Spark, V2 DataFusion, or V3 Rust execution. | [../../src/DeltaTableService.Client/DeltaTableServiceClient.cs](../../src/DeltaTableService.Client/DeltaTableServiceClient.cs) |
| `IDeltaTableServiceBackend` | Internal contract shared by Flight and native backends. | [../../src/DeltaTableService.Client/Internal/IDeltaTableServiceBackend.cs](../../src/DeltaTableService.Client/Internal/IDeltaTableServiceBackend.cs) |
| `StorageConfig` | Legacy per-request storage account and SAS configuration. | [../../src/DeltaTableService.Client/Models/StorageConfig.cs](../../src/DeltaTableService.Client/Models/StorageConfig.cs) |
| `GenericStorageOptions` | Dictionary-based storage options for delta-rs/native scenarios. | [../../src/DeltaTableService.Client/Models/GenericStorageOptions.cs](../../src/DeltaTableService.Client/Models/GenericStorageOptions.cs) |
| `RecordBatch` / `IArrowArrayStream` | Arrow-native data exchange formats. | [../../src/DeltaTableService.Client/DeltaTableServiceClient.cs](../../src/DeltaTableService.Client/DeltaTableServiceClient.cs) |
| `DbDataReader` | Row-oriented consumption surface for .NET callers. | [../../src/DeltaTableService.Client/Internal/ArrowStreamDataReader.cs](../../src/DeltaTableService.Client/Internal/ArrowStreamDataReader.cs) |
| `DeltaAdbcDriver` | ADBC driver entry point for read-only Arrow consumers. | [../../src/DeltaTableService.Adbc/DeltaAdbcDriver.cs](../../src/DeltaTableService.Adbc/DeltaAdbcDriver.cs) |

## Architectural Philosophy

The SDK keeps one C# client shape over multiple execution engines. Backends differ in process boundary, feature availability, and operational cost, but the public client tries to preserve common read, query, write, and DML patterns.

Key design choices:

- Use Arrow as the primary transport and in-memory exchange representation.
- Keep Flight backends service-based for V1/V2 compatibility and test coverage.
- Use native in-process Rust as the preferred client SDK and ADBC execution path.
- Expose row-oriented APIs through `DbDataReader` without making row materialization the default.
- Keep backend-specific limitations explicit through `NotSupportedException` rather than silently emulating unavailable behavior.

## Dominant Execution Model

| Mode | Boundary | Transport | Typical Use |
| --- | --- | --- | --- |
| `V1_Spark` | External service/container | Arrow Flight over gRPC | Service-backed compatibility and test harness scenarios requiring Spark-compatible behavior. |
| `V2_DataFusion` | External service/container | Arrow Flight over gRPC | Service-backed compatibility and test harness scenarios where DataFusion/delta-rs behavior is required. |
| `V3_Rust` | In-process native DLL | C ABI plus Arrow C Data/C Stream | Preferred SDK and ADBC runtime for native reads/writes, CDF, partition planning, and local execution. |

Most SDK operations are asynchronous and stream Arrow `RecordBatch` values. Helper APIs can materialize results into `DbDataReader`, `DataTable`, dictionaries, or Arrow streams, but consumers should prefer streaming for large tables.

## Lifecycle Expectations

- Create one `DeltaTableServiceClient` per backend configuration or service endpoint.
- Dispose clients after use; Flight mode owns a gRPC channel and V3 owns a native engine handle.
- Treat `IAsyncEnumerable<RecordBatch>` and `IArrowArrayStream` results as streaming resources.
- Avoid buffering full tables unless data volume is known to be small.
- Use per-request storage options for SAS/OneLake authentication instead of global mutable state.
- For V3 native operations, assume a synchronous FFI boundary inside the backend call path even when the public API is asynchronous.

## Major Tradeoffs

| Tradeoff | Consequence |
| --- | --- |
| One client over heterogeneous engines | Integration is simpler, but agents must check backend capability before choosing APIs. |
| Arrow-first data model | Efficient streaming is possible, but row materialization can be expensive. |
| V1/V2 external services | Preserve compatibility and integration-test coverage, but require endpoint/container orchestration. |
| V3 native interop | Lower overhead and advanced features, but native library packaging and memory ownership matter. |
| ADBC read-only MVP | Works well for Arrow-native consumers, but writes, transactions, and prepared statements are intentionally absent. |

## Operational Assumptions

- The caller controls table paths and storage credentials.
- Delta table mutations should be treated as storage-backed operations with Delta transaction log semantics, not in-memory mutations.
- Not every backend implements every public method; V1/V2 throw for V3-only APIs.
- The native Rust DLL must be present for V3 and ADBC execution.
- `net472` and `netstandard2.0` consumers rely on compatibility package references and the legacy native interop path.

## Glossary

| Term | Meaning |
| --- | --- |
| Arrow Flight | gRPC-based Arrow transport used by V1 and V2 services. |
| Arrow C Data Interface | FFI representation used to exchange Arrow schema/streams with the V3 native Rust engine. |
| CDF | Delta Change Data Feed; public C# read/query APIs are V3-only. |
| Delta transaction log | `_delta_log` metadata that defines table versions, schema, protocol, and actions. |
| Flight backend | V1 or V2 service accessed through `FlightClientWrapper`. |
| Native backend | V3 Rust engine loaded into the .NET process. |
| Partition descriptor | Opaque `DeltaReadPartition` token generated by V3 partition planning. |
| Storage options | Per-request authentication and object store options supplied through `StorageConfig` or `GenericStorageOptions`. |
