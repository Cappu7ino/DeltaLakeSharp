# Execution Model

## Summary

DeltaTableService exposes one C# client shape over three backend execution modes. The public package can connect to service-backed Flight endpoints or run the V3 native Rust engine in process.

## Backend Selection

| Mode | Boundary | Owner | Recommended Use |
| --- | --- | --- | --- |
| `V1_Spark` | External service/container | Caller or test harness | Spark-compatible service-backed compatibility. |
| `V2_DataFusion` | External service/container | Caller or test harness | DataFusion/delta-rs service-backed compatibility. |
| `V3_Rust` | In-process native DLL | `DeltaTableServiceClient` | Preferred SDK runtime and required ADBC runtime. |

Source: [../../src/DeltaTableService.Client/DeltaTableServiceClient.cs](../../src/DeltaTableService.Client/DeltaTableServiceClient.cs)

## Client Lifecycle

- `DeltaTableServiceClient(Uri)` defaults to `V1_Spark`.
- `DeltaTableServiceClient(Uri, ServiceMode)` selects V1, V2, or V3.
- `DeltaTableServiceClient(ServiceMode)` accepts only `V3_Rust` because V1/V2 require a server URI.
- The client implements `IDisposable` and owns backend resources.

## Backend Contract

`IDeltaTableServiceBackend` is the internal abstraction shared by `FlightClientWrapper` and `NativeRustBackend`.

Source: [../../src/DeltaTableService.Client/Internal/IDeltaTableServiceBackend.cs](../../src/DeltaTableService.Client/Internal/IDeltaTableServiceBackend.cs)

The contract covers:

- health checks
- schema reads
- table reads
- SQL queries
- create/insert/write operations
- DML operations
- CDF operations
- partition planning and partition reads
- protocol upgrades

## Flight Execution

V1 and V2 use `FlightClientWrapper`.

Source: [../../src/DeltaTableService.Client/Internal/FlightClientWrapper.cs](../../src/DeltaTableService.Client/Internal/FlightClientWrapper.cs)

Properties:

- Uses Arrow Flight over gRPC.
- Owns a `GrpcChannel` and `FlightClient`.
- Requires a reachable server endpoint.
- Uses service/container orchestration in tests.
- Throws `NotSupportedException` for V3-only public APIs.

## Native Execution

V3 uses `NativeRustBackend`.

Source: [../../src/DeltaTableService.Client/Internal/NativeRustBackend.cs](../../src/DeltaTableService.Client/Internal/NativeRustBackend.cs)

Properties:

- Runs in process through a native Rust DLL.
- Uses JSON command payloads for metadata and operation parameters.
- Uses Arrow C Data/C Stream interfaces for schemas and batches.
- Owns a native engine handle through a `SafeHandle` wrapper.
- Avoids the Flight service boundary but requires native runtime assets.

## Streaming First

The dominant result shape is streaming Arrow data:

- `IAsyncEnumerable<RecordBatch>` for C# async streaming.
- `IArrowArrayStream` for Arrow-native consumers.
- `DbDataReader` for row-oriented adapters.

Materialization helpers are convenience APIs and should not be generated as the default for unknown-size data.

## Capability Failures

Backend-specific limitations should remain visible. The wrapper throws explicit exceptions rather than silently emulating unsupported features. Agents should use these failures to select the correct backend instead of suppressing them.
