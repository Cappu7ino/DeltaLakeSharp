# Execution Model

## Summary

DeltaLakeSharp exposes one C# client shape over three backend execution modes. The public package can connect to service-backed Flight endpoints or run the V3 native Rust engine in process.

## Backend Selection

| Mode | Boundary | Owner | Recommended Use |
| --- | --- | --- | --- |
| `V1_Spark` | External service/container | Caller or test harness | Spark-compatible service-backed compatibility. |
| `V2_DataFusion` | External service/container | Caller or test harness | DataFusion/delta-rs service-backed compatibility. |
| `V3_Rust` | In-process native DLL | `DeltaTableServiceClient` | Preferred SDK runtime and required ADBC runtime. |

Source: [../../src/DeltaLakeSharp.Client/DeltaTableServiceClient.cs](../../src/DeltaLakeSharp.Client/DeltaTableServiceClient.cs)

## Client Lifecycle

- `DeltaTableServiceClient(Uri)` defaults to `V1_Spark`.
- `DeltaTableServiceClient(Uri, ServiceMode)` selects V1, V2, or V3.
- `DeltaTableServiceClient(ServiceMode)` accepts only `V3_Rust` because V1/V2 require a server URI.
- The client implements `IDisposable` and owns backend resources.

## Backend Contract

`IDeltaLakeBackend` is the internal abstraction shared by `FlightClientWrapper` and `NativeRustBackend`.

Source: [../../src/DeltaLakeSharp.Client/Internal/IDeltaLakeBackend.cs](../../src/DeltaLakeSharp.Client/Internal/IDeltaLakeBackend.cs)

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

Source: [../../src/DeltaLakeSharp.Client/Internal/FlightClientWrapper.cs](../../src/DeltaLakeSharp.Client/Internal/FlightClientWrapper.cs)

Properties:

- Uses Arrow Flight over gRPC.
- Owns a `GrpcChannel` and `FlightClient`.
- Requires a reachable server endpoint.
- Uses service/container orchestration in tests.
- Throws `NotSupportedException` for V3-only public APIs.

## Native Execution

V3 uses `NativeRustBackend`.

Source: [../../src/DeltaLakeSharp.Client/Internal/NativeRustBackend.cs](../../src/DeltaLakeSharp.Client/Internal/NativeRustBackend.cs)

Properties:

- Runs in process through a native Rust DLL.
- Uses JSON command payloads for metadata and operation parameters.
- Uses Arrow C Data/C Stream interfaces for schemas and batches.
- Uses direct Arrow C Stream pulls by default, with bounded Rust-side prefetch available through `DeltaTableServiceClientOptions.EnableNativeReadPrefetch`.
- Owns a native engine handle through a `SafeHandle` wrapper.
- Uses a native async operation `SafeHandle` for V3 partition planning.
- Shares one process-wide Tokio runtime across native engine handles.
- Avoids the Flight service boundary but requires native runtime assets.

Each `NativeRustBackend` still owns its native engine handle and per-engine error state. The shared runtime reduces thread and stack reservation overhead when multiple V3 clients are created in the same process. Native merge work is scheduled onto Tokio worker threads so deep delta-rs/DataFusion merge execution does not run on the .NET caller stack.

`GetReadPartitionsAsync` starts native partition planning as a callback-notified operation on the shared Tokio runtime. The managed backend awaits a `TaskCompletionSource`, takes the JSON result once after native completion is signaled, and cancels the native operation if the managed cancellation token is signaled. The public API and partition descriptor model are unchanged.

When read-stream prefetch is enabled, production is bounded in two ways: each exported stream has a small native queue, and active backend batch production is capped process-wide. These limits provide backpressure for high-concurrency readers without changing the public `IAsyncEnumerable<RecordBatch>` or `IArrowArrayStream` shapes. The default read path remains direct batch pulling because local benchmarks showed prefetch overhead can dominate small/local reads.

## Streaming First

The dominant result shape is streaming Arrow data:

- `IAsyncEnumerable<RecordBatch>` for C# async streaming.
- `IArrowArrayStream` for Arrow-native consumers.
- `DbDataReader` for row-oriented adapters.

Materialization helpers are convenience APIs and should not be generated as the default for unknown-size data.

## Capability Failures

Backend-specific limitations should remain visible. The wrapper throws explicit exceptions rather than silently emulating unsupported features. Agents should use these failures to select the correct backend instead of suppressing them.
