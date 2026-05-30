# Native Rust Interop

## Summary

The V3 backend runs the Delta engine inside the .NET process through a native Rust library. It combines simple C ABI entry points, JSON command metadata, and Arrow C Data/C Stream interfaces for columnar exchange.

## Managed Entry Points

Managed V3 execution flows through:

- [../../src/DeltaLakeSharp.Client/Internal/NativeRustBackend.cs](../../src/DeltaLakeSharp.Client/Internal/NativeRustBackend.cs)
- [../../src/DeltaLakeSharp.Client/Internal/Native/NativeEngineHandle.cs](../../src/DeltaLakeSharp.Client/Internal/Native/NativeEngineHandle.cs)
- [../../src/DeltaLakeSharp.Client/Internal/Native/NativeAsyncOperationHandle.cs](../../src/DeltaLakeSharp.Client/Internal/Native/NativeAsyncOperationHandle.cs)
- [../../src/DeltaLakeSharp.Client/Internal/Native/NativeMethods.net8.cs](../../src/DeltaLakeSharp.Client/Internal/Native/NativeMethods.net8.cs)
- [../../src/DeltaLakeSharp.Client/Internal/Native/NativeMethods.net472.cs](../../src/DeltaLakeSharp.Client/Internal/Native/NativeMethods.net472.cs)

## Framework-Specific Interop

| Target | Interop Path | Reason |
| --- | --- | --- |
| `net8.0` | source-generated `LibraryImport` | Modern interop and native library resolver support. |
| `net472` | `DllImport` | Legacy framework compatibility. |
| `netstandard2.0` | `DllImport` | Broad package consumption compatibility. |

## Engine And Runtime Ownership

`NativeEngineHandle` owns a Rust engine pointer. The engine pointer is a lightweight per-client handle that keeps per-engine state such as the last native error message. Tokio runtime ownership is process-wide: V3 uses a lazily initialized shared Tokio runtime, and each engine stores a cloned runtime handle.

Lifecycle:

1. `NativeEngineHandle.Create()` ensures the native library is loaded.
2. It calls native engine creation.
3. The native engine clones a handle to the shared Tokio runtime.
4. The handle is stored by `NativeRustBackend`.
5. Dispose releases the handle.
6. `ReleaseHandle()` destroys the native engine.

This keeps per-engine cleanup tied to managed client disposal while avoiding a separate Tokio worker pool for every managed V3 backend. The shared runtime intentionally remains alive for the process lifetime so exported Arrow streams can continue to use runtime handles after the FFI call that created them returns.

Native merge work runs on Tokio worker threads instead of polling the whole merge future on the foreign .NET caller stack. Worker threads use an explicit stack size to avoid stack overflows through deep delta-rs/DataFusion merge paths.

## Data Exchange

| Data | Representation | Ownership Rule |
| --- | --- | --- |
| command metadata | JSON string | Managed code builds command payload; Rust parses it. |
| schema | Arrow C Data schema | Managed code imports schema and frees temporary native structures; async schema reads take the schema result exactly once. |
| read batches | Arrow C Stream | Imported managed stream owns the release callback; Rust can use bounded prefetch behind the stream when enabled. |
| write batches | Arrow C Stream | Managed stream is exported to Rust for operation duration; async insert and merge keep the exported stream and native storage alive until completion notification. |
| one-shot async operation | native operation pointer | Managed code awaits a `TaskCompletionSource`, takes the owned result string or Arrow stream once after native completion notification, and destroys the operation handle. |
| string results | native string pointer | Managed code frees returned native strings. |

## Native Library Discovery

The runtime attempts package-local, runtime-specific, development, and system search paths. For external consumers, the package must carry the native runtime asset expected by V3.

Local source builds produce platform-specific native artifacts under `src/DeltaLakeSharp.Server/v3/target/<profile>/`:

- Windows: `delta_table_service_native.dll`
- macOS: `libdelta_table_service_native.dylib`
- Linux: `libdelta_table_service_native.so`

The V3 fixture binary is also platform-specific: `delta-table-service-v3-fixture.exe` on Windows and `delta-table-service-v3-fixture` on macOS/Linux.

Common failure modes:

- Native library not copied to output.
- Wrong runtime identifier.
- Rust library not built for local development.
- Platform-specific library name mismatch.

## Concurrency Expectations

The public API is asynchronous, and the main V3 native operations use callback-notified native operation handles. Do not assume unlimited parallelism through a single client instance. For parallel reads, prefer V3 partition planning and independent partition consumption.

Schema reads, partition planning, table creation, protocol upgrade, SQL DML operations, table/query/CDF/partition stream setup, and insert/merge setup use native async operation handles with completion notification. Managed code starts the relevant `*_async_with_callback` export, awaits a `TaskCompletionSource`, takes the result string with `dts_async_operation_take_result`, the schema with `dts_async_operation_take_schema`, or the stream with `dts_async_operation_take_stream` after the native callback fires, and releases the handle through `dts_async_operation_destroy`. Cancellation requests call `dts_async_operation_cancel` before managed code surfaces `OperationCanceledException`. This keeps the public API shapes unchanged while moving those one-shot operations onto the shared Tokio runtime instead of blocking the managed caller thread for the whole native operation.

Native async operations have a small state machine exposed through stable integer status values. Managed code mirrors those values with an internal enum and treats unknown values as native failures.

| Native state | Status | Result ownership |
| --- | --- | --- |
| `Pending` | `0` | Tokio task may still complete. No result, schema, stream, or error may be taken. |
| `Succeeded` | `1` | JSON result is owned by Rust until `dts_async_operation_take_result` transfers it exactly once to managed code, which frees it with `dts_free_string`. |
| `SucceededSchema` | `1` | Arrow schema is owned by Rust until `dts_async_operation_take_schema` writes it exactly once into caller-provided `FFI_ArrowSchema` storage. |
| `SucceededStream` | `1` | Arrow stream reader is owned by Rust until `dts_async_operation_take_stream` writes it exactly once into caller-provided `FFI_ArrowArrayStream` storage. |
| `Failed` | `2` | Error message pointer and error code are borrowed from the operation state and remain valid until the operation is destroyed or mutated. |
| `Cancelled` | `3` | Error message pointer and `Cancelled` code are borrowed from the operation state. Managed cancellation detection prefers this typed code. |
| destroyed | n/a | `dts_async_operation_destroy` clears the callback operation pointer and aborts any pending task. Late native completion must not invoke the managed callback. |

The native callback only signals that a terminal state is available; managed code must still query status and take the appropriate result. Result, schema, and stream take operations are single-use. Destroying an operation before completion suppresses late callbacks by clearing the stored operation pointer before aborting the task.

Native async task bodies catch panics and convert them to `Failed` with an internal error code before notifying the callback. This keeps managed callers from waiting forever if a native async task exits abnormally before producing a normal result.

For async insert and merge, Rust imports the caller-provided Arrow C Stream before spawning the write task. Managed code keeps both the exported `IArrowArrayStream` adapter and the `CArrowArrayStream` storage alive until native completion is signaled, then disposes and frees them. Cancellation waits for the aborted native task to drop the imported reader before notifying managed code, which prevents the native writer from reading through released managed stream state while still avoiding a blocking write FFI call.

Synchronous Rust C ABI exports are retained for native ABI compatibility, direct Rust unit coverage, and diagnostics. Managed SDK production paths should prefer the callback exports for operations with meaningful native work.

FFI helper boundaries stay intentionally narrow. Command pointer validation maps null `command_json` to `InvalidRequest`. Output pointer validation maps null schema and stream outputs to `InvalidRequest`. Source stream validation maps null inputs to `InvalidRequest`, while Arrow C Stream import failures map to `Arrow`. JSON result conversion failures map to `Internal`. Service-layer failures should preserve their `ServiceErrorCode` through the native error state rather than being collapsed to `Internal`.

Arrow schema, stream, and JSON result transfers from async operation handles are single-use ownership transfers. Null operation handles or null output buffers fail without taking ownership. Managed callers may partially consume exported read streams and dispose them early; native stream release must not require draining the stream. For async insert and merge, managed exported source streams and their `CArrowArrayStream` storage remain alive until native completion or cancellation is signaled.

By default, V3 read streams pull each batch through the Arrow C Stream callback and synchronously bridge to the async DataFusion stream. `DeltaTableServiceClientOptions.EnableNativeReadPrefetch` enables an experimental prefetch mode that places a small Rust-owned bounded queue behind the exported Arrow C Stream. In that mode, a Tokio producer task advances the DataFusion stream and sends ready batch results into the queue, while the Arrow C Stream pull side drains queued batches. The queue is bounded per stream, and native read production is guarded by a process-wide active-production limit so full per-stream queues do not monopolize global read capacity.

Multiple V3 clients share the same process-wide Tokio runtime. This reduces thread and stack overhead compared with one runtime per engine, but it also means blocking native work can affect other V3 clients in the same process. Imported Arrow streams from managed code should avoid long blocking pulls; if a write source can block for a long time, isolate that behavior before sharing the client across high-concurrency workflows.

## Performance Coverage

V3 performance coverage is split between smoke tests and BenchmarkDotNet scenarios. Smoke tests live in [../../tests/DeltaLakeSharp.Tests/NativeRustPerformanceSmokeTests.cs](../../tests/DeltaLakeSharp.Tests/NativeRustPerformanceSmokeTests.cs) and intentionally avoid strict wall-clock thresholds. They assert structural behavior such as bounded partition token payloads, first-batch availability without draining a stream, prefetch first-batch behavior, many-client creation, and concurrent partition read row counts.

Benchmark scenarios live under [../../benchmarks/DeltaLakeSharp.Benchmark](../../benchmarks/DeltaLakeSharp.Benchmark). The Phase 9 V3 benchmarks generate deterministic local datasets for small baseline, many-file, wide-schema, partitioned, and CDF-enabled profiles. Benchmarks measure public schema reads, partition planning token sizes, first-batch latency, full Arrow scans, concurrent partition reads, and CDF reads. These benchmarks are intended for local trend tracking and regression investigation, not for tight CI pass/fail timing gates.

Dataset generation is local-only and credential-free. Generated data defaults to benchmark output paths and can be redirected with benchmark CLI options. Prefetch is measured both disabled and enabled so regressions in the experimental prefetch path can be spotted without implying that prefetch is required for normal reads.

## Error Handling

Native failures are surfaced as managed exceptions that include operation context, the native last-error message, and the native error code when available. Agents should preserve these messages in diagnostics and not replace them with generic errors.

The native ABI exposes stable integer error codes for engine-level last errors and async operation failures. `0` means success/no error; non-zero values distinguish invalid requests, missing tables, Delta/DataFusion/Arrow/JSON failures, internal failures, and cancellation. Managed code maps these values to an internal `NativeServiceErrorCode` enum for diagnostics and control flow. Cancellation detection uses the typed async error code first and message matching only as a compatibility fallback.
