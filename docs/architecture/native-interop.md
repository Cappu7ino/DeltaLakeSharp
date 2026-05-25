# Native Rust Interop

## Summary

The V3 backend runs the Delta engine inside the .NET process through a native Rust library. It combines simple C ABI entry points, JSON command metadata, and Arrow C Data/C Stream interfaces for columnar exchange.

## Managed Entry Points

Managed V3 execution flows through:

- [../../src/DeltaLakeSharp.Client/Internal/NativeRustBackend.cs](../../src/DeltaLakeSharp.Client/Internal/NativeRustBackend.cs)
- [../../src/DeltaLakeSharp.Client/Internal/Native/NativeEngineHandle.cs](../../src/DeltaLakeSharp.Client/Internal/Native/NativeEngineHandle.cs)
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
| schema | Arrow C Data schema | Managed code imports schema and frees temporary native structures. |
| read batches | Arrow C Stream | Imported managed stream owns the release callback; Rust uses bounded prefetch behind the stream. |
| write batches | Arrow C Stream | Managed stream is exported to Rust for operation duration. |
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

The public API is asynchronous, but V3 crosses a synchronous FFI boundary for native calls. Do not assume unlimited parallelism through a single client instance. For parallel reads, prefer V3 partition planning and independent partition consumption.

V3 read streams use a small Rust-owned bounded prefetch queue behind the exported Arrow C Stream. A Tokio producer task advances the DataFusion stream and sends ready batch results into the queue, while the Arrow C Stream pull side drains queued batches. The queue is bounded per stream, and native read production is also guarded by a process-wide active-production limit so full per-stream queues do not monopolize global read capacity.

Multiple V3 clients share the same process-wide Tokio runtime. This reduces thread and stack overhead compared with one runtime per engine, but it also means blocking native work can affect other V3 clients in the same process. Imported Arrow streams from managed code should avoid long blocking pulls; if a write source can block for a long time, isolate that behavior before sharing the client across high-concurrency workflows.

## Error Handling

Native failures are surfaced as managed exceptions that include operation context and the native last-error message when available. Agents should preserve these messages in diagnostics and not replace them with generic errors.
