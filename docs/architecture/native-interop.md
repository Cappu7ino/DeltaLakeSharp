# Native Rust Interop

## Summary

The V3 backend runs the Delta engine inside the .NET process through a native Rust library. It combines simple C ABI entry points, JSON command metadata, and Arrow C Data/C Stream interfaces for columnar exchange.

## Managed Entry Points

Managed V3 execution flows through:

- [../../src/DeltaTableService.Client/Internal/NativeRustBackend.cs](../../src/DeltaTableService.Client/Internal/NativeRustBackend.cs)
- [../../src/DeltaTableService.Client/Internal/Native/NativeEngineHandle.cs](../../src/DeltaTableService.Client/Internal/Native/NativeEngineHandle.cs)
- [../../src/DeltaTableService.Client/Internal/Native/NativeMethods.net8.cs](../../src/DeltaTableService.Client/Internal/Native/NativeMethods.net8.cs)
- [../../src/DeltaTableService.Client/Internal/Native/NativeMethods.net472.cs](../../src/DeltaTableService.Client/Internal/Native/NativeMethods.net472.cs)

## Framework-Specific Interop

| Target | Interop Path | Reason |
| --- | --- | --- |
| `net8.0` | source-generated `LibraryImport` | Modern interop and native library resolver support. |
| `net472` | `DllImport` | Legacy framework compatibility. |
| `netstandard2.0` | `DllImport` | Broad package consumption compatibility. |

## Engine Ownership

`NativeEngineHandle` owns the Rust engine pointer.

Lifecycle:

1. `NativeEngineHandle.Create()` ensures the native library is loaded.
2. It calls native engine creation.
3. The handle is stored by `NativeRustBackend`.
4. Dispose releases the handle.
5. `ReleaseHandle()` destroys the native engine.

This keeps native engine cleanup tied to managed client disposal.

## Data Exchange

| Data | Representation | Ownership Rule |
| --- | --- | --- |
| command metadata | JSON string | Managed code builds command payload; Rust parses it. |
| schema | Arrow C Data schema | Managed code imports schema and frees temporary native structures. |
| read batches | Arrow C Stream | Imported managed stream owns the release callback. |
| write batches | Arrow C Stream | Managed stream is exported to Rust for operation duration. |
| string results | native string pointer | Managed code frees returned native strings. |

## Native Library Discovery

The runtime attempts package-local, runtime-specific, development, and system search paths. For external consumers, the package must carry the native runtime asset expected by V3.

Common failure modes:

- Native DLL not copied to output.
- Wrong runtime identifier.
- Rust library not built for local development.
- Platform-specific library name mismatch.

## Concurrency Expectations

The public API is asynchronous, but V3 crosses a synchronous FFI boundary for native calls. Do not assume unlimited parallelism through a single client instance. For parallel reads, prefer V3 partition planning and independent partition consumption.

## Error Handling

Native failures are surfaced as managed exceptions that include operation context and the native last-error message when available. Agents should preserve these messages in diagnostics and not replace them with generic errors.
