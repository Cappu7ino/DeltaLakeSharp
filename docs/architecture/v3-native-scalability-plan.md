# V3 Native Scalability Plan

## Summary

This plan focuses on improving V3 native SDK and ADBC scalability without switching to same-machine Arrow Flight IPC. The preferred direction is to keep embedded Rust as the default high-performance runtime and address the current sync-over-async pressure points directly.

The two optimization techniques are:

- Native async operation handles for long one-shot operations.
- Rust-owned bounded prefetch queues for streaming reads.

The goal is to improve responsiveness under concurrent reads and writes while preserving the advantages of the current in-process V3 architecture: low operational friction, Arrow-native exchange, and no same-machine IPC layer.

## Rollout Stages

1. **Per-stream bounded prefetch.** Replace per-batch `block_on(stream.next())` with a Rust-owned producer queue behind the existing Arrow C Stream shape.
2. **Global active-production limits.** Add runtime-wide caps so many concurrent streams cannot create unbounded DataFusion/object-store work or unbounded aggregate prefetch memory.
3. **Cancellation, disposal, and stress coverage.** Prove early stream release, producer errors, and concurrent readers behave correctly under load.
4. **Native async operation handles.** Apply the same responsiveness model to long one-shot operations such as writes, DML, merge, planning, and schema reads.

The first implementation slices should be treated as incremental hardening of the embedded V3 path, not as a complete replacement for full concurrency benchmarking.

## Current Problem Shape

V3 exposes asynchronous C# APIs, but many native calls cross a synchronous FFI boundary. Rust then drives async work through the shared Tokio runtime. This can hold managed caller threads during I/O-bound native work and can repeat the same sync-over-async pattern on every streamed batch.

This is most concerning for:

- high-concurrency SDK or ADBC consumers;
- long one-shot operations such as writes, DML, merge, partition planning, schema reads, and stream setup;
- streaming reads where every batch pull may block on Rust async stream progress;
- cancellation and disposal scenarios where work should stop promptly;
- workloads where unbounded parallelism would create memory, object-store, or ThreadPool pressure.

## Design Goals

- Preserve the current public SDK and ADBC surface where possible.
- Keep embedded V3 native Rust as the primary performance path.
- Avoid same-machine Flight IPC as the first performance fix.
- Remove long managed-thread waits for one-shot native operations.
- Reduce per-batch sync-over-async overhead in streaming reads.
- Add explicit backpressure instead of allowing unbounded concurrency.
- Treat cancellation, disposal, and ownership as first-class design requirements.
- Preserve explicit native error propagation rather than hiding failures as empty results.

## Technique 1: Native Async Operation Handles

### Purpose

Use native async operation handles for operations that produce one final result but may take meaningful time.

Candidate operations include:

- schema reads;
- partition planning;
- create table;
- insert/write;
- update/delete DML;
- merge-data operations;
- protocol upgrade;
- expensive stream-opening paths.

### Current Shape

The current call shape is synchronous at the native boundary:

```text
C# calls native function
Rust block_on(async work)
C# caller thread waits
native returns result
C# returns a completed Task
```

This makes the API async-shaped, but the initiating managed thread remains occupied for the native operation duration.

### Proposed Shape

Split native execution into start, completion, result collection, cancellation, and cleanup:

```text
C# calls native start function
Rust creates an operation handle and spawns async work on Tokio
native returns the operation handle immediately
C# awaits a Task without blocking a managed ThreadPool thread
Rust completes the operation later
C# collects result, error, or cancellation through the handle
```

### Native ABI Model

Add an opaque native operation type owned by Rust. The lifecycle should be explicit:

- `start`: begins work and returns quickly with an operation handle.
- `status` or notification: indicates completion without blocking the initiating P/Invoke.
- `take_result`: retrieves the successful result exactly once.
- `get_error`: retrieves terminal native error details.
- `cancel`: requests cancellation.
- `destroy`: releases native operation state.

Operation handles should be separate from `DeltaServiceEngine` handles, but associated with the originating engine for runtime access, last-error behavior, and resource ownership.

Terminal states should be explicit:

- pending;
- completed;
- failed;
- cancelled;
- result taken;
- destroyed.

Every operation must have one cleanup path, including races between completion, cancellation, disposal, and failed awaits.

### Completion Strategy

Prefer a completion callback or completion queue over periodic polling for production behavior.

In C#:

- create a `TaskCompletionSource<T>` for each operation;
- use `TaskCreationOptions.RunContinuationsAsynchronously`;
- register cancellation tokens against native cancellation;
- complete the task only after native completion has been signaled and the result or error has been collected.

In Rust:

- spawn the operation future onto the shared Tokio runtime;
- store terminal result state in the operation handle;
- notify completion without making the initiating P/Invoke wait;
- keep callbacks minimal and avoid running managed continuations on Rust/Tokio worker threads.

### Cancellation

Each async operation should support native cancellation where practical.

C# cancellation should call native cancel on the operation handle. Rust should store cancellation channels, task handles, or equivalent state per operation and observe cancellation at practical boundaries, such as before opening tables, planning partitions, reading or writing batches, and committing operations.

Cancelled operations should surface as cancellation or failure, not as successful empty results.

### Expected Benefits

- Managed caller threads are not held for the full native operation duration.
- Long writes, DML, schema reads, and planning calls become responsive to cancellation.
- Native operation lifecycle becomes observable and measurable.
- Bounded operation queues can provide backpressure under high concurrency.

## Technique 2: Rust-Owned Bounded Prefetch Queues

### Purpose

Use Rust-owned bounded prefetch queues for operations that return many `RecordBatch` values.

Candidate streams include:

- table reads;
- partition reads;
- SQL query reads;
- Change Data Feed reads;
- ADBC read streams.

### Current Shape

The current streaming path can block once per batch:

```text
C# asks for next batch
Rust block_on(datafusion_stream.next())
C# waits for that batch
repeat for every batch
```

This means C# batch pulls can repeatedly drive Rust async stream progress across a synchronous boundary.

### Proposed Shape

Introduce a native stream state with a bounded producer/consumer queue:

```text
Rust starts a producer task for the stream
producer asynchronously pulls batches from DataFusion
producer places ready batches into a small bounded queue
C# Arrow stream pulls drain the queue
if the queue is empty, C# waits for producer progress
if the queue is full, Rust producer pauses
```

The queue is a controlled read-ahead buffer. It should improve throughput and responsiveness without turning streaming into full materialization.

### Native Stream State

The native stream state should own:

- schema;
- bounded queue of batch results;
- producer task;
- cancellation signal;
- terminal state;
- error state;
- release/disposal state;
- optional counters for diagnostics.

The queue should hold successful batches, terminal errors, and end-of-stream state explicitly. Producer errors must not be converted into silent end-of-stream.

### Backpressure

Backpressure should be explicit and bounded at two levels.

Per stream:

- start with a small default prefetch depth, such as 2-4 batches;
- when the queue is full, the producer waits for capacity instead of reading more data.

Process-wide or runtime-wide:

- cap active batch-production work;
- cap total prefetched batches or bytes;
- keep partitioned reads bounded so many partitions do not multiply memory unexpectedly.

The initial global limit can be a conservative active-production cap. A producer should hold this permit only while polling or awaiting the next backend batch, and release it before waiting on a full per-stream queue. This prevents unlimited Tokio/DataFusion producer fan-out without allowing idle full queues to starve later streams. Byte-aware limits can follow if benchmarks show large-batch memory pressure.

Byte-aware limits can be added after batch-count limits if benchmarks show large batch memory risk.

### Cancellation And Disposal

On C# stream disposal, cancellation, or failure:

- signal the native stream producer to stop;
- wake any waiting consumer;
- drop queued batches exactly once;
- release Arrow C Stream state safely;
- preserve Apache Arrow C Data/C Stream ownership rules.

Early stream disposal must stop prefetching. Otherwise, native producer tasks could continue reading data nobody will consume.

### Expected Benefits

- Rust/Tokio can drive DataFusion streams naturally instead of being driven one batch at a time by C# pulls.
- C# pulls often return ready batches.
- Object-store and Parquet read latency can be smoothed.
- Queue bounds prevent unbounded memory growth.
- Queue metrics can expose whether readers are producer-bound, consumer-bound, or over-prefetching.

## Combined Read Flow

The two techniques compose naturally for reads:

```text
C# starts read operation
Rust asynchronously prepares stream state
C# awaits stream handle without blocking a managed ThreadPool thread
Rust stream producer begins bounded prefetch
C# consumes batches from the Arrow C Stream
queue backpressure controls memory and producer pace
```

Async operation handles improve stream setup. Bounded prefetch queues improve stream consumption.

## Implementation Plan

1. Establish baseline behavior and success metrics.
   - Map current blocking V3 paths in `NativeRustBackend` and native Rust exports.
   - Classify operations into one-shot operations and streaming reads.
   - Define target metrics: managed ThreadPool availability, operation latency, read throughput, p95/p99 batch wait time, memory growth, cancellation latency, and error propagation correctness.
   - Run existing V3 correctness tests and focused read benchmarks before changes.

2. Design the native async operation handle ABI.
   - Add an opaque native operation type owned by Rust.
   - Define start, notification/status, take-result, get-error, cancel, and destroy exports.
   - Define terminal states and cleanup rules.

3. Implement async operation completion.
   - Prefer a completion callback or completion queue over periodic polling.
   - Wrap native operations in C# `TaskCompletionSource<T>` instances using `RunContinuationsAsynchronously`.
   - Spawn Rust futures onto the shared Tokio runtime and notify completion after terminal state is stored.

4. Apply async operation handles to one-shot operations.
   - Convert schema reads, partition planning, create table, DML, merge, insert/write, protocol upgrade, and expensive stream-opening paths.
   - Preserve public SDK and ADBC signatures.
   - Preserve existing native error style.

5. Add explicit cancellation for async operations.
   - Register C# cancellation tokens to native cancel.
   - Add Rust-side cancellation channels or task handles.
   - Define cancellation semantics and tests.

6. Design bounded prefetch queues for read streams.
   - Replace per-batch `block_on(stream.next())` behavior with producer/consumer stream state.
   - Push DataFusion batches into a bounded queue from a Tokio producer task.
   - Have Arrow C Stream `next` drain the queue and surface terminal errors or end-of-stream explicitly.

7. Add backpressure and global resource limits.
   - Add per-stream queue depth limits.
   - Add global active producer and total prefetch limits.
   - Define overload behavior when limits are reached.

8. Integrate stream cancellation and disposal.
   - Stop producers on stream release, cancellation, or failure.
   - Drop queued batches exactly once.
   - Wake waiting consumers during terminal transitions.

9. Add observability and diagnostics.
   - Track operation duration, cancellation latency, stream queue depth, queue empty wait time, queue full wait time, producer count, prefetched batch count, and native errors.
   - Avoid logging storage credentials, SAS tokens, storage options, partition tokens, or private paths.

10. Validate correctness and performance.
    - Run V3 integration coverage for reads, SQL, partitions, CDF, writes, DML, merge-data, and protocol operations.
    - Add stress tests for cancellation, early stream disposal, concurrent readers, concurrent writers, producer errors, consumer errors, and engine disposal while operations are pending.
    - Benchmark low, medium, and high concurrency read/write scenarios before and after the changes.

11. Update documentation and guidance.
    - Document native async operation handles, stream prefetch/backpressure, cancellation behavior, and concurrency limits.
    - Keep embedded V3 as the default SDK and ADBC guidance.
    - Document tuning knobs only if intentionally public.

## Relevant Files

- `src/DeltaLakeSharp.Client/Internal/NativeRustBackend.cs` - main managed V3 backend; convert blocking native call sites to async operation-handle flows and integrate stream behavior.
- `src/DeltaLakeSharp.Client/Internal/Native/NativeMethods.net8.cs` - add source-generated P/Invoke declarations for async operation and stream lifecycle exports.
- `src/DeltaLakeSharp.Client/Internal/Native/NativeMethods.net472.cs` - add legacy framework P/Invoke declarations matching the new ABI.
- `src/DeltaLakeSharp.Client/Internal/Native/NativeEngineHandle.cs` - ensure engine lifetime is safe while async operation handles or streams are pending.
- `src/DeltaLakeSharp.Client/Internal/IDeltaLakeBackend.cs` - preserve the existing backend contract and use it as the operation inventory.
- `src/DeltaLakeSharp.Client/Internal/ArrowStreamDataReader.cs` - verify sync `DbDataReader` reads interact safely with prefetching streams.
- `src/DeltaLakeSharp.Adbc/Internal/DeltaAdbcClientAdapter.cs` - verify ADBC read paths benefit from prefetch and preserve read-only behavior.
- `src/DeltaLakeSharp.Server/v3/src/interop/native.rs` - define operation handles, lifecycle exports, callback/completion strategy, cancellation, and stream export integration.
- `src/DeltaLakeSharp.Server/v3/src/service/read.rs` - replace block-on-per-batch behavior with bounded prefetch stream state.
- `src/DeltaLakeSharp.Server/v3/src/service/write.rs` - apply async operation handles to insert/merge/write paths and cancellation where practical.
- `tests/DeltaLakeSharp.Tests/IntegrationScenarios/V3ClientSdkScenarioTests.cs` - public SDK scenario coverage for reads, SQL, partitions, CDF, and writes.
- `benchmarks/DeltaLakeSharp.Benchmark/DeltaReadPerformanceBenchmark.cs` - extend for concurrency and prefetch measurements.
- `docs/architecture/native-interop.md` - document revised native async and stream ownership model.
- `docs/architecture/execution-model.md` - document concurrency and backpressure behavior.
- `docs/architecture/adbc.md` - document any ADBC implications from async native and prefetch behavior.

## Verification

1. Build managed projects:

   ```powershell
   dotnet build DeltaLakeSharp.sln /p:SkipRustBuild=true -m:1
   ```

2. Run Rust validation from `src/DeltaLakeSharp.Server/v3`:

   ```powershell
   cargo test
   ```

3. Run focused V3 tests on macOS ARM64:

   ```powershell
   dotnet test tests/DeltaLakeSharp.Tests/DeltaLakeSharp.Tests.csproj --framework net8.0 --arch arm64 --filter "TestCategory=V3" /p:PlatformTarget=arm64 /p:SkipRustBuild=true
   ```

4. Add and run stress tests for operation cancellation, early stream disposal, engine disposal with pending operations, concurrent stream consumption, and producer error propagation.

5. Benchmark current vs optimized V3 under concurrency levels such as 1, 4, 8, 16, and 32, and with batch sizes such as default, 1k, 8k, and 64k rows.

6. Confirm one-shot operations complete via `Task` without tying up managed ThreadPool threads for the full native operation duration.

7. Confirm stream prefetch remains bounded under many concurrent readers and does not silently convert streaming reads into materialization.

8. Confirm errors and cancellation are surfaced as managed exceptions or results consistent with existing V3 behavior.

## Decisions

- Keep embedded V3 native as the primary performance path.
- Do not switch to same-machine Flight IPC for this optimization effort.
- Optimize long one-shot operations with async operation handles.
- Optimize streaming reads with Rust-owned bounded prefetch queues.
- Preserve public SDK and ADBC surface compatibility where possible.
- Prefer bounded queues and explicit backpressure over unbounded concurrency.
- Treat cancellation, disposal, and ownership as core design requirements.

## Open Questions

1. Should the first async operation milestone use callbacks, a native completion queue, or polling?
2. Should prefetch limits be internal constants, environment-configurable diagnostics, or public client options?
3. Should byte-aware prefetch limits be implemented immediately or after batch-count limits are benchmarked?
4. Should async operation handles be introduced behind parallel exports first, keeping synchronous exports for compatibility during migration?
5. Should this plan be promoted to an ADR before implementation, given the ABI and lifecycle implications?