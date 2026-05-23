# AI Bootstrap For DeltaLakeSharp SDK Integration

## Repository Identity

- Primary deliverable: .NET NuGet packages for Delta Lake access.
- Main package: `DeltaLakeSharp.Client`.
- Secondary package: `DeltaLakeSharp.Adbc` for read-only Arrow Database Connectivity.
- Data model: Apache Arrow batches and streams first; row APIs are adapters.

## Backend Selection Rules

- For new external NuGet integrations, prefer `ServiceMode.V3_Rust` unless the user explicitly needs an existing V1/V2 Flight service.
- `ServiceMode.V3_Rust` is the de-facto client SDK runtime and the required runtime behind the ADBC offering.
- Default client constructor with URI uses `ServiceMode.V1_Spark`.
- Use `ServiceMode.V1_Spark` for Spark-backed Flight service compatibility and integration-test workflows.
- Use `ServiceMode.V2_DataFusion` for DataFusion/delta-rs Flight service compatibility and integration-test workflows.
- Use `ServiceMode.V3_Rust` for in-process native execution, CDF public APIs, partitioned reads, and schema-mode writes.
- Use ADBC only for read-only Arrow-native consumers.

## Preferred Patterns

- Wrap `DeltaTableServiceClient` in `using` or dispose it explicitly.
- Prefer `await foreach` over materializing full result sets.
- Pass `CancellationToken` through SDK calls.
- Use `ReadTableAsDataReaderAsync` only when a downstream API needs `DbDataReader`.
- Use `ReadTableAsArrowStreamAsync` for Arrow-native integrations.
- Use `GenericStorageOptions` for delta-rs/native object-store options.
- Use `StorageConfig` for storage-account/SAS compatibility flows.
- Use `MergeDataAsync` for streaming source-data merge workflows.
- Use V3 partition planning before parallel partition reads.

## Forbidden Or Risky Patterns

- Do not buffer unknown-size tables with `ToListAsync` or `ToDataTableAsync` by default.
- Do not assume all backends support every method on `DeltaTableServiceClient`.
- Do not use V1/V2 for CDF or partition APIs.
- Do not present V1/V2 as the recommended default for new external SDK consumers.
- Do not log SAS tokens or storage option dictionaries.
- Do not construct `DeltaReadPartition` values from guessed tokens.
- Do not treat ADBC as a write-capable driver.
- Do not call protocol upgrades unless the user explicitly asks to change table protocol/features.
- Do not assume SQL dialect parity across Spark, DataFusion, and native Rust.

## Critical Invariants

- `batchSize`, when supplied, must be positive.
- CDF `startingVersion` must be non-negative.
- CDF `endingVersion` must be greater than or equal to `startingVersion`.
- DELETE/UPDATE/MERGE helper methods require SQL that starts with the corresponding operation.
- Partition descriptors are tied to the planned snapshot and backend implementation.
- V3 native runtime requires a discoverable native library.
- ADBC exposes a synthetic single-table namespace, commonly `delta_table`.

## Performance Expectations

- Streaming Arrow batches is the scalable default.
- Row and dictionary conversion trade performance for convenience.
- Flight mode performance includes service and gRPC overhead.
- V3 avoids the Flight service boundary but introduces native FFI ownership concerns.
- V3 engine handles share one process-wide Tokio runtime; avoid long blocking work inside imported managed Arrow streams because it can affect other V3 clients in the process.
- Partitioned reads are for parallel consumers; they are not a general replacement for simple reads.

## Naming And Terminology

- `V1_Spark`: Spark/Delta Lake Flight backend for service-backed compatibility and tests.
- `V2_DataFusion`: DataFusion/delta-rs Flight backend for service-backed compatibility and tests.
- `V3_Rust`: native Rust in-process backend; preferred SDK and ADBC runtime.
- CDF: Change Data Feed.
- DML: DELETE, UPDATE, MERGE operations.
- `StorageConfig`: legacy typed storage account/SAS options.
- `GenericStorageOptions`: dictionary pass-through options.
- `DeltaReadPartition`: opaque partition descriptor.

## Verification Checklist For Generated Code

- Backend mode matches the requested capability.
- Client is disposed.
- Large reads remain streaming.
- Storage credentials are passed per request and not logged.
- CDF and partition APIs use V3 or ADBC-supported paths.
- ADBC examples are read-only.
- Protocol upgrades are opt-in and described as irreversible.
- Code compiles against the intended target framework.
