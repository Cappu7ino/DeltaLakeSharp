# DeltaLakeSharp Semantic Public API Inventory

This inventory explains API intent and usage semantics. It is not a signature dump.

## Package Surface

| Package | Purpose | Primary Consumers |
| --- | --- | --- |
| `DeltaLakeSharp.Client` | High-level C# SDK for Delta table reads, SQL, writes, DML, CDF, partitions, and native execution. V3 Rust is the preferred external NuGet runtime; V1/V2 remain public service-backed compatibility modes. | .NET applications, services, tests, data tools. |
| `DeltaLakeSharp.Adbc` | Read-only ADBC driver over Delta tables backed by the V3 Rust path. | Arrow-native query engines and ADBC consumers. |
| `DeltaLakeSharp.Testing` | Testcontainers/native test support. | Repository tests and downstream validation infrastructure. |

## `DeltaTableServiceClient`

Source: [../src/DeltaLakeSharp.Client/DeltaTableServiceClient.cs](../src/DeltaLakeSharp.Client/DeltaTableServiceClient.cs)

Responsibility:

- Select and own a backend implementation.
- Provide public read, query, write, DML, CDF, partition, schema, and protocol APIs.
- Convert backend streams into Arrow, `DbDataReader`, or materialized helper forms.

Lifecycle:

- Implements `IDisposable`.
- Flight modes own a gRPC channel through `FlightClientWrapper`.
- V3 mode owns a native Rust engine handle through `NativeRustBackend`.

Intended usage:

- Prefer `new DeltaTableServiceClient(ServiceMode.V3_Rust)` for new external SDK integrations unless the consumer explicitly needs an existing Flight endpoint.
- `new DeltaTableServiceClient(uri)` for default V1 Flight service.
- `new DeltaTableServiceClient(uri, ServiceMode.V2_DataFusion)` for DataFusion Flight service.
- `new DeltaTableServiceClient(ServiceMode.V3_Rust)` for native in-process mode.
- `new DeltaTableServiceClient(ServiceMode.V3_Rust, options)` for V3 native mode with client-level tuning.
- Dispose after use.

Common pitfalls:

- Assuming default constructor behavior selects V3.
- Presenting V1/V2 as the recommended default for new NuGet consumers.
- Calling V3-only methods on V1/V2 modes.
- Materializing large streams through convenience helpers.
- Ignoring storage options for cloud paths.

Related APIs:

- `ServiceMode`
- `DeltaTableServiceClientOptions`
- `StorageConfig`
- `GenericStorageOptions`
- `DeltaDataReaderOptions`
- `DeltaReadPartition`
- `MergeOptions`

## `ServiceMode`

Responsibility:

- Encodes the backend execution mode: Spark Flight, DataFusion Flight, or native Rust.
- Documents public compatibility with V1/V2 while letting integrations prefer V3 for SDK and ADBC scenarios.

Lifecycle:

- Immutable enum value supplied at client construction.

Thread safety:

- Value type; no state.

Common pitfalls:

- Treating backend modes as feature-equivalent.
- Choosing V1/V2 for CDF, partitions, or schema-mode write APIs.

## `DeltaTableServiceClientOptions`

Source: [../src/DeltaLakeSharp.Client/DeltaTableServiceClient.cs](../src/DeltaLakeSharp.Client/DeltaTableServiceClient.cs)

Responsibility:

- Configure client-level backend behavior at construction time.
- Provide V3 native tuning without changing individual read method signatures.

Current options:

- `EnableNativeReadPrefetch` enables the experimental V3 native read-stream prefetch path.

Intended usage:

- Leave `EnableNativeReadPrefetch` disabled by default for general local reads.
- Enable prefetch deliberately for benchmark or workload validation where read-ahead may help, such as high-latency storage or slower managed consumers.

Common pitfalls:

- Assuming prefetch always improves local or small-table reads.
- Treating V3-specific tuning as meaningful for V1/V2 Flight backends.

## Read APIs

Method families:

- `ReadTableAsync`
- `ReadTableAsDataReaderAsync`
- `ReadTableAsArrowStreamAsync`
- `ExecuteQueryAsync`
- `ExecuteQueryAsDataReaderAsync`
- `ExecuteQueryAsArrowStreamAsync`

Responsibility:

- Read Delta snapshots and query results as streaming Arrow data.

Lifecycle:

- Returned streams/readers should be consumed promptly.
- Row readers are forward-only.

Thread safety:

- Treat returned streams/readers as single-consumer objects.

Common pitfalls:

- Converting unknown-size results to `DataTable` or dictionaries.
- Assuming SQL dialect behavior is identical across backends.
- Omitting table registration parameters for SQL paths that need them.

## CDF APIs

Method families:

- `ReadChangeDataAsync`
- `ReadChangeDataAsDataReaderAsync`
- `ReadChangeDataAsArrowStreamAsync`
- `ExecuteChangeDataQueryAsync`
- `ExecuteChangeDataQueryAsDataReaderAsync`
- `ExecuteChangeDataQueryAsArrowStreamAsync`

Responsibility:

- Stream Delta Change Data Feed rows by version range or SQL query.

Intended usage:

- Use with V3 native client or ADBC CDF options.
- Provide explicit `startingVersion`.

Common pitfalls:

- Calling CDF APIs on V1/V2 Flight wrappers.
- Passing invalid version ranges.
- Querying CDF without using the backend's expected relation name.

## Partition APIs

Types and methods:

- `DeltaReadPartition`
- `GetReadPartitionsAsync`
- `ReadTablePartitionAsync`
- `ReadTablePartitionByTokenAsync`
- Arrow stream partition variants

Responsibility:

- Plan and read independent partitions for a pinned table snapshot.

Lifecycle:

- Partition descriptors are generated by the backend and consumed by follow-up reads.
- Tokens are opaque, trusted, short-lived backend execution descriptors.
- Tokens can encode backend scan metadata and should not be logged, mutated, or persisted.

Common pitfalls:

- Constructing tokens manually.
- Persisting tokens as long-term stable IDs.
- Logging or mutating tokens.
- Using V1/V2 modes for partition APIs.

## Write And DML APIs

Method families:

- `CreateTableAsync`
- `InsertAsync`
- `BeginDistributedWriteAsync`
- `StageDistributedWriteAsync`
- `CommitDistributedWriteAsync`
- `AbortDistributedWriteAsync`
- `DeleteAsync`
- `UpdateAsync`
- `MergeAsync`
- `MergeDataAsync`
- `UpgradeTableProtocolAsync`

Responsibility:

- Mutate Delta tables through schema creation, batch insertion, SQL DML, streaming merge, and protocol upgrades.

Intended usage:

- Supply `TableSchema` that matches outgoing Arrow batches.
- Use `SaveMode` for append/overwrite.
- Use `WriteSchemaMode` only where backend supports schema evolution modes.
- Use `MergeDataAsync` for streaming source data.
- Use distributed write APIs only with V3 native Rust; the current implementation supports existing-table append and keeps new-table creation, overwrite, and schema evolution for follow-up slices.

Common pitfalls:

- Calling protocol upgrade APIs during normal setup.
- Using SQL strings that do not match the helper method prefix.
- Assuming ADBC can perform writes.

## Distributed Write APIs

Types and methods:

- `DeltaDistributedWriteOptions`
- `DeltaDistributedWriteSession`
- `DeltaStagedWriteResult`
- `DeltaDistributedCommitOptions`
- `DistributedWriteTableDisposition`
- `DistributedOverwriteScope`
- `BeginDistributedWriteAsync`
- `StageDistributedWriteAsync`
- `CommitDistributedWriteAsync`
- `AbortDistributedWriteAsync`

Responsibility:

- Model a V3 worker/coordinator write workflow keyed by a caller-provided `Guid` run ID.
- Allow workers to stage uncommitted data files and Add-action artifacts.
- Allow one coordinator to commit staged artifacts atomically once the implementation is complete.

Lifecycle:

- Callers create a globally unique `Guid` run ID and pass it in `DeltaDistributedWriteOptions.RunId`.
- `BeginDistributedWriteAsync` returns a `DeltaDistributedWriteSession` that is shared by all workers and the coordinator.
- `StageDistributedWriteAsync`, `CommitDistributedWriteAsync`, and `AbortDistributedWriteAsync` currently support existing-table append only.
- New table creation, overwrite, touched-partition overwrite, and schema evolution are planned follow-up scopes.

Common pitfalls:

- Passing `Guid.Empty` as a run ID.
- Calling distributed write APIs on V1/V2 Flight modes.
- Assuming staged artifacts are committed before the coordinator commit completes.
- Treating staging cleanup as equivalent to Delta vacuum or data-file retention cleanup.

## `StorageConfig`

Source: [../src/DeltaLakeSharp.Client/Models/StorageConfig.cs](../src/DeltaLakeSharp.Client/Models/StorageConfig.cs)

Responsibility:

- Carry storage account and SAS-token settings for per-request access.

Lifecycle:

- Immutable per-call configuration object.

Common pitfalls:

- Logging SAS tokens.
- Treating `EvictFileSystemCache` as a universal backend behavior.

## `GenericStorageOptions`

Source: [../src/DeltaLakeSharp.Client/Models/GenericStorageOptions.cs](../src/DeltaLakeSharp.Client/Models/GenericStorageOptions.cs)

Responsibility:

- Carry dictionary-based object-store options, especially for delta-rs/native paths.

Intended usage:

- Use for backend-specific storage option pass-through.
- Use `FromStorageConfig` when migrating from typed storage config.

Common pitfalls:

- Assuming all keys are understood by all backends.
- Logging secret values.

## `DeltaDataReaderOptions`

Source: [../src/DeltaLakeSharp.Client/Models/DeltaDataReaderOptions.cs](../src/DeltaLakeSharp.Client/Models/DeltaDataReaderOptions.cs)

Responsibility:

- Configure row-reader decimal behavior.

Common pitfalls:

- Assuming high-precision Delta decimals always fit in .NET `decimal`.
- Ignoring overflow behavior in downstream integrations.

## `TableSchema` And `ColumnDefinition`

Responsibility:

- Describe table/write schema using logical column names, data type strings, and nullability.

Intended usage:

- Use for create and insert workflows.
- Keep schema aligned with outgoing Arrow batch fields.

Common pitfalls:

- Assuming schema evolution behavior is identical across backends.
- Reusing stale schemas after table evolution.

## `SaveMode` And `WriteSchemaMode`

Responsibility:

- `SaveMode` controls append versus overwrite writes.
- `WriteSchemaMode` controls schema merge versus schema overwrite where supported.

Common pitfalls:

- Confusing overwrite data semantics with overwrite schema semantics.
- Calling schema mode APIs on V1/V2 public wrappers.

## `MergeOptions`

Source: [../src/DeltaLakeSharp.Client/Models/MergeOptions.cs](../src/DeltaLakeSharp.Client/Models/MergeOptions.cs)

Responsibility:

- Configure streaming `MergeDataAsync` predicates, aliases, and matched/not-matched actions.

Common pitfalls:

- Omitting the target/source predicate.
- Mixing SQL MERGE expectations with streaming merge option semantics.

## `ExecuteResult`

Responsibility:

- Return success status, message, and optional row-dictionary results for DDL/DML/control operations.

Common pitfalls:

- Treating `Message` as structured data.
- Assuming `Result` is always present.

## `ArrowConverter` And `ReadStreamExtensions`

Responsibility:

- Convert Arrow batches to/from `DataTable`, row dictionaries, CSV, and async streams.

Intended usage:

- Use for small or bounded data conversion.
- Prefer Arrow streaming for large data.

Common pitfalls:

- Large in-memory materialization.
- Unsupported Arrow shapes such as dictionary index cases.

## ADBC Types

Types:

- `DeltaAdbcDriver`
- `DeltaAdbcConnection`
- `DeltaAdbcStatement`
- `DeltaAdbcConnectOptions`
- `DeltaAdbcStatementOptions`

Responsibility:

- Expose a path-scoped, read-only Delta table through ADBC.

Lifecycle:

- Connection creates statements.
- Statement options control table path, version, CDF, max rows, and storage options.

Common pitfalls:

- Expecting writes, transactions, prepared statements, or real catalog discovery.
- Combining partitioned execution with unsupported options such as CDF or max rows.
