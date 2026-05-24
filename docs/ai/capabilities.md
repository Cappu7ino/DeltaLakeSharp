# DeltaLakeSharp Capabilities

## Runtime Positioning

V3 Rust is the preferred and de-facto runtime for the client SDK and the required runtime behind the ADBC offering. V1 Spark and V2 DataFusion are still public `ServiceMode` choices for callers that already operate Arrow Flight services, but they should be treated as service-backed compatibility backends that are primarily exercised through the testing harness and integration infrastructure.

Agents generating new external NuGet integrations should choose V3 unless the task explicitly says to connect to a V1 or V2 Flight endpoint.

## Backend Capability Matrix

| Capability | V1 Spark | V2 DataFusion | V3 Rust | ADBC |
| --- | --- | --- | --- | --- |
| Basic table reads | Supported | Supported | Supported | Supported |
| SQL reads | Supported | Supported | Supported | Supported through statement queries |
| `DbDataReader` consumption | Supported through client | Supported through client | Supported through client | Not the ADBC surface |
| Arrow stream consumption | Supported | Supported | Supported | Primary result model |
| Create table | Supported | Supported with delta-rs caveats | Supported | Not supported |
| Insert append/overwrite | Supported | Supported | Supported | Not supported |
| DELETE / UPDATE SQL | Supported | Supported | Supported | Not supported |
| MERGE SQL | Supported | Supported | Use `MergeDataAsync` instead | Not supported |
| Streaming merge data | Supported | Supported | Supported | Not supported |
| Change Data Feed read/query | Not supported by public wrapper | Not supported by public wrapper | Supported | Supported for direct read and `_cdf` query |
| Partition planning/read APIs | Not supported by public wrapper | Not supported by public wrapper | Supported | Supported with restrictions |
| `WriteSchemaMode.Merge` / `Overwrite` | Not supported by public wrapper | Not supported by public wrapper | Supported | Not supported |
| Protocol upgrade | Supported | Supported | Supported | Not supported |
| Time-travel/versioned read | Supported by API path | Supported by API path | Supported | Supported through options |
| OneLake/SAS storage options | Supported | Supported | Supported | Supported through storage options |

## Core Read Workloads

Purpose:

- Read Delta table snapshots from local paths or supported cloud/object-store paths.
- Stream results as Arrow batches for efficient processing.
- Provide row-oriented views for consumers that expect `DbDataReader`.

Recommended usage:

- Prefer `ReadTableAsync` for large reads and pipeline composition.
- Use `ReadTableAsDataReaderAsync` when integrating with APIs that expect `DbDataReader`.
- Use `ReadTableAsArrowStreamAsync` for Arrow-native consumers.
- Pass `batchSize` when consumers need predictable batch sizing.
- Pass `version` for explicit time-travel reads.

Constraints:

- `batchSize` must be positive when provided.
- `ToListAsync`, `ToDataTableAsync`, and dictionary materialization buffer data in memory.
- Backend availability and table feature support differ by mode.

## SQL Query Workloads

Purpose:

- Execute SELECT-style queries and metadata commands against Delta tables.
- Register a table path under a logical table name when the backend requires it.

Recommended usage:

- Provide `tablePath` and `tableName` for SQL over a Delta table.
- Keep SQL dialect assumptions backend-specific; Spark, DataFusion, and native delta-rs behavior can diverge.
- Use streaming return types for large query results.

Constraints:

- SQL support is not a cross-engine guarantee for every dialect feature.
- ADBC exposes SQL reads but rejects update/write paths.

## Write And DML Workloads

Purpose:

- Create Delta tables.
- Insert Arrow `RecordBatch` streams with append or overwrite semantics.
- Execute DELETE, UPDATE, and MERGE workflows where supported.

Recommended usage:

- Build a `TableSchema` that matches outgoing Arrow batches.
- Use `SaveMode.Append` for incremental writes and `SaveMode.Overwrite` for replacement writes.
- Use `WriteSchemaMode.Merge` or `WriteSchemaMode.Overwrite` only when using V3 public client APIs.
- Use `MergeDataAsync` for streaming merge sources, especially with V3.

Constraints:

- SQL DML helpers validate statement prefixes and reject mismatched operation strings.
- ADBC is read-only.
- Schema evolution is not uniformly available across backend wrappers.
- Protocol/config acceptance is not the same as full feature implementation.

## Change Data Feed

Purpose:

- Read row-level changes between Delta versions.
- Query the `_cdf` relation for filtered change events.

Recommended usage:

- Use V3 `ReadChangeDataAsync` or `ExecuteChangeDataQueryAsync` for C# client workflows.
- Set `startingVersion`; set `endingVersion` only when a closed range is required.
- For ADBC, use CDF statement options or SQL against `_cdf`.

Constraints:

- V1/V2 public wrappers throw `NotSupportedException` for CDF APIs.
- `startingVersion` must be non-negative.
- `endingVersion` must be greater than or equal to `startingVersion`.
- ADBC partitioned execution is not compatible with CDF mode.

## Partitioned Reads

Purpose:

- Plan a Delta snapshot into opaque read partitions.
- Read partitions independently for distributed or parallel consumers.

Recommended usage:

- Use V3 `GetReadPartitionsAsync` to obtain `DeltaReadPartition` descriptors.
- Treat partition tokens as opaque; do not construct or mutate them manually.
- Read partitions from the same table snapshot they were generated for and consume descriptors promptly.
- Use ADBC `ExecutePartitioned` only for supported read modes.

Constraints:

- V1/V2 public wrappers throw for partition APIs.
- ADBC partitioned execution has restrictions with CDF, `MaxRows`, and some deletion-vector layouts.
- Partition tokens are trusted, short-lived backend execution descriptors. They can encode scan metadata and should not be logged, mutated, or persisted as long-term stable identifiers.

## Storage And Authentication

Purpose:

- Provide per-request storage credentials and object-store behavior.
- Support OneLake/ABFSS and SAS-based access paths.

Recommended usage:

- Use `StorageConfig` for storage account plus SAS-token flows.
- Use `GenericStorageOptions` for delta-rs/native object-store option pass-through.
- Prefer per-request options over global process configuration.

Constraints:

- `StorageConfig.EvictFileSystemCache` is primarily a V1 Spark cache behavior knob.
- OneLake endpoint routing depends on backend object-store support.
- Do not log SAS tokens or generated storage option dictionaries containing secrets.

## ADBC Integration

Purpose:

- Expose one Delta table through an Arrow-native ADBC driver backed by the V3 Rust path.
- Support direct table reads, SQL reads, metadata discovery, versioned reads, partition execution, and CDF read/query flows.

Recommended usage:

- Configure the table path through ADBC options.
- Treat the logical table as the synthetic `delta_table` table.
- Use ADBC when the consumer ecosystem already speaks Arrow Database Connectivity.

Constraints:

- ADBC is read-only in this repository.
- Prepared statements, parameter binding, writes, transactions, statistics, and real catalog discovery are not implemented.
- Some option combinations are rejected to avoid ambiguous semantics.
