# DeltaLakeSharp Limitations

This file is intentionally direct. Use it to prevent incorrect SDK integrations and overconfident agent-generated code.

## Runtime Positioning Boundary

- V3 Rust is the preferred runtime for new external client SDK integrations.
- ADBC is V3-backed and should be documented as a native Rust offering.
- V1 Spark and V2 DataFusion remain public `ServiceMode` values, but they require service endpoints and are primarily exercised by test harness and integration infrastructure.
- Do not describe V1/V2 as removed, private, or impossible for consumers; describe them as service-backed compatibility paths.

## Backend Capability Boundaries

| Boundary | Impact | Correct Pattern |
| --- | --- | --- |
| V1/V2 public wrappers do not expose CDF APIs. | `ReadChangeDataAsync` and CDF query variants throw `NotSupportedException`. | Use V3 Rust or ADBC CDF options. |
| V1/V2 public wrappers do not expose partition planning/read APIs. | Distributed partition reads cannot be generated through the C# public wrapper for Flight backends. | Use V3 `GetReadPartitionsAsync` and partition read methods. |
| V1/V2 public wrappers do not expose `WriteSchemaMode` APIs. | Schema merge/overwrite write modes are unavailable through those wrappers. | Use V3 for schema evolution write modes. |
| V3 does not support SQL `MergeAsync` in the same way as Flight backends. | SQL MERGE through that backend path is rejected. | Use `MergeDataAsync` with streaming source batches. |
| ADBC is read-only. | Writes, updates, deletes, prepared statements, transactions, and parameter binding are unavailable. | Use `DeltaTableServiceClient` for mutations. |

## Protocol And Delta Feature Caveats

- Delta protocol upgrades are irreversible. Generated integrations must not call `UpgradeTableProtocolAsync` casually.
- Delta configuration flags are not a guarantee that every backend fully implements the feature.
- Column mapping and deletion-vector behavior depends on the backend and table layout.
- Type widening may be readable for existing fixtures but write/create flows can reject unsupported configuration.
- Schema evolution can alter downstream row shapes. Generated consumers should avoid assuming fixed ordinals after append/merge operations.

## Storage And Authentication Caveats

- SAS tokens and storage option dictionaries are sensitive. Do not log them.
- `StorageConfig` is per-request; do not treat it as a global connection state object.
- `StorageConfig.EvictFileSystemCache` is mainly relevant to Spark filesystem cache behavior.
- OneLake routing may require backend-specific options such as Fabric endpoint handling.
- A table path alone may not be enough for cloud reads; storage credentials must match the backend and URI scheme.

## Concurrency Caveats

- The public APIs are asynchronous, but backend execution does not imply unlimited parallelism.
- Flight clients own a gRPC channel per `DeltaTableServiceClient` instance.
- V3 native calls cross a synchronous FFI boundary and use a native engine handle.
- Avoid issuing multiple concurrent native operations through the same client unless the caller has validated backend behavior for that scenario.
- Partitioned reads are the preferred pattern for independent parallel reads on V3.

## Memory And Streaming Caveats

- Arrow `RecordBatch` streaming is the preferred large-result pattern.
- `ToListAsync`, `ToDataTableAsync`, and dictionary conversion materialize results and can create large allocations.
- `DbDataReader` is forward-only. Do not generate code that expects rewind, random access, or multiple enumeration.
- Arrow C stream ownership is delicate in V3 native interop; consumers should dispose streams/readers promptly.
- Decimal conversion can overflow .NET `decimal`; configure `DeltaDataReaderOptions.DecimalBehavior` deliberately.

## SQL And Query Caveats

- SQL dialect behavior can differ across Spark, DataFusion, and native Rust paths.
- DELETE, UPDATE, and MERGE helper methods validate operation prefixes and reject mismatched SQL.
- SQL queries over table paths may require `tablePath` plus `tableName` registration.
- CDF SQL queries must use the relation expected by the backend, such as `_cdf` for CDF reads.

## Deployment Caveats

- V1 and V2 require reachable Arrow Flight services, usually container-backed in integration tests.
- V3 requires the native Rust library to be built and discoverable at runtime.
- `net472` and `netstandard2.0` builds use the legacy native interop implementation, not source-generated .NET 8 interop.
- Compile-only validation can skip native Rust rebuilds, but runtime V3 validation cannot.

## ADBC Caveats

- The logical table name is synthetic and path-scoped; this is not a real multi-table catalog.
- SQL query plus `MaxRows` is rejected; row limits apply to direct table scans.
- Partitioned execution with CDF is rejected.
- Partitioned execution with some deletion-vector layouts is rejected.
- The driver is suitable for read-oriented Arrow consumers, not write orchestration.

## Anti-Misuse Rules For Agents

- Do not choose V1/V2 for CDF, partition APIs, or public schema-mode write APIs.
- Do not generate code that buffers unknown-size tables by default.
- Do not construct partition tokens manually.
- Do not log storage credentials.
- Do not assume ADBC supports writes or transactions.
- Do not call protocol upgrade APIs as part of normal read/write setup.
- Do not hide `NotSupportedException`; use it to select the correct backend or fail clearly.
