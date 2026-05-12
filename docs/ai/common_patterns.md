# Common Integration Patterns

Use these patterns when generating code or guidance for consumers of `Microsoft.DI.DeltaTableService.Client` and `Microsoft.DI.DeltaTableService.Adbc`.

## Choose V3 For New SDK Consumers

Purpose:

- Use the preferred in-process SDK runtime.
- Avoid requiring a caller to host or discover a Flight service.
- Unlock CDF, partitioned reads, schema-mode writes, and ADBC-backed behavior.

Pattern:

```csharp
using Microsoft.DI.DeltaTableService.Client;

using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
```

Use V1/V2 only when the user explicitly provides an existing Arrow Flight endpoint or needs compatibility coverage for those service-backed paths.

## Stream Table Reads

Purpose:

- Keep memory bounded for large Delta tables.
- Preserve Arrow-native columnar processing.

Pattern:

```csharp
await foreach (RecordBatch batch in client.ReadTableAsync(tablePath, cancellationToken: cancellationToken))
{
    // Process one Arrow batch at a time.
}
```

Notes:

- Prefer streaming over `ToDataTableAsync` for unknown-size tables.
- Pass `batchSize` when downstream consumers need predictable batch boundaries.
- Pass `version` for deterministic time-travel reads.

## Use `DbDataReader` As An Adapter

Purpose:

- Integrate with .NET APIs that expect row-oriented `DbDataReader`.
- Keep the SDK source of truth Arrow-first.

Pattern:

```csharp
var options = new DeltaDataReaderOptions
{
    DecimalBehavior = DeltaDataReaderDecimalBehavior.OverflowDecimalAsString,
};

using DbDataReader reader = await client.ReadTableAsDataReaderAsync(
    tablePath,
    options: options,
    cancellationToken: cancellationToken);

while (await reader.ReadAsync(cancellationToken))
{
    // Consume forward-only rows.
}
```

Notes:

- Choose decimal behavior deliberately.
- Treat the reader as forward-only and single-consumer.

## Pass Storage Options Per Request

Purpose:

- Avoid global mutable credential state.
- Support multiple accounts or credential scopes in one process.

Pattern:

```csharp
var storageConfig = new StorageConfig(
    storageAccount: "onelake",
    sasToken: sasToken,
    evictFileSystemCache: false);

await foreach (RecordBatch batch in client.ReadTableAsync(
    tablePath,
    storageConfig: storageConfig,
    cancellationToken: cancellationToken))
{
    // Process batch.
}
```

For delta-rs/native options, prefer `GenericStorageOptions` or `GenericStorageOptions.FromStorageConfig(storageConfig)`.

## Query Change Data Feed With V3

Purpose:

- Read row-level changes between Delta versions.
- Keep agents from choosing unsupported V1/V2 APIs.

Pattern:

```csharp
await foreach (RecordBatch batch in client.ReadChangeDataAsync(
    tablePath,
    startingVersion: 1,
    endingVersion: 5,
    cancellationToken: cancellationToken))
{
    // Read _change_type, _commit_version, and data columns.
}
```

Notes:

- Use `ServiceMode.V3_Rust`.
- `startingVersion` must be non-negative.
- `endingVersion` must be greater than or equal to `startingVersion`.

## Plan Then Read Partitions

Purpose:

- Split V3 reads into independent partition descriptors.
- Enable parallel or distributed consumption without inventing partition tokens.

Pattern:

```csharp
IReadOnlyList<DeltaReadPartition> partitions = await client.GetReadPartitionsAsync(
    tablePath,
    cancellationToken: cancellationToken);

foreach (DeltaReadPartition partition in partitions)
{
    await foreach (RecordBatch batch in client.ReadTablePartitionAsync(
        tablePath,
        partition,
        cancellationToken: cancellationToken))
    {
        // Process partition batch.
    }
}
```

Notes:

- Treat `DeltaReadPartition.Token` as opaque.
- Use descriptors against the same snapshot semantics they were planned for.

## Use ADBC For Read-Only Arrow Consumers

Purpose:

- Integrate with Arrow Database Connectivity consumers.
- Expose one Delta table as a path-scoped logical table.

Pattern:

- Configure the table path through `delta.table_uri`.
- Query the synthetic `delta_table` table.
- Use ADBC CDF and partition options only in supported combinations.

Notes:

- ADBC is V3-backed.
- ADBC does not implement writes, transactions, prepared statements, or real catalog discovery.

## Treat Protocol Upgrades As Explicit Operations

Purpose:

- Avoid irreversible table changes in ordinary setup code.

Pattern:

- Call `UpgradeTableProtocolAsync` only when the user explicitly asks to enable a table feature.
- Document the reader/writer versions and feature flags being enabled.
- Validate downstream readers before upgrading shared tables.
