# How To Use V3 Features

## Goal

Use public SDK capabilities that are V3-oriented: CDF, partitioned reads, schema-mode writes, and native execution.

## Create A V3 Client

```csharp
using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
```

Do not pass a Flight URI unless the integration intentionally targets V1 or V2 service-backed compatibility.

## Read Change Data Feed

```csharp
await foreach (RecordBatch batch in client.ReadChangeDataAsync(
    tablePath,
    startingVersion: 1,
    endingVersion: 5,
    cancellationToken: cancellationToken))
{
    // Inspect _change_type, _commit_version, _commit_timestamp, and data columns.
}
```

Rules:

- `startingVersion` must be non-negative.
- `endingVersion` must be greater than or equal to `startingVersion`.
- The table must have CDF enabled.

## Query Change Data Feed

```csharp
await foreach (RecordBatch batch in client.ExecuteChangeDataQueryAsync(
    "SELECT id, _change_type FROM _cdf WHERE _change_type <> 'update_preimage'",
    tablePath,
    startingVersion: 1,
    endingVersion: null,
    cancellationToken: cancellationToken))
{
    // Process filtered CDF rows.
}
```

Use `_cdf` as the CDF relation in CDF SQL queries.

## Plan And Read Partitions

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
        // Process partition rows.
    }
}
```

Rules:

- Treat partition tokens as opaque.
- Do not construct partition descriptors manually.
- Keep planning and reading tied to the intended snapshot.

## Use Schema Modes On Writes

```csharp
await client.InsertAsync(
    tablePath,
    schema,
    batches,
    mode: SaveMode.Append,
    schemaMode: WriteSchemaMode.Merge,
    cancellationToken: cancellationToken);
```

Use schema modes only when the selected backend supports them. In current public wrapper behavior, this is a V3 path.

## Prefer `MergeDataAsync` For Streaming Merge

```csharp
var mergeOptions = new MergeOptions(
    predicate: "target.id = source.id",
    sourceAlias: "source",
    targetAlias: "target")
{
    WhenMatchedUpdateAll = true,
    WhenNotMatchedInsertAll = true,
};

ExecuteResult result = await client.MergeDataAsync(
    tablePath,
    schema,
    sourceBatches,
    mergeOptions,
    cancellationToken: cancellationToken);
```

This is clearer for generated code than constructing raw SQL MERGE strings with streaming source data.
