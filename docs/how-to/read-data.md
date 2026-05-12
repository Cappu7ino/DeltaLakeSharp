# How To Read Data

## Goal

Read Delta table data using the most appropriate result shape for the downstream consumer.

## Prefer Arrow Batch Streaming

```csharp
using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);

await foreach (RecordBatch batch in client.ReadTableAsync(
    tablePath,
    batchSize: 8192,
    cancellationToken: cancellationToken))
{
    // Process one batch at a time.
}
```

Use this pattern for large or unknown-size datasets.

## Read A Specific Table Version

```csharp
await foreach (RecordBatch batch in client.ReadTableAsync(
    tablePath,
    version: 42,
    cancellationToken: cancellationToken))
{
    // Process version-pinned data.
}
```

Versioned reads help make integrations deterministic.

## Use `DbDataReader` For Row-Oriented Consumers

```csharp
var readerOptions = new DeltaDataReaderOptions
{
    DecimalBehavior = DeltaDataReaderDecimalBehavior.OverflowDecimalAsString,
};

using DbDataReader reader = await client.ReadTableAsDataReaderAsync(
    tablePath,
    options: readerOptions,
    cancellationToken: cancellationToken);

while (await reader.ReadAsync(cancellationToken))
{
    // Forward-only row consumption.
}
```

Use this only when an API needs `DbDataReader`.

## Use Arrow Streams For Arrow-Native Consumers

```csharp
using IArrowArrayStream stream = await client.ReadTableAsArrowStreamAsync(
    tablePath,
    cancellationToken: cancellationToken);
```

Dispose Arrow streams promptly.

## Materialize Only Bounded Results

Use materialization helpers for tests, demos, or bounded administrative results:

```csharp
DataTable table = await client.ReadTableAsync(tablePath).ToDataTableAsync(cancellationToken);
```

Do not make this the default for large tables.

## Query With SQL

```csharp
await foreach (RecordBatch batch in client.ExecuteQueryAsync(
    "SELECT id, name FROM delta_table WHERE id > 10",
    tablePath: tablePath,
    tableName: "delta_table",
    cancellationToken: cancellationToken))
{
    // Process query result.
}
```

Provide `tablePath` and `tableName` when the backend needs to register the table for the SQL query.
