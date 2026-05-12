# How To Troubleshoot Integrations

## Native Library Not Found

Symptoms:

- V3 client creation or health check fails.
- Error mentions native library loading.
- ADBC read fails before query execution.

Checks:

- Confirm the native Rust library is copied to the application output.
- Confirm the runtime identifier matches the package asset path.
- Confirm local development builds produced the native library when running from source.

Relevant source:

- [../../src/DeltaLakeSharp.Client/Internal/Native/NativeMethods.cs](../../src/DeltaLakeSharp.Client/Internal/Native/NativeMethods.cs)
- [../../src/DeltaLakeSharp.Client/Internal/Native/NativeEngineHandle.cs](../../src/DeltaLakeSharp.Client/Internal/Native/NativeEngineHandle.cs)

## `NotSupportedException` From V1 Or V2

Symptoms:

- CDF API fails.
- Partitioned read API fails.
- Schema-mode write API fails.

Cause:

- The public client type exposes shared methods, but V1/V2 service-backed wrappers do not implement V3-only capabilities.

Fix:

- Use `ServiceMode.V3_Rust` for CDF, partitioned reads, and schema-mode writes.
- Keep V1/V2 for explicit Flight service compatibility scenarios.

## Large Memory Usage

Symptoms:

- High memory usage during reads.
- Slow conversion to `DataTable` or dictionaries.

Cause:

- Materializing full result sets instead of streaming Arrow batches.

Fix:

- Use `await foreach` over `ReadTableAsync`.
- Use `DbDataReader` only for row-oriented consumers.
- Materialize only bounded data.

## Decimal Conversion Errors

Symptoms:

- Decimal overflow or unexpected decimal value type from `DbDataReader`.

Cause:

- Delta decimals can exceed .NET `decimal` expectations.

Fix:

- Set `DeltaDataReaderOptions.DecimalBehavior` explicitly.
- Use `OverflowDecimalAsString` when downstream systems can accept string fallback.

## SQL Query Cannot Find Table

Symptoms:

- Query fails to resolve table name.

Cause:

- Backend may need the Delta path registered under a logical table name.

Fix:

```csharp
await foreach (RecordBatch batch in client.ExecuteQueryAsync(
    sql,
    tablePath: tablePath,
    tableName: "delta_table",
    cancellationToken: cancellationToken))
{
    // Process results.
}
```

## ADBC Write Or Transaction Failure

Symptoms:

- `ExecuteUpdate`, prepared statements, or transactions fail.

Cause:

- ADBC package is read-only and path-scoped.

Fix:

- Use `DeltaTableServiceClient` for writes and DML.
- Use ADBC only for read-only Arrow-native consumption.

## Storage Authentication Failure

Symptoms:

- Cloud path reads fail with authorization or object-store errors.

Checks:

- Verify table URI scheme and storage account.
- Verify SAS token scope and expiry.
- Verify `StorageConfig` or `GenericStorageOptions` are passed to the operation.
- Do not rely on credentials from a different operation.

Fix:

- Pass credentials per request.
- Redact credentials in logs.
