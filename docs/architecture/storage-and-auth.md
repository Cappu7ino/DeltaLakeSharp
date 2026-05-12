# Storage And Authentication Model

## Summary

DeltaTableService passes storage credentials and object-store options per operation. This avoids global mutable credential state and supports multiple storage scopes in one process.

## Storage Option Types

| Type | Purpose | Typical Backend |
| --- | --- | --- |
| `StorageConfig` | Typed storage account and SAS-token configuration. | V1 compatibility and simple SDK consumers. |
| `GenericStorageOptions` | Dictionary pass-through for object-store options. | V3/delta-rs/native paths. |
| ADBC options | Connection/statement keys for table URI, version, storage, and Azure aliases. | ADBC read consumers. |

Sources:

- [../../src/DeltaTableService.Client/Models/StorageConfig.cs](../../src/DeltaTableService.Client/Models/StorageConfig.cs)
- [../../src/DeltaTableService.Client/Models/GenericStorageOptions.cs](../../src/DeltaTableService.Client/Models/GenericStorageOptions.cs)
- [../../src/DeltaTableService.Adbc/DeltaAdbcConnectOptions.cs](../../src/DeltaTableService.Adbc/DeltaAdbcConnectOptions.cs)

## Per-Request Pattern

Most client APIs accept storage options directly. Prefer passing credentials at the call site:

```csharp
await foreach (RecordBatch batch in client.ReadTableAsync(
    tablePath,
    storageConfig: storageConfig,
    genericStorageOptions: genericOptions,
    cancellationToken: cancellationToken))
{
    // Process batch.
}
```

Guidance:

- Do not store credentials in static process-wide state.
- Do not log SAS tokens or full option dictionaries.
- Keep table URI, storage account, and credential lifetime aligned.

## `StorageConfig`

Fields:

- storage account name
- SAS token
- file-system cache eviction flag

`EvictFileSystemCache` is primarily a V1 Spark compatibility knob for SAS rotation and Hadoop filesystem cache behavior. Do not present it as a universal cache control.

## `GenericStorageOptions`

Purpose:

- Carry backend-specific key-value options.
- Support delta-rs/native object-store integrations.
- Convert typed `StorageConfig` through `FromStorageConfig` where appropriate.

Risk:

- Option names and meanings are backend-specific.
- Secret values can be embedded in the dictionary.

## OneLake And SAS

OneLake/ABFSS flows commonly require:

- table URI using the correct scheme
- storage account or endpoint context
- SAS token or generated SAS
- backend-specific endpoint handling for Fabric/OneLake paths

`OneLakeSasHelper` supports SAS generation flows in the client package. Keep token generation, token use, and token logging separate in generated code.

## ADBC Storage Options

ADBC uses connection and statement options such as:

- `delta.table_uri`
- `delta.version`
- `delta.max_rows`
- `delta.storage.option.<key>`
- Azure aliases for storage account and SAS token

Use ADBC options for read-only connection setup. Use `DeltaTableServiceClient` storage options for mutations and SDK workflows.
