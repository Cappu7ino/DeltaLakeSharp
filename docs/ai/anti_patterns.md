# Anti-Patterns

These patterns lead to incorrect integrations, hidden performance costs, or misleading generated code.

## Using The URI Constructor When V3 Was Intended

Incorrect:

```csharp
using var client = new DeltaTableServiceClient(new Uri("http://localhost:8815"));
```

Why it is wrong:

- The URI constructor defaults to `ServiceMode.V1_Spark`.
- New external SDK consumers should usually use V3.

Consequence:

- Generated code may require a service that the consumer does not run.
- V3-only APIs such as CDF and partitioned reads will not be available.

Correct alternative:

```csharp
using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
```

## Buffering Unknown-Size Tables

Incorrect:

```csharp
DataTable table = await client.ReadTableAsync(path).ToDataTableAsync();
```

Why it is wrong:

- It materializes the entire result into memory.
- It discards the Arrow streaming model.

Consequence:

- High memory pressure, slow processing, and possible out-of-memory failures.

Correct alternative:

```csharp
await foreach (RecordBatch batch in client.ReadTableAsync(path, cancellationToken: cancellationToken))
{
    // Process bounded chunks.
}
```

Use materialization helpers only for small, bounded results.

## Choosing V1/V2 For CDF Or Partition APIs

Incorrect:

- Generate CDF code with `ServiceMode.V1_Spark` or `ServiceMode.V2_DataFusion`.
- Generate partitioned read code for Flight backends.

Why it is wrong:

- The Flight wrapper throws `NotSupportedException` for those public APIs.

Consequence:

- Runtime failure even though the API exists on the shared client type.

Correct alternative:

- Use `ServiceMode.V3_Rust` for CDF and partitioned read APIs.
- Use ADBC CDF/partition options only for supported read-only scenarios.

## Logging Storage Credentials

Incorrect:

- Log `StorageConfig.SasToken`.
- Log full `GenericStorageOptions.Options` dictionaries.

Why it is wrong:

- These values can contain SAS tokens or object-store credentials.

Consequence:

- Credential leakage into logs, traces, or prompt transcripts.

Correct alternative:

- Log credential presence and storage account names only when safe.
- Redact secret values before diagnostics.

## Constructing Partition Tokens Manually

Incorrect:

```csharp
var partition = new DeltaReadPartition("guessed-token", version: 10, ordinal: 0, totalPartitions: 1, fileCount: 1);
```

Why it is wrong:

- Tokens are backend-generated opaque descriptors.
- They are tied to planning and snapshot semantics.

Consequence:

- Invalid reads, stale snapshot behavior, or backend errors.

Correct alternative:

- Call `GetReadPartitionsAsync` and pass returned descriptors to read APIs.

## Treating ADBC As A Write Driver

Incorrect:

- Generate `ExecuteUpdate` write workflows for `DeltaLakeSharp.Adbc`.
- Expect prepared statements, transactions, or parameter binding.

Why it is wrong:

- The ADBC package is a read-only MVP backed by V3.

Consequence:

- `AdbcException.NotImplemented` or invalid operation errors.

Correct alternative:

- Use `DeltaTableServiceClient` for writes and DML.
- Use ADBC for read-only Arrow-native query flows.

## Hiding Backend Capability Failures

Incorrect:

```csharp
try
{
    await client.ReadChangeDataAsync(path, 1).ToListAsync();
}
catch
{
    // Ignore and return empty data.
}
```

Why it is wrong:

- `NotSupportedException` is a capability signal, not an empty result.

Consequence:

- Silent data loss and misleading downstream behavior.

Correct alternative:

- Fail clearly or choose a backend that supports the requested capability.

## Casual Protocol Upgrades

Incorrect:

- Call `UpgradeTableProtocolAsync` during normal client initialization.

Why it is wrong:

- Delta protocol upgrades are irreversible and can affect other readers/writers.

Consequence:

- Shared table compatibility can be broken unexpectedly.

Correct alternative:

- Require explicit user intent, document feature flags, and validate downstream support.
