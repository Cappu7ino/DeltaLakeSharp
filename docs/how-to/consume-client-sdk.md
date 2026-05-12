# How To Consume The Client SDK

## Goal

Reference `DeltaLakeSharp.Client` from a .NET application and choose the correct runtime path.

## Choose Runtime

For new external NuGet integrations, prefer V3:

```csharp
using DeltaLakeSharp.Client;

using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
```

Use V1/V2 only when the caller already operates a Flight endpoint:

```csharp
using var client = new DeltaTableServiceClient(
    new Uri("http://localhost:8815"),
    ServiceMode.V2_DataFusion);
```

Avoid using `new DeltaTableServiceClient(uri)` unless V1 Spark is intended, because that overload defaults to `ServiceMode.V1_Spark`.

## Target Framework Notes

The client package is intended for multiple .NET consumers, including modern .NET and compatibility targets. V3 runtime execution still requires the native Rust library to be present for the active runtime.

## Basic Health Check

```csharp
bool healthy = await client.HealthCheckAsync(cancellationToken);
```

For V3, this validates native backend availability. For V1/V2, this validates the remote Flight service.

## Disposal

Always dispose the client:

```csharp
await using var cancellationRegistration = cancellationToken.Register(() => { });
using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
```

`DeltaTableServiceClient` implements `IDisposable`, not `IAsyncDisposable`. A plain `using` is sufficient.

## Add Storage Options When Needed

For ABFSS/OneLake/SAS scenarios, pass storage options on each operation rather than storing global credentials.

```csharp
var storage = new StorageConfig("onelake", sasToken, evictFileSystemCache: false);

await foreach (RecordBatch batch in client.ReadTableAsync(
    tablePath,
    storageConfig: storage,
    cancellationToken: cancellationToken))
{
    // Process batch.
}
```

## Verify Capability Before Generating Code

Use V3 when code needs:

- CDF APIs
- partitioned read APIs
- schema-mode writes
- ADBC-backed behavior

Use V1/V2 only for service-backed compatibility workflows.
