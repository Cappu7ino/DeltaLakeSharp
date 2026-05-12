# AI Integration Docs

These files help humans and coding agents integrate the DeltaLakeSharp SDK correctly. They are intentionally concise, retrieval-friendly, and limitation-forward.

## Read Order

| File | Purpose |
| --- | --- |
| [overview.md](overview.md) | Bootstrap the repository purpose, architecture, execution model, lifecycle, and terminology. |
| [capabilities.md](capabilities.md) | Map public capabilities to recommended usage, constraints, and backend availability. |
| [limitations.md](limitations.md) | Prevent misuse by documenting unsupported scenarios, operational assumptions, and risky patterns. |
| [common_patterns.md](common_patterns.md) | Canonical integration patterns for SDK, V3, storage, CDF, partitions, and ADBC. |
| [anti_patterns.md](anti_patterns.md) | Incorrect usage patterns, consequences, and safer alternatives. |
| [bootstrap.md](bootstrap.md) | Compact agent instructions for downstream SDK integration and code generation. |
| [api_ergonomics.md](api_ergonomics.md) | API surfaces that are easy for agents to misuse and future redesign recommendations. |
| [../../api/public-api.md](../../api/public-api.md) | Semantic public API inventory for the NuGet-facing surface. |
| [../../api/semantic-index.json](../../api/semantic-index.json) | Machine-readable API and capability metadata for retrieval systems. |
| [../../examples/DeltaLakeSharp.Client.Examples/README.md](../../examples/DeltaLakeSharp.Client.Examples/README.md) | Compileable SDK consumer example that references the client `netstandard2.0` asset. |
| [../../tests/DeltaLakeSharp.Tests/IntegrationScenarios/V3ClientSdkScenarioTests.cs](../../tests/DeltaLakeSharp.Tests/IntegrationScenarios/V3ClientSdkScenarioTests.cs) | Executable V3 integration scenarios for streaming reads, SQL, partitions, and CDF. |

## Focused Architecture Docs

| File | Purpose |
| --- | --- |
| [../architecture/execution-model.md](../architecture/execution-model.md) | Backend selection, lifecycle, streaming model, and capability failures. |
| [../architecture/native-interop.md](../architecture/native-interop.md) | V3 C ABI, Arrow C Data/C Stream ownership, and native runtime loading. |
| [../architecture/adbc.md](../architecture/adbc.md) | ADBC read-only, path-scoped, V3-backed architecture. |
| [../architecture/storage-and-auth.md](../architecture/storage-and-auth.md) | Per-request storage credentials, `StorageConfig`, `GenericStorageOptions`, and ADBC options. |
| [../architecture/cdf-and-partitioned-reads.md](../architecture/cdf-and-partitioned-reads.md) | CDF and partitioned read semantics, constraints, and ADBC behavior. |

## How-To Guides

| File | Purpose |
| --- | --- |
| [../how-to/consume-client-sdk.md](../how-to/consume-client-sdk.md) | Choose runtime, create a client, dispose it, and pass storage options. |
| [../how-to/read-data.md](../how-to/read-data.md) | Read tables as Arrow batches, `DbDataReader`, Arrow streams, materialized results, or SQL queries. |
| [../how-to/use-v3-features.md](../how-to/use-v3-features.md) | Use V3 CDF, partitioned reads, schema-mode writes, and streaming merge. |
| [../how-to/troubleshoot.md](../how-to/troubleshoot.md) | Diagnose native library loading, backend capability, memory, decimal, SQL, ADBC, and storage issues. |

## Architecture Decision Records

| File | Decision |
| --- | --- |
| [../adr/0001-v3-preferred-sdk-runtime.md](../adr/0001-v3-preferred-sdk-runtime.md) | V3 native Rust is the preferred SDK runtime. |
| [../adr/0002-arrow-first-data-model.md](../adr/0002-arrow-first-data-model.md) | Arrow batches and streams are the canonical data model. |
| [../adr/0003-adbc-read-only-single-table-scope.md](../adr/0003-adbc-read-only-single-table-scope.md) | ADBC is read-only and path-scoped. |
| [../adr/0004-explicit-backend-limitations.md](../adr/0004-explicit-backend-limitations.md) | Unsupported backend capabilities fail explicitly. |
| [../adr/0005-per-request-storage-options.md](../adr/0005-per-request-storage-options.md) | Storage credentials and options are passed per request. |

## Source Of Truth

Prefer source and tests over prose when behavior is ambiguous:

- Client package surface: [../../src/DeltaLakeSharp.Client/DeltaTableServiceClient.cs](../../src/DeltaLakeSharp.Client/DeltaTableServiceClient.cs)
- Backend contract: [../../src/DeltaLakeSharp.Client/Internal/IDeltaLakeBackend.cs](../../src/DeltaLakeSharp.Client/Internal/IDeltaLakeBackend.cs)
- Flight backend wrapper: [../../src/DeltaLakeSharp.Client/Internal/FlightClientWrapper.cs](../../src/DeltaLakeSharp.Client/Internal/FlightClientWrapper.cs)
- Native Rust backend wrapper: [../../src/DeltaLakeSharp.Client/Internal/NativeRustBackend.cs](../../src/DeltaLakeSharp.Client/Internal/NativeRustBackend.cs)
- ADBC package surface: [../../src/DeltaLakeSharp.Adbc/README.md](../../src/DeltaLakeSharp.Adbc/README.md)
- Integration behavior: [../../tests/DeltaLakeSharp.Tests](../../tests/DeltaLakeSharp.Tests)
- ADBC behavior: [../../tests/DeltaLakeSharp.Adbc.Tests](../../tests/DeltaLakeSharp.Adbc.Tests)

## Current Artifact Scope

This artifact set covers SDK-facing client and ADBC semantics, architecture, how-to guidance, common patterns, anti-patterns, ADRs, a compileable SDK example, and V3 integration scenario tests. Add future examples only when they can compile or reuse existing test infrastructure.
