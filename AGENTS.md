# AGENTS

This repository is optimized for agent-assisted work on DeltaLakeSharp, an experimental Delta Lake SDK for .NET built from managed C# code and a native Rust V3 runtime.

## Current Product Direction

- V3 native Rust is the recommended SDK runtime for external consumers.
- V1 Spark and V2 DataFusion remain service-backed compatibility modes.
- Public read APIs are Arrow-first and stream `RecordBatch` values by default.
- `DbDataReader` and materialization helpers are adapters, not the preferred default for large tables.
- ADBC is read-only, path-scoped, and backed by the V3 runtime.

## Repository Map

- `src/DeltaLakeSharp.Client/` - main managed SDK surface and backend abstraction
- `src/DeltaLakeSharp.Adbc/` - ADBC driver over the V3 client path
- `src/DeltaLakeSharp.Testing/` - test and container harness support
- `src/DeltaLakeSharp.Server/v3/` - Rust native runtime
- `src/DeltaLakeSharp.Server/v1/` and `src/DeltaLakeSharp.Server/v2/` - service-backed compatibility servers
- `tests/` - MSTest unit, compatibility, and integration coverage
- `tests/DeltaLakeSharp.Tests/IntegrationScenarios/` - executable public SDK workflow scenarios
- `examples/` - compile-checked consumer examples
- `api/` - semantic public API inventory and machine-readable index
- `docs/ai/` - AI-optimized integration guidance
- `docs/architecture/`, `docs/how-to/`, and `docs/adr/` - focused architecture, workflow, and decision docs

## Working Rules

- Keep public APIs explicit and capability-aware.
- Prefer streaming Arrow APIs for unknown-size data.
- Do not hide backend capability failures behind empty results or silent fallbacks.
- Treat partition tokens as opaque backend-generated descriptors.
- Treat protocol upgrades as explicit, irreversible table operations.
- Do not log storage credentials, SAS tokens, object-store secrets, or real private paths.
- Keep V1/V2 Docker-backed tests separate from normal non-container validation.

## Documentation Hygiene

- After any code change, assess whether tracked markdown files need updates.
- Update docs in the same change when behavior, setup, validation commands, architecture, public API usage, limitations, or troubleshooting guidance changes.
- Prefer focused updates to existing docs over creating new docs.
- If no markdown update is needed, say so in the final summary.
- Do not commit code changes until documentation impact has been considered.

Documentation targets:

- Architecture or runtime behavior: `docs/architecture/`
- Consumer workflows: `docs/how-to/`
- AI and agent guidance: `docs/ai/`
- Validation and contributor commands: `CONTRIBUTING.md` and `AGENTS.md`
- Public API changes: `README.md`, `api/public-api.md`, and `api/semantic-index.json`

## Validation Commands

```powershell
dotnet build DeltaLakeSharp.sln /p:SkipRustBuild=true -m:1
```

```powershell
dotnet build examples\DeltaLakeSharp.Client.Examples\DeltaLakeSharp.Client.Examples.csproj /p:SkipRustBuild=true
```

```powershell
dotnet test tests\DeltaLakeSharp.Client.Compatibility.Tests\DeltaLakeSharp.Client.Compatibility.Tests.csproj
```

Run Rust validation from `src/DeltaLakeSharp.Server/v3`:

```powershell
cargo test
```

For V3 runtime changes, build the Rust fixture and run focused V3 tests:

```powershell
dotnet test tests\DeltaLakeSharp.Tests\DeltaLakeSharp.Tests.csproj --filter "TestCategory=V3"
```

On macOS ARM64, prefer the explicit ARM64 `net8.0` invocation when validating V3 tests locally:

```powershell
dotnet test tests\DeltaLakeSharp.Tests\DeltaLakeSharp.Tests.csproj --framework net8.0 --arch arm64 --filter "TestCategory=V3" /p:PlatformTarget=arm64 /p:SkipRustBuild=true
```

## Files Worth Reading Before Major Changes

- `README.md`
- `api/public-api.md`
- `api/semantic-index.json`
- `docs/ai/bootstrap.md`
- `docs/ai/capabilities.md`
- `docs/architecture/execution-model.md`
- `docs/architecture/native-interop.md`
- `src/DeltaLakeSharp.Client/DeltaTableServiceClient.cs`
- `src/DeltaLakeSharp.Client/Internal/NativeRustBackend.cs`
- `src/DeltaLakeSharp.Server/v3/src/interop/native.rs`
