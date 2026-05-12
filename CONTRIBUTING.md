# Contributing to DeltaLakeSharp

DeltaLakeSharp is an experimental, incubating Delta Lake SDK for .NET. Contributions should preserve the Arrow-first data model, keep V3 native Rust as the recommended SDK runtime, and make backend limitations explicit.

## Development Setup

Install:

- .NET SDK 8.0 or later
- Rust stable toolchain
- Docker Desktop if you plan to run V1/V2 service-backed integration tests

Restore and build:

```powershell
dotnet restore
dotnet build DeltaLakeSharp.sln /p:SkipRustBuild=true -m:1
```

Build the native V3 runtime when running native integration tests from source:

```powershell
Push-Location src\DeltaLakeSharp.Server\v3
cargo build
cargo test
Pop-Location
```

## Validation

Use these commands for typical changes:

```powershell
dotnet build DeltaLakeSharp.sln /p:SkipRustBuild=true -m:1
dotnet build examples\DeltaLakeSharp.Client.Examples\DeltaLakeSharp.Client.Examples.csproj /p:SkipRustBuild=true
dotnet test tests\DeltaLakeSharp.Client.Compatibility.Tests\DeltaLakeSharp.Client.Compatibility.Tests.csproj
```

For V3 runtime or public SDK workflow changes, also run the focused V3 tests after building the Rust fixture:

```powershell
dotnet test tests\DeltaLakeSharp.Tests\DeltaLakeSharp.Tests.csproj --filter "TestCategory=V3"
```

V1/V2 compatibility tests require Docker/Testcontainers and may be run separately.

## Pull Requests

- Keep changes focused and source-backed.
- Update README, docs, examples, and API metadata when public behavior changes.
- Add or update tests for new public SDK behavior.
- Do not commit generated `bin/`, `obj/`, `target/`, test result, or benchmark artifact folders.

## License

By contributing, you agree that your contribution is licensed under the MIT license.
