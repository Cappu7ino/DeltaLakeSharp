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

The local native library name is platform-specific: `delta_table_service_native.dll` on Windows, `libdelta_table_service_native.dylib` on macOS, and `libdelta_table_service_native.so` on Linux. The V3 fixture binary is `delta-table-service-v3-fixture.exe` on Windows and `delta-table-service-v3-fixture` on macOS/Linux.

## Validation

Pull request CI runs required Windows and Linux jobs in parallel. Windows is the full compatibility gate, including `net472` coverage. Linux validates `net8.0` portability, the V3 native Rust runtime, and Linux native-library loading behavior. macOS is not currently a required PR runner.

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

On macOS ARM64, run the `net8.0` V3 tests with an ARM64 host when the project `PlatformTarget` would otherwise request x64:

```powershell
dotnet test tests\DeltaLakeSharp.Tests\DeltaLakeSharp.Tests.csproj --framework net8.0 --arch arm64 --filter "TestCategory=V3" /p:PlatformTarget=arm64 /p:SkipRustBuild=true
```

V3 performance smoke tests use structural assertions rather than tight timing thresholds. Run them when changing native read planning, Arrow stream setup, prefetching, or partition reads:

```powershell
dotnet test tests\DeltaLakeSharp.Tests\DeltaLakeSharp.Tests.csproj --framework net8.0 --arch arm64 --filter "FullyQualifiedName~NativeRustPerformanceSmokeTests" /p:PlatformTarget=arm64 /p:SkipRustBuild=true
```

For local V3 benchmark trend checks, generate deterministic local datasets and run the V3 benchmark subset:

```powershell
dotnet run --project benchmarks\DeltaLakeSharp.Benchmark\DeltaLakeSharp.Benchmark.csproj -c Release -- --filter V3 --generate-datasets --dataset small,partitioned --prefetch both -trial
```

Use `--output <path>` to choose the generated dataset root and `--overwrite-datasets` to regenerate existing benchmark datasets.

The manual package dry-run workflow runs on Windows and Linux. Windows packs the full current package shape. Linux packs the `net8.0` SDK/ADBC slices to catch cross-platform pack and path issues without making Linux responsible for `net472` package validation.

V1/V2 compatibility tests require Docker/Testcontainers and may be run separately.

## Pull Requests

- Keep changes focused and source-backed.
- Update README, docs, examples, and API metadata when public behavior changes.
- Add or update tests for new public SDK behavior.
- Do not commit generated `bin/`, `obj/`, `target/`, test result, or benchmark artifact folders.

## License

By contributing, you agree that your contribution is licensed under the MIT license.
