# Run V3 Performance Checks

V3 performance coverage has two layers:

- Smoke tests catch obvious regressions in partition planning payload size, first-batch behavior, prefetch reads, client creation, and concurrent partition reads.
- BenchmarkDotNet scenarios track local trends for schema reads, partition planning, first-batch latency, full scans, concurrent partition reads, and CDF reads.

The smoke tests intentionally avoid tight timing thresholds. They use structural assertions so normal validation is less sensitive to machine load.

## Run Smoke Tests

Build the Rust V3 runtime first, or pass `/p:SkipRustBuild=true` when the native library is already built and copied for the current platform.

```powershell
dotnet test tests\DeltaLakeSharp.Tests\DeltaLakeSharp.Tests.csproj --framework net8.0 --arch arm64 --filter "FullyQualifiedName~NativeRustPerformanceSmokeTests" /p:PlatformTarget=arm64 /p:SkipRustBuild=true
```

On non-macOS ARM64 hosts, drop the `--arch arm64` and `/p:PlatformTarget=arm64` arguments unless your local build requires them.

## Generate Local Benchmark Datasets

The benchmark project can generate deterministic local V3 datasets with no storage credentials or network access.

```powershell
dotnet run --project benchmarks\DeltaLakeSharp.Benchmark\DeltaLakeSharp.Benchmark.csproj -c Release -- --generate-datasets --dataset all
```

Supported dataset profiles:

- `small`: narrow append-only table for setup and first-batch overhead.
- `many-files`: many small Add actions for planning payload checks.
- `wide`: 100 metric columns for schema conversion and metadata handling.
- `partitioned`: partitioned local table for partition planning and concurrent reads.
- `cdf`: change data feed table with updates, deletes, and appends.

Use `--dataset small,partitioned` to generate a subset, `--output <path>` to choose the dataset root, and `--overwrite-datasets` to regenerate existing datasets.

## Run V3 Benchmarks

Use BenchmarkDotNet filtering to keep runs focused:

```powershell
dotnet run --project benchmarks\DeltaLakeSharp.Benchmark\DeltaLakeSharp.Benchmark.csproj -c Release -- --filter V3 --dataset small,wide,partitioned --prefetch both -trial
```

Useful options:

- `--filter V3`: run only benchmark classes whose names match V3.
- `--dataset small|many-files|wide|partitioned|cdf|all`: select local dataset profiles.
- `--prefetch true|false|both`: select native read prefetch mode.
- `--concurrency 1,2,4,8`: select concurrent partition read fan-out values.
- `--generate-datasets`: generate or reuse local datasets before the benchmark run.
- `--output <path>`: set the local generated dataset root.
- `-trial`: use the shorter benchmark job for fast validation.

Benchmark results should be interpreted as local trend data. Prefer comparing runs on the same machine, runtime, build configuration, and dataset root. Smoke tests are the normal pass/fail gates; benchmarks are for investigation and performance review.