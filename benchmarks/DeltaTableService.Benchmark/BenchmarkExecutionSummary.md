# Benchmark Execution Summary

This document summarizes the benchmark runs executed so far in this workspace session, with a focus on the benchmark results that were actually measured.

## Executed Benchmark Runs

Production-grade non-Hyper-V benchmark run (`10M` IDataReader vs baseline, non-decimal + decimal):

```bash
benchmarks/DeltaTableService.Benchmark/bin/Release/net472/DeltaTableService.Benchmark.exe --filter "*DeltaReadIDataReaderVsBaselineBenchmark*"
```

Configuration notes:

- Runtime: `.NET Framework 4.7.2` benchmark target
- BenchmarkDotNet job: production/default config (not `ShortRun`)
- Machine: Windows 11 non-Hyper-V host (`12th Gen Intel Core i9-12900`)
- Scope: `DeltaReadIDataReaderVsBaselineBenchmark`
- Scenarios: `10M:` non-decimal and `10M Decimal`
- Native memory profiler: enabled
- Coverage: Microsoft baseline and repo `IDataReader` path for full-table and full-CDF reads

Production-grade non-Hyper-V benchmark run (`10M` non-decimal):

```bash
benchmarks/DeltaTableService.Benchmark/bin/Release/net472/DeltaTableService.Benchmark.exe -scenario "10M:" --filter "*DeltaReadPerformanceBenchmark*"
```

Configuration notes:

- Runtime: `.NET Framework 4.7.2` benchmark target
- BenchmarkDotNet job: production/default config (not `ShortRun`)
- Machine: Windows 11 non-Hyper-V host (`12th Gen Intel Core i9-12900`)
- Scope: `DeltaReadPerformanceBenchmark`
- Scenario filter: `10M:` non-decimal
- Native memory profiler: enabled
- Coverage in the added report: Microsoft baseline and repo Arrow-batch path for full-table and full-CDF reads

Focused baseline-vs-IDataReader comparison trial:

```bash
benchmarks/DeltaTableService.Benchmark/bin/Release/net472/DeltaTableService.Benchmark.exe -trial -scenario "1M" --filter "*DeltaReadPerformanceBenchmark*"
```

Configuration notes:

- Runtime: `.NET Framework 4.7.2` benchmark target
- BenchmarkDotNet job: `ShortRun`
- Scope: `DeltaReadPerformanceBenchmark`
- Scenario filter: exact substring match on `1M`, which captured both `1M` and `1M Decimal`
- Coverage: `Microsoft.DI.Delta` baseline, repo Arrow batch reader, and repo `IDataReader` reader for snapshot + CDF
- Important caveat: `Microsoft.DI.Delta` returns column blocks while the repo `IDataReader` path is row-oriented, so this is useful directional data rather than a perfectly apples-to-apples API comparison

Focused decimal-only benchmark trial:

```bash
benchmarks/DeltaTableService.Benchmark/bin/Release/net472/DeltaTableService.Benchmark.exe -trial -decimal-only --filter "*DeltaReadPerformanceBenchmark*"
```

Configuration notes:

- Runtime: `.NET Framework 4.7.2` benchmark target
- BenchmarkDotNet job: `ShortRun`
- Scope: `DeltaReadPerformanceBenchmark`
- Scenario filter: decimal datasets only
- Native memory profiler: skipped automatically because the process was not running as Administrator

Focused non-decimal-only benchmark trial:

```bash
benchmarks/DeltaTableService.Benchmark/bin/Release/net472/DeltaTableService.Benchmark.exe -trial -non-decimal-only --filter "*DeltaReadPerformanceBenchmark*"
```

Configuration notes:

- Runtime: `.NET Framework 4.7.2` benchmark target
- BenchmarkDotNet job: `ShortRun`
- Scope: `DeltaReadPerformanceBenchmark`
- Scenario filter: non-decimal datasets only
- Native memory profiler: skipped automatically because the process was not running as Administrator

Previously captured legacy non-decimal benchmark artifact:

- Source artifacts: `BenchmarkDotNet.Artifacts/results/DeltaTableService.Benchmark.DeltaReadPerformanceBenchmark-report.csv` and `BenchmarkDotNet.Artifacts/results/DeltaTableService.Benchmark.DeltaReadPerformanceBenchmark-report-brief.json`
- Scope captured in those artifacts: non-decimal snapshot full-table read only
- Dataset path in the recorded run: `.../TestData/delta-full-read`
- Note: this appears to predate the later size-based snapshot dataset reorganization, so it should be treated as an earlier non-decimal baseline rather than a directly matched companion to the decimal matrix

## Benchmark Dataset Schemas

### Default snapshot and CDF base table schema

| Column | Type | Nullable |
| --- | --- | --- |
| `id` | `long` | No |
| `tenant_id` | `int32` | No |
| `event_ts` | `timestamp` | No |
| `region` | `string` | No |
| `category` | `string` | No |
| `amount` | `int32` | No |
| `quantity` | `int32` | No |
| `is_active` | `boolean` | No |
| `note` | `string` | Yes |

Default dataset dimensions:

| Dataset | Total Rows | Parquet Files | Average Single Parquet File Size |
| --- | ---: | ---: | ---: |
| `delta-full-read/1m` | `1,000,000` | `4` | `5.67 MB` |
| `delta-full-read/2m` | `2,000,000` | `4` | `10.78 MB` |
| `delta-full-read/5m` | `5,000,000` | `5` | `20.88 MB` |
| `delta-full-read/10m` | `10,000,000` | `5` | `41.81 MB` |
| `delta-full-cdf` | initial `1,000,000` | base `67`, change `22` | base `30.93 MB`, change `5.98 MB` |

### Decimal snapshot and CDF base table schema

| Column | Type | Nullable |
| --- | --- | --- |
| `id` | `long` | No |
| `tenant_id` | `int32` | No |
| `event_ts` | `timestamp` | No |
| `region` | `string` | No |
| `category` | `string` | No |
| `amount` | `int32` | No |
| `unit_price` | `decimal(18,2)` | No |
| `quantity` | `int32` | No |
| `is_active` | `boolean` | No |
| `note` | `string` | Yes |

Decimal dataset dimensions:

| Dataset | Total Rows | Parquet Files | Average Single Parquet File Size |
| --- | ---: | ---: | ---: |
| `delta-full-read-decimal/1m` | `1,000,000` | `4` | `6.00 MB` |
| `delta-full-read-decimal/2m` | `2,000,000` | `4` | `11.36 MB` |
| `delta-full-read-decimal/5m` | `5,000,000` | `5` | `22.24 MB` |
| `delta-full-read-decimal/10m` | `10,000,000` | `5` | `44.26 MB` |
| `delta-full-cdf-decimal` | latest visible `1,850,205` | base `64`, change `40` | base `18.95 MB`, change `1.96 MB` |

CDF reads add Delta metadata columns such as `'_change_type'`, `'_commit_version'`, and `'_commit_timestamp'` on top of the base table schema.

## Measured Non-Decimal Results

Production-grade `10M` non-decimal `IDataReader` vs baseline:

| Scenario | Microsoft.DI.Delta Full Table | Repo IDataReader Full Table | Table Speedup | Microsoft.DI.Delta Full CDF | Repo IDataReader Full CDF | CDF Speedup |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `10M` | `2.677 s` | `1.006 s` | `2.66x` | `4.054 s` | `1.487 s` | `2.73x` |

Production-grade `IDataReader` allocation comparison:

| Scenario | Microsoft.DI.Delta Full Table | Repo IDataReader Full Table | Table Managed Allocation Reduction | Repo IDataReader Full Table Native | Microsoft.DI.Delta Full CDF | Repo IDataReader Full CDF | CDF Managed Allocation Reduction | Repo IDataReader Full CDF Native |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `10M` | `2603.22 MB` | `15.5 MB` | `↓ 99.40%` | `8 MB` | `3469.28 MB` | `23.55 MB` | `↓ 99.32%` | `12 MB` |

Production-grade `10M` non-decimal addendum:

| Scenario | Microsoft.DI.Delta Full Table | Repo Arrow Full Table | Table Speedup | Microsoft.DI.Delta Full CDF | Repo Arrow Full CDF | CDF Speedup |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `10M` | `2.743 s` | `492.5 ms` | `5.57x` | `3.992 s` | `1.329 s` | `3.00x` |

Production-grade allocation comparison:

| Scenario | Microsoft.DI.Delta Full Table | Repo Arrow Full Table | Table Allocation Reduction | Repo Arrow Full Table Native | Microsoft.DI.Delta Full CDF | Repo Arrow Full CDF | CDF Allocation Reduction | Repo Arrow Full CDF Native |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `10M` | `2597.78 MB` | `4.06 MB` | `↓ 99.84%` | `8 MB` | `3469.29 MB` | `5.97 MB` | `↓ 99.83%` | `12 MB` |

| Scenario | Microsoft.DI.Delta Full Table | Repo Arrow Full Table | Table Speedup | Microsoft.DI.Delta Full CDF | Repo Arrow Full CDF | CDF Speedup |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `1M` | `471.930 ms` | `66.941 ms` | `7.05x` | `6.792 s` | `2.651 s` | `2.56x` |
| `2M` | `918.849 ms` | `101.449 ms` | `9.06x` | `6.697 s` | `2.771 s` | `2.42x` |
| `5M` | `2.321 s` | `208.782 ms` | `11.12x` | `6.704 s` | `2.618 s` | `2.56x` |
| `10M` | `4.856 s` | `698.937 ms` | `6.95x` | `6.739 s` | `2.688 s` | `2.51x` |

Managed allocation comparison:

| Scenario | Microsoft.DI.Delta Full Table | Repo Arrow Full Table | Table Allocation Reduction | Microsoft.DI.Delta Full CDF | Repo Arrow Full CDF | CDF Allocation Reduction |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `1M` | `288,778.64 KB` | `812 KB` | `↓ 99.72%` | `3,553,521.5 KB` | `5,984 KB` | `↓ 99.83%` |
| `2M` | `542,779.28 KB` | `1,737.79 KB` | `↓ 99.68%` | `3,553,511.49 KB` | `6,008 KB` | `↓ 99.83%` |
| `5M` | `1,332,423.71 KB` | `4,317.33 KB` | `↓ 99.68%` | `3,553,490.98 KB` | `5,984 KB` | `↓ 99.83%` |
| `10M` | `2,665,764.69 KB` | `4,408 KB` | `↓ 99.83%` | `3,553,503.73 KB` | `6,208 KB` | `↓ 99.83%` |

Legacy artifact snapshot-only result:

| Scenario | Microsoft.DI.Delta Full Table | Repo Arrow Full Table | Table Speedup |
| --- | ---: | ---: | ---: |
| `Legacy Non-Decimal Snapshot` | `4,778.3 ms` | `813.1 ms` | `5.88x` |

Notes:

- The latest non-decimal matrix was measured from the current size-based datasets.
- The legacy artifact is preserved only as historical context.

## Measured Decimal Results

Production-grade `10M Decimal` `IDataReader` vs baseline:

| Scenario | Microsoft.DI.Delta Full Table | Repo IDataReader Full Table | Table Speedup | Microsoft.DI.Delta Full CDF | Repo IDataReader Full CDF | CDF Speedup |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `10M Decimal` | `2.990 s` | `1.117 s` | `2.68x` | `2.104 s` | `696.5 ms` | `3.02x` |

Production-grade decimal `IDataReader` allocation comparison:

| Scenario | Microsoft.DI.Delta Full Table | Repo IDataReader Full Table | Table Managed Allocation Reduction | Repo IDataReader Full Table Native | Microsoft.DI.Delta Full CDF | Repo IDataReader Full CDF | CDF Managed Allocation Reduction | Repo IDataReader Full CDF Native |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `10M Decimal` | `3063.12 MB` | `20.89 MB` | `↓ 99.32%` | `9 MB` | `1917.97 MB` | `9.66 MB` | `↓ 99.50%` | `6 MB` |

| Scenario | Microsoft.DI.Delta Full Table | Repo Arrow Full Table | Table Speedup | Microsoft.DI.Delta Full CDF | Repo Arrow Full CDF | CDF Speedup |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `1M Decimal` | `522.837 ms` | `62.964 ms` | `8.30x` | `3.323 s` | `843.877 ms` | `3.94x` |
| `2M Decimal` | `996.597 ms` | `102.676 ms` | `9.71x` | `3.334 s` | `912.210 ms` | `3.66x` |
| `5M Decimal` | `2.563 s` | `201.898 ms` | `12.69x` | `3.349 s` | `865.366 ms` | `3.87x` |
| `10M Decimal` | `5.044 s` | `763.914 ms` | `6.60x` | `3.362 s` | `902.502 ms` | `3.73x` |

Focused `1M Decimal` baseline vs `IDataReader` addendum:

| Scenario | Microsoft.DI.Delta Full Table | Repo Arrow Full Table | Repo IDataReader Full Table | IDataReader vs Baseline | Microsoft.DI.Delta Full CDF | Repo Arrow Full CDF | Repo IDataReader Full CDF | IDataReader vs Baseline |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `1M Decimal` | `553.757 ms` | `73.495 ms` | `214.658 ms` | `2.58x faster` | `3.811 s` | `961.498 ms` | `1.153 s` | `3.31x faster` |

Managed allocation comparison:

| Scenario | Microsoft.DI.Delta Full Table | Repo Arrow Full Table | Table Allocation Reduction | Microsoft.DI.Delta Full CDF | Repo Arrow Full CDF | CDF Allocation Reduction |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `1M Decimal` | `337,235.22 KB` | `869 KB` | `↓ 99.74%` | `1,965,078.21 KB` | `3,144 KB` | `↓ 99.84%` |
| `2M Decimal` | `639,114.45 KB` | `1,993.33 KB` | `↓ 99.69%` | `1,965,220.3 KB` | `3,120 KB` | `↓ 99.84%` |
| `5M Decimal` | `1,568,603 KB` | `4,629.33 KB` | `↓ 99.70%` | `1,965,180.75 KB` | `3,048 KB` | `↓ 99.84%` |
| `10M Decimal` | `3,136,664.3 KB` | `4,558.3 KB` | `↓ 99.85%` | `1,965,177.42 KB` | `3,056 KB` | `↓ 99.84%` |

## Result Summary

- The new production-grade `IDataReader` report shows the row-oriented path also beats `Microsoft.DI.Delta` comfortably at `10M` scale for both non-decimal and decimal datasets.
- For `10M` non-decimal, `IDataReader` was `2.66x` faster for full-table reads and `2.73x` faster for full CDF reads.
- For `10M Decimal`, `IDataReader` was `2.68x` faster for full-table reads and `3.02x` faster for full CDF reads.
- Managed allocation stayed dramatically lower on the `IDataReader` path as well: `15.5 MB` to `23.55 MB` for non-decimal and `9.66 MB` to `20.89 MB` for decimal, versus baseline memory footprints measured in multi-GB.
- Native memory on the `IDataReader` path remained modest in the production-grade runs: `8 MB`/`12 MB` for non-decimal full-table/CDF and `9 MB`/`6 MB` for decimal full-table/CDF.
- The added production-grade `10M` non-decimal run on a non-Hyper-V machine confirms the same overall direction as the earlier trial runs.
- In that run, the repo client was `5.57x` faster for full-table reads and `3.00x` faster for full CDF reads.
- The same run showed extreme managed allocation reductions: `2597.78 MB` to `4.06 MB` for full-table and `3469.29 MB` to `5.97 MB` for full CDF.
- Native memory remained modest on the repo path in the production-grade run: `8 MB` for full-table and `12 MB` for full CDF, with `0 MB` reported leak.
- The current non-decimal matrix favors the repo client in every scenario.
- Non-decimal full table reads showed `6.95x` to `11.12x` speedups.
- Non-decimal full CDF reads showed `2.42x` to `2.56x` speedups.
- The repo client outperformed `Microsoft.DI.Delta` in every measured decimal scenario.
- Full table reads showed the largest gains, ranging from `6.60x` to `12.69x` faster.
- Full CDF reads were also consistently faster, ranging from `3.66x` to `3.94x`.
- The repo client remained under `1s` for all measured decimal CDF runs.
- The repo client stayed notably faster for decimal full-table reads even at `10M` rows.
- In the focused `1M` comparison run, the repo `IDataReader` path was slower than the repo Arrow-batch path, but still materially faster than the `Microsoft.DI.Delta` baseline for both snapshot and CDF reads.

## Notable Observations

- The new non-Hyper-V `IDataReader` report closes the earlier evidence gap: the publication-quality results now cover both the repo Arrow-batch path and the repo `IDataReader` path against `Microsoft.DI.Delta`.
- As expected, the repo `IDataReader` path is slower than the repo Arrow-batch path, but it still preserves a substantial advantage over the Microsoft baseline on both latency and memory.
- On the non-Hyper-V machine, the `IDataReader` path kept total memory use very low relative to baseline even with its row-oriented overhead: tens of MB instead of multiple GB.
- The non-Hyper-V production run reduced the environment noise from the earlier Hyper-V `ShortRun` measurements while preserving the same winner and the same broad performance shape.
- In the production-grade `10M` run, `Microsoft.DI.Delta` reported `0 MB` native allocation while the repo path reported small native allocations (`8 MB` full-table, `12 MB` full CDF); despite that, the repo path still had dramatically lower total memory pressure because Microsoft managed allocation stayed in the multi-GB range.
- The Arrow-batch report and the newer `IDataReader` report together now provide publication-quality evidence for both repo read shapes versus `Microsoft.DI.Delta`.
- `Microsoft.DI.Delta` non-decimal CDF timings were almost flat across dataset sizes, staying around `6.70s` to `6.79s`.
- Each measured `Microsoft.DI.Delta` non-decimal CDF run reported `Exceptions: 11` in the BenchmarkDotNet output.
- Repo-client non-decimal CDF runs did not show the same repeated exception pattern.
- Managed allocation in non-decimal runs heavily favored the repo client: Microsoft CDF stayed around `3.55 GB` per operation while repo CDF stayed around `6 MB`; Microsoft full-table ranged from about `289 MB` to `2.67 GB` while repo full-table stayed in the `KB` to low-`MB` range.
- `Microsoft.DI.Delta` CDF timings were almost flat across dataset sizes, staying around `3.32s` to `3.36s`.
- Each measured `Microsoft.DI.Delta` CDF run reported `Exceptions: 20` in the BenchmarkDotNet output.
- Repo-client CDF runs did not show the same repeated exception pattern.
- Managed allocation in decimal runs also heavily favored the repo client: Microsoft CDF stayed around `1.97 GB` per operation while repo CDF stayed around `3 MB`; Microsoft full-table ranged from about `337 MB` to `3.14 GB` while repo full-table stayed under `5 MB`.
- In the focused `1M Decimal` comparison, the repo `IDataReader` path allocated about `2.18 MB` for full-table reads and `9.9 MB` for full CDF reads, which is higher than the repo Arrow-batch path but still dramatically below the `Microsoft.DI.Delta` baseline.
- Allocation reduction is dramatic in both modes: roughly `↓ 99.68%` to `↓ 99.83%` for non-decimal full-table, `↓ 99.83%` for non-decimal CDF, `↓ 99.69%` to `↓ 99.85%` for decimal full-table, and `↓ 99.84%` for decimal CDF.
- GC pressure was substantially higher on the `Microsoft.DI.Delta` path than on the repo-client path, with Microsoft showing consistent Gen2 activity while repo runs were mostly Gen0-driven.
- Because this was a `ShortRun` trial with `N=3`, the numbers are best treated as directional rather than final publication-grade measurements.

## Practical Takeaway

- The new `DeltaReadIDataReaderVsBaselineBenchmark` report shows that the repo's row-oriented `IDataReader` path is also production-grade competitive, not just the Arrow-batch path.
- For both `10M` non-decimal and `10M Decimal`, the `IDataReader` API is materially faster than `Microsoft.DI.Delta` while using far less managed memory.
- The new non-Hyper-V production run strengthens confidence that the repo client's performance advantage is real and not just a Hyper-V or short-run artifact.
- For the `10M` non-decimal scenario, the repo Arrow-batch path is already comfortably ahead of `Microsoft.DI.Delta` on both time and memory.
- Both the current non-decimal matrix and the decimal matrix point in the same direction: the repo client is materially faster than `Microsoft.DI.Delta`.
- Decimal support is functioning end-to-end in the benchmark flow.
- For both default and decimal datasets measured so far, the repo client is clearly ahead for both snapshot reads and CDF reads.
- The strongest current follow-up area is understanding the repeated exceptions on the `Microsoft.DI.Delta` CDF path: `11` per run for non-decimal and `20` per run for decimal.

## Related Files

- Full BenchmarkDotNet markdown report: `BenchmarkDotNet.Artifacts/results/DeltaTableService.Benchmark.DeltaReadPerformanceBenchmark-report-github.md`
- Production-grade non-Hyper-V markdown report: `benchmarks/DeltaTableService.Benchmark/results/DeltaTableService.Benchmark.DeltaReadPerformanceBenchmark-report-github.md`
- Production-grade non-Hyper-V `IDataReader` vs baseline markdown report: `benchmarks/DeltaTableService.Benchmark/results/DeltaTableService.Benchmark.DeltaReadIDataReaderVsBaselineBenchmark-report-github.md`
- Benchmark CSV export: `BenchmarkDotNet.Artifacts/results/DeltaTableService.Benchmark.DeltaReadPerformanceBenchmark-report.csv`
- Benchmark JSON export: `BenchmarkDotNet.Artifacts/results/DeltaTableService.Benchmark.DeltaReadPerformanceBenchmark-report-brief.json`
- Schema and dataset dimensions: `BenchmarkDotNet.Artifacts/results/DecimalTableSchemas.md`
