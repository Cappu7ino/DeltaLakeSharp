using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using DeltaLakeSharp.Client;

namespace DeltaLakeSharp.Benchmark
{
    [MemoryDiagnoser]
    public class DeltaReadPerformanceBenchmark
    {
        private DeltaTableServiceClient _repoClient = null!;

        [ParamsSource(nameof(ScenarioSources))]
        public DeltaReadBenchmarkScenario Scenario { get; set; } = null!;

        public IEnumerable<DeltaReadBenchmarkScenario> ScenarioSources =>
            FilterScenarios(new[]
            {
                new DeltaReadBenchmarkScenario(
                    label: "1M",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read", "1m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf"),
                    startingVersion: 0,
                    endingVersion: null),
                new DeltaReadBenchmarkScenario(
                    label: "2M",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read", "2m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf"),
                    startingVersion: 0,
                    endingVersion: null),
                new DeltaReadBenchmarkScenario(
                    label: "5M",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read", "5m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf"),
                    startingVersion: 0,
                    endingVersion: null),
                new DeltaReadBenchmarkScenario(
                    label: "10M",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read", "10m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf"),
                    startingVersion: 0,
                    endingVersion: null),
                new DeltaReadBenchmarkScenario(
                    label: "1M Decimal",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read-decimal", "1m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf-decimal"),
                    startingVersion: 0,
                    endingVersion: null),
                new DeltaReadBenchmarkScenario(
                    label: "2M Decimal",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read-decimal", "2m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf-decimal"),
                    startingVersion: 0,
                    endingVersion: null),
                new DeltaReadBenchmarkScenario(
                    label: "5M Decimal",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read-decimal", "5m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf-decimal"),
                    startingVersion: 0,
                    endingVersion: null),
                new DeltaReadBenchmarkScenario(
                    label: "10M Decimal",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read-decimal", "10m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf-decimal"),
                    startingVersion: 0,
                    endingVersion: null),
            });

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            ValidateScenarioPaths(Scenario);

            _repoClient = new DeltaTableServiceClient(ServiceMode.V3_Rust);
            if (!await _repoClient.HealthCheckAsync())
            {
                throw new InvalidOperationException("The DeltaLakeSharp V3 native backend is not healthy.");
            }

            await RepoClient_FullTableRead();
            await RepoClient_DataReaderFullTableRead();
            await RepoClient_FullChangeDataRead();
            await RepoClient_DataReaderFullChangeDataRead();
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _repoClient?.Dispose();
        }

        [Benchmark(Baseline = true, Description = "DeltaLakeSharp Arrow full table read")]
        public async Task<ReadIterationResult> RepoClient_FullTableRead()
        {
            return await ConsumeArrowBatchesAsync(_repoClient.ReadTableAsync(Scenario.SnapshotTablePath));
        }

        [Benchmark(Description = "DeltaLakeSharp IDataReader full table read")]
        public async Task<ReadIterationResult> RepoClient_DataReaderFullTableRead()
        {
            using DbDataReader reader = await _repoClient.ReadTableAsDataReaderAsync(Scenario.SnapshotTablePath);
            return ConsumeDataReader(reader);
        }

        [Benchmark(Description = "DeltaLakeSharp Arrow full CDF read")]
        public async Task<ReadIterationResult> RepoClient_FullChangeDataRead()
        {
            return await ConsumeArrowBatchesAsync(
                _repoClient.ReadChangeDataAsync(
                    Scenario.CdfTablePath,
                    Scenario.StartingVersion,
                    Scenario.EndingVersion));
        }

        [Benchmark(Description = "DeltaLakeSharp IDataReader full CDF read")]
        public async Task<ReadIterationResult> RepoClient_DataReaderFullChangeDataRead()
        {
            using DbDataReader reader = await _repoClient.ReadChangeDataAsDataReaderAsync(
                Scenario.CdfTablePath,
                Scenario.StartingVersion,
                Scenario.EndingVersion);
            return ConsumeDataReader(reader);
        }

        private static async Task<ReadIterationResult> ConsumeArrowBatchesAsync(IAsyncEnumerable<RecordBatch> batches)
        {
            long rowCount = 0;
            long blockCount = 0;

            await foreach (RecordBatch batch in batches)
            {
                blockCount++;
                rowCount += batch.Length;
            }

            return new ReadIterationResult(rowCount, blockCount);
        }

        private static ReadIterationResult ConsumeDataReader(DbDataReader reader)
        {
            long rowCount = 0;
            int fieldCount = reader.FieldCount;

            while (reader.Read())
            {
                rowCount++;
            }

            return new ReadIterationResult(rowCount, fieldCount);
        }

        private static void ValidateScenarioPaths(DeltaReadBenchmarkScenario scenario)
        {
            if (!Directory.Exists(scenario.SnapshotTablePath))
            {
                throw new DirectoryNotFoundException(
                    $"Snapshot dataset path '{scenario.SnapshotTablePath}' does not exist. Generate it first with the benchmark dataset generator.");
            }

            if (!Directory.Exists(scenario.CdfTablePath))
            {
                throw new DirectoryNotFoundException(
                    $"CDF dataset path '{scenario.CdfTablePath}' does not exist. Generate it first with the benchmark dataset generator.");
            }
        }

        private static IEnumerable<DeltaReadBenchmarkScenario> FilterScenarios(IEnumerable<DeltaReadBenchmarkScenario> scenarios)
        {
            string? filter = Environment.GetEnvironmentVariable("DTS_BENCHMARK_SCENARIO_FILTER");
            if (string.IsNullOrWhiteSpace(filter))
            {
                return scenarios;
            }

            DeltaReadBenchmarkScenario[] filtered = scenarios
                .Where(s => MatchesScenarioFilter(s.Label, filter))
                .ToArray();

            if (filtered.Length == 0)
            {
                throw new InvalidOperationException($"No benchmark scenarios matched filter '{filter}'.");
            }

            return filtered;
        }

        private static bool MatchesScenarioFilter(string label, string filter)
        {
            if (string.Equals(filter, "non-decimal", StringComparison.OrdinalIgnoreCase))
            {
                return label.IndexOf("decimal", StringComparison.OrdinalIgnoreCase) < 0;
            }

            return label.IndexOf(filter, StringComparison.OrdinalIgnoreCase) >= 0;
        }

        public sealed class DeltaReadBenchmarkScenario
        {
            public DeltaReadBenchmarkScenario(string label, string snapshotTablePath, string cdfTablePath, long startingVersion, long? endingVersion)
            {
                Label = label;
                SnapshotTablePath = snapshotTablePath;
                CdfTablePath = cdfTablePath;
                StartingVersion = startingVersion;
                EndingVersion = endingVersion;
            }

            public string Label { get; }

            public string SnapshotTablePath { get; }

            public string CdfTablePath { get; }

            public long StartingVersion { get; }

            public long? EndingVersion { get; }

            public override string ToString()
            {
                string cdfEnd = EndingVersion.HasValue ? EndingVersion.Value.ToString() : "latest";
                return $"{Label}: Snapshot={SnapshotTablePath}, Cdf={CdfTablePath}, CdfStart={StartingVersion}, CdfEnd={cdfEnd}";
            }
        }

        public struct ReadIterationResult
        {
            public ReadIterationResult(long rowCount, long blockCount)
            {
                RowCount = rowCount;
                BlockCount = blockCount;
            }

            public long RowCount { get; }

            public long BlockCount { get; }
        }
    }
}
