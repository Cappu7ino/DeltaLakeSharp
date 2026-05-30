using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using BenchmarkDotNet.Attributes;
using DeltaLakeSharp.Client;
using DeltaLakeSharp.Client.Models;

namespace DeltaLakeSharp.Benchmark
{
    [MemoryDiagnoser]
    public class V3PerformanceBenchmarks
    {
        private DeltaTableServiceClient _client = null!;
        private string _tablePath = null!;

        [ParamsSource(nameof(DatasetProfiles))]
        public string DatasetProfile { get; set; } = V3BenchmarkDatasetManager.SmallBaseline;

        [ParamsSource(nameof(PrefetchValues))]
        public bool PrefetchEnabled { get; set; }

        [Params(0, 1024, 8192)]
        public int BatchSize { get; set; }

        public IEnumerable<string> DatasetProfiles => V3BenchmarkDatasetManager
            .ResolveProfiles(Environment.GetEnvironmentVariable("DTS_BENCHMARK_V3_DATASET_FILTER"))
            .Where(profile => !string.Equals(profile, V3BenchmarkDatasetManager.CdfEnabled, StringComparison.OrdinalIgnoreCase));

        public IEnumerable<bool> PrefetchValues => V3BenchmarkParameterParser.ResolveBooleanValues(
            Environment.GetEnvironmentVariable("DTS_BENCHMARK_V3_PREFETCH"));

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            _tablePath = await V3BenchmarkDatasetManager.EnsureProfileAsync(DatasetProfile).ConfigureAwait(false);

            var options = new DeltaTableServiceClientOptions
            {
                EnableNativeReadPrefetch = PrefetchEnabled,
            };

            _client = new DeltaTableServiceClient(ServiceMode.V3_Rust, options);
            if (!await _client.HealthCheckAsync().ConfigureAwait(false))
            {
                throw new InvalidOperationException("The DeltaLakeSharp V3 native backend is not healthy.");
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _client?.Dispose();
        }

        [Benchmark(Description = "V3 public schema read")]
        public async Task<int> ReadPublicSchema()
        {
            TableSchema schema = await _client.GetSchemaAsync(_tablePath).ConfigureAwait(false);
            return schema.Columns.Count;
        }

        [Benchmark(Description = "V3 partition planning payload")]
        public async Task<PartitionPlanningResult> PlanReadPartitions()
        {
            IReadOnlyList<DeltaReadPartition> partitions = await _client.GetReadPartitionsAsync(_tablePath).ConfigureAwait(false);
            return PartitionPlanningResult.From(partitions);
        }

        [Benchmark(Description = "V3 first batch latency")]
        public async Task<FirstBatchReadResult> ReadFirstBatch()
        {
            int? batchSize = BatchSize == 0 ? null : BatchSize;
            var stopwatch = Stopwatch.StartNew();
            using IArrowArrayStream stream = await _client.ReadTableAsArrowStreamAsync(_tablePath, batchSize: batchSize).ConfigureAwait(false);
            long openTicks = stopwatch.ElapsedTicks;
            RecordBatch? batch = await stream.ReadNextRecordBatchAsync().ConfigureAwait(false);
            long firstBatchTicks = stopwatch.ElapsedTicks;

            return new FirstBatchReadResult(
                batch?.Length ?? 0,
                openTicks,
                firstBatchTicks);
        }

        [Benchmark(Description = "V3 full table Arrow scan")]
        public async Task<ReadIterationResult> FullScan()
        {
            int? batchSize = BatchSize == 0 ? null : BatchSize;
            return await ConsumeAsync(_client.ReadTableAsync(_tablePath, batchSize: batchSize)).ConfigureAwait(false);
        }

        private static async Task<ReadIterationResult> ConsumeAsync(IAsyncEnumerable<RecordBatch> batches)
        {
            long rowCount = 0;
            long batchCount = 0;

            await foreach (RecordBatch batch in batches.ConfigureAwait(false))
            {
                rowCount += batch.Length;
                batchCount++;
            }

            return new ReadIterationResult(rowCount, batchCount);
        }

    }

    [MemoryDiagnoser]
    public class V3PartitionConcurrencyBenchmarks
    {
        private DeltaTableServiceClient _client = null!;
        private string _tablePath = null!;
        private IReadOnlyList<DeltaReadPartition> _partitions = System.Array.Empty<DeltaReadPartition>();

        [ParamsSource(nameof(DatasetProfiles))]
        public string DatasetProfile { get; set; } = V3BenchmarkDatasetManager.Partitioned;

        [ParamsSource(nameof(PrefetchValues))]
        public bool PrefetchEnabled { get; set; }

        [ParamsSource(nameof(ConcurrencyValues))]
        public int Concurrency { get; set; }

        [Params(0, 1024, 8192)]
        public int BatchSize { get; set; }

        public IEnumerable<string> DatasetProfiles => V3BenchmarkDatasetManager
            .ResolveProfiles(Environment.GetEnvironmentVariable("DTS_BENCHMARK_V3_DATASET_FILTER"))
            .Where(profile => string.Equals(profile, V3BenchmarkDatasetManager.Partitioned, StringComparison.OrdinalIgnoreCase));

        public IEnumerable<bool> PrefetchValues => V3BenchmarkParameterParser.ResolveBooleanValues(
            Environment.GetEnvironmentVariable("DTS_BENCHMARK_V3_PREFETCH"));

        public IEnumerable<int> ConcurrencyValues => V3BenchmarkParameterParser.ResolveIntValues(
            Environment.GetEnvironmentVariable("DTS_BENCHMARK_V3_CONCURRENCY"),
            new[] { 1, 2, 4, 8 });

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            _tablePath = await V3BenchmarkDatasetManager.EnsureProfileAsync(DatasetProfile).ConfigureAwait(false);

            var options = new DeltaTableServiceClientOptions
            {
                EnableNativeReadPrefetch = PrefetchEnabled,
            };

            _client = new DeltaTableServiceClient(ServiceMode.V3_Rust, options);
            if (!await _client.HealthCheckAsync().ConfigureAwait(false))
            {
                throw new InvalidOperationException("The DeltaLakeSharp V3 native backend is not healthy.");
            }

            _partitions = await _client.GetReadPartitionsAsync(_tablePath).ConfigureAwait(false);
            if (_partitions.Count == 0)
            {
                throw new InvalidOperationException($"Partitioned benchmark dataset '{_tablePath}' produced no read partitions.");
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _client?.Dispose();
        }

        [Benchmark(Description = "V3 concurrent partition reads")]
        public async Task<ConcurrentPartitionReadResult> ReadPartitionsConcurrently()
        {
            int? batchSize = BatchSize == 0 ? null : BatchSize;
            using var semaphore = new SemaphoreSlim(Concurrency);
            Task<ReadIterationResult>[] tasks = _partitions
                .Select(partition => ReadPartitionAsync(partition, batchSize, semaphore))
                .ToArray();

            ReadIterationResult[] results = await Task.WhenAll(tasks).ConfigureAwait(false);
            return new ConcurrentPartitionReadResult(
                _partitions.Count,
                results.Sum(result => result.RowCount),
                results.Sum(result => result.BatchCount));
        }

        private async Task<ReadIterationResult> ReadPartitionAsync(DeltaReadPartition partition, int? batchSize, SemaphoreSlim semaphore)
        {
            await semaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                return await V3BenchmarkParameterParser.ConsumeAsync(_client.ReadTablePartitionAsync(_tablePath, partition, batchSize: batchSize)).ConfigureAwait(false);
            }
            finally
            {
                semaphore.Release();
            }
        }
    }

    [MemoryDiagnoser]
    public class V3CdfReadBenchmarks
    {
        private DeltaTableServiceClient _client = null!;
        private string _tablePath = null!;

        [ParamsSource(nameof(DatasetProfiles))]
        public string DatasetProfile { get; set; } = V3BenchmarkDatasetManager.CdfEnabled;

        [ParamsSource(nameof(PrefetchValues))]
        public bool PrefetchEnabled { get; set; }

        public IEnumerable<string> DatasetProfiles => V3BenchmarkDatasetManager
            .ResolveProfiles(Environment.GetEnvironmentVariable("DTS_BENCHMARK_V3_DATASET_FILTER"))
            .Where(profile => string.Equals(profile, V3BenchmarkDatasetManager.CdfEnabled, StringComparison.OrdinalIgnoreCase));

        public IEnumerable<bool> PrefetchValues => V3BenchmarkParameterParser.ResolveBooleanValues(
            Environment.GetEnvironmentVariable("DTS_BENCHMARK_V3_PREFETCH"));

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            _tablePath = await V3BenchmarkDatasetManager.EnsureProfileAsync(DatasetProfile).ConfigureAwait(false);
            var options = new DeltaTableServiceClientOptions
            {
                EnableNativeReadPrefetch = PrefetchEnabled,
            };

            _client = new DeltaTableServiceClient(ServiceMode.V3_Rust, options);
            if (!await _client.HealthCheckAsync().ConfigureAwait(false))
            {
                throw new InvalidOperationException("The DeltaLakeSharp V3 native backend is not healthy.");
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _client?.Dispose();
        }

        [Benchmark(Description = "V3 CDF first batch latency")]
        public async Task<FirstBatchReadResult> ReadCdfFirstBatch()
        {
            var stopwatch = Stopwatch.StartNew();
            using IArrowArrayStream stream = await _client.ReadChangeDataAsArrowStreamAsync(_tablePath, startingVersion: 1).ConfigureAwait(false);
            long openTicks = stopwatch.ElapsedTicks;
            RecordBatch? batch = await stream.ReadNextRecordBatchAsync().ConfigureAwait(false);
            long firstBatchTicks = stopwatch.ElapsedTicks;

            return new FirstBatchReadResult(batch?.Length ?? 0, openTicks, firstBatchTicks);
        }

        [Benchmark(Description = "V3 CDF full Arrow scan")]
        public async Task<ReadIterationResult> FullCdfScan()
        {
            return await V3BenchmarkParameterParser.ConsumeAsync(_client.ReadChangeDataAsync(_tablePath, startingVersion: 1)).ConfigureAwait(false);
        }
    }

    public readonly struct ReadIterationResult
    {
        public ReadIterationResult(long rowCount, long batchCount)
        {
            RowCount = rowCount;
            BatchCount = batchCount;
        }

        public long RowCount { get; }

        public long BatchCount { get; }
    }

    public readonly struct FirstBatchReadResult
    {
        public FirstBatchReadResult(long firstBatchRows, long openTicks, long firstBatchTicks)
        {
            FirstBatchRows = firstBatchRows;
            OpenTicks = openTicks;
            FirstBatchTicks = firstBatchTicks;
        }

        public long FirstBatchRows { get; }

        public long OpenTicks { get; }

        public long FirstBatchTicks { get; }
    }

    public readonly struct PartitionPlanningResult
    {
        public PartitionPlanningResult(int partitionCount, int totalTokenBytes, int maxTokenBytes, double averageTokenBytes)
        {
            PartitionCount = partitionCount;
            TotalTokenBytes = totalTokenBytes;
            MaxTokenBytes = maxTokenBytes;
            AverageTokenBytes = averageTokenBytes;
        }

        public int PartitionCount { get; }

        public int TotalTokenBytes { get; }

        public int MaxTokenBytes { get; }

        public double AverageTokenBytes { get; }

        public static PartitionPlanningResult From(IReadOnlyList<DeltaReadPartition> partitions)
        {
            int totalTokenBytes = partitions.Sum(partition => partition.Token.Length);
            int maxTokenBytes = partitions.Count == 0 ? 0 : partitions.Max(partition => partition.Token.Length);
            double averageTokenBytes = partitions.Count == 0 ? 0 : (double)totalTokenBytes / partitions.Count;
            return new PartitionPlanningResult(partitions.Count, totalTokenBytes, maxTokenBytes, averageTokenBytes);
        }
    }

    public readonly struct ConcurrentPartitionReadResult
    {
        public ConcurrentPartitionReadResult(int partitionCount, long rowCount, long batchCount)
        {
            PartitionCount = partitionCount;
            RowCount = rowCount;
            BatchCount = batchCount;
        }

        public int PartitionCount { get; }
        public long RowCount { get; }
        public long BatchCount { get; }
    }

    internal static class V3BenchmarkParameterParser
    {
        internal static IEnumerable<bool> ResolveBooleanValues(string? value)
        {
            if (string.IsNullOrWhiteSpace(value) || string.Equals(value, "both", StringComparison.OrdinalIgnoreCase))
            {
                return new[] { false, true };
            }

            return new[] { bool.Parse(value) };
        }

        internal static IEnumerable<int> ResolveIntValues(string? value, IReadOnlyList<int> defaultValues)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return defaultValues;
            }

            return value!
                .Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(v => int.Parse(v.Trim(), System.Globalization.CultureInfo.InvariantCulture));
        }

        internal static Task<ReadIterationResult> ConsumeAsync(IAsyncEnumerable<RecordBatch> batches)
        {
            return ConsumeCoreAsync(batches);
        }

        private static async Task<ReadIterationResult> ConsumeCoreAsync(IAsyncEnumerable<RecordBatch> batches)
        {
            long rowCount = 0;
            long batchCount = 0;

            await foreach (RecordBatch batch in batches.ConfigureAwait(false))
            {
                rowCount += batch.Length;
                batchCount++;
            }

            return new ReadIterationResult(rowCount, batchCount);
        }
    }
}