using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using DeltaLakeSharp.Client;

namespace DeltaLakeSharp.Benchmark
{
    /// <summary>
    /// Exercises V3 native read-stream prefetching under concurrent readers and
    /// optional slow-consumer backpressure.
    /// </summary>
    [MemoryDiagnoser]
    public class V3ReadConcurrencyBenchmark
    {
        private DeltaTableServiceClient[] _clients = System.Array.Empty<DeltaTableServiceClient>();
        private string _tablePath = null!;

        [Params(false, true)]
        public bool PrefetchEnabled { get; set; }

        [Params(1, 4, 8, 16, 32)]
        public int Concurrency { get; set; }

        /// <summary>
        /// Uses the backend default batch size when set to 0.
        /// </summary>
        [Params(0, 1024, 8192, 65536)]
        public int BatchSize { get; set; }

        /// <summary>
        /// Adds a small delay after each consumed batch to exercise native queue
        /// backpressure with slower managed consumers.
        /// </summary>
        [Params(0, 1)]
        public int ConsumerDelayMilliseconds { get; set; }

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            _tablePath = await ResolveTablePathAsync().ConfigureAwait(false);
            ValidateTablePath(_tablePath);

            var options = new DeltaTableServiceClientOptions
            {
                EnableNativeReadPrefetch = PrefetchEnabled,
            };

            _clients = Enumerable.Range(0, Concurrency)
                .Select(_ => new DeltaTableServiceClient(ServiceMode.V3_Rust, options))
                .ToArray();

            foreach (DeltaTableServiceClient client in _clients)
            {
                if (!await client.HealthCheckAsync().ConfigureAwait(false))
                {
                    throw new InvalidOperationException("The DeltaLakeSharp V3 native backend is not healthy.");
                }
            }

            await ConcurrentFullTableRead().ConfigureAwait(false);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            foreach (DeltaTableServiceClient client in _clients)
            {
                client.Dispose();
            }

            _clients = System.Array.Empty<DeltaTableServiceClient>();
        }

        [Benchmark(Description = "V3 concurrent Arrow table reads")]
        public async Task<ConcurrentReadIterationResult> ConcurrentFullTableRead()
        {
            int? batchSize = BatchSize == 0 ? null : BatchSize;
            int minAvailableWorkers = int.MaxValue;
            long workingSetBefore = Environment.WorkingSet;

            Task<ReadIterationResult>[] tasks = _clients
                .Select(client => ConsumeArrowBatchesAsync(
                    client.ReadTableAsync(_tablePath, batchSize: batchSize),
                    ConsumerDelayMilliseconds,
                    () => SampleThreadPool(ref minAvailableWorkers)))
                .ToArray();

            ReadIterationResult[] results = await Task.WhenAll(tasks).ConfigureAwait(false);
            long workingSetAfter = Environment.WorkingSet;

            return new ConcurrentReadIterationResult(
                results.Sum(result => result.RowCount),
                results.Sum(result => result.BatchCount),
                results.Min(result => result.RowCount),
                results.Max(result => result.RowCount),
                minAvailableWorkers == int.MaxValue ? -1 : minAvailableWorkers,
                workingSetBefore,
                workingSetAfter);
        }

        private static async Task<ReadIterationResult> ConsumeArrowBatchesAsync(
            IAsyncEnumerable<RecordBatch> batches,
            int consumerDelayMilliseconds,
            Action sampleThreadPool)
        {
            long rowCount = 0;
            long batchCount = 0;

            sampleThreadPool();
            await foreach (RecordBatch batch in batches.ConfigureAwait(false))
            {
                sampleThreadPool();
                batchCount++;
                rowCount += batch.Length;

                if (consumerDelayMilliseconds > 0)
                {
                    await Task.Delay(consumerDelayMilliseconds).ConfigureAwait(false);
                }
            }

            sampleThreadPool();
            return new ReadIterationResult(rowCount, batchCount);
        }

        private static void SampleThreadPool(ref int minAvailableWorkers)
        {
            ThreadPool.GetAvailableThreads(out int workerThreads, out _);

            while (true)
            {
                int current = Volatile.Read(ref minAvailableWorkers);
                if (workerThreads >= current)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref minAvailableWorkers, workerThreads, current) == current)
                {
                    return;
                }
            }
        }

        private static async Task<string> ResolveTablePathAsync()
        {
            string? explicitPath = Environment.GetEnvironmentVariable("DTS_BENCHMARK_READ_PATH");
            if (!string.IsNullOrWhiteSpace(explicitPath))
            {
                return Path.GetFullPath(explicitPath);
            }

            return await V3BenchmarkDatasetManager.EnsureProfileAsync(V3BenchmarkDatasetManager.Partitioned).ConfigureAwait(false);
        }

        private static void ValidateTablePath(string tablePath)
        {
            if (!Directory.Exists(tablePath))
            {
                throw new DirectoryNotFoundException(
                    $"Read benchmark dataset path '{tablePath}' does not exist. Generate it first with the benchmark dataset generator or set DTS_BENCHMARK_READ_PATH.");
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

        public readonly struct ConcurrentReadIterationResult
        {
            public ConcurrentReadIterationResult(
                long rowCount,
                long batchCount,
                long minRowsPerReader,
                long maxRowsPerReader,
                int minAvailableWorkerThreads,
                long workingSetBefore,
                long workingSetAfter)
            {
                RowCount = rowCount;
                BatchCount = batchCount;
                MinRowsPerReader = minRowsPerReader;
                MaxRowsPerReader = maxRowsPerReader;
                MinAvailableWorkerThreads = minAvailableWorkerThreads;
                WorkingSetBefore = workingSetBefore;
                WorkingSetAfter = workingSetAfter;
            }

            public long RowCount { get; }
            public long BatchCount { get; }
            public long MinRowsPerReader { get; }
            public long MaxRowsPerReader { get; }
            public int MinAvailableWorkerThreads { get; }
            public long WorkingSetBefore { get; }
            public long WorkingSetAfter { get; }
        }
    }
}