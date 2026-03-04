// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using Microsoft.ADMS.Testing.DeltaTableService.Client;
using Microsoft.ADMS.Testing.DeltaTableService.Client.Internal;
using Microsoft.ADMS.Testing.DeltaTableService.Client.Models;

namespace DeltaTableService.Benchmark
{
    /// <summary>
    /// Compares Delta table query performance between V1 (PySpark/Arrow Flight) and
    /// V2 (DataFusion/Arrow Flight) backends using local Docker containers.
    /// <para>
    /// The benchmark table uses a rich schema with partitioning on <c>region</c> (5 values)
    /// to exercise partition pruning, predicate pushdown, aggregation, and date range filtering.
    /// </para>
    /// <para>
    /// Benchmarked scenarios:
    /// <list type="bullet">
    ///   <item><b>ReadTable</b>: Full table scan via <c>ReadTableAsync</c>.</item>
    ///   <item><b>PartitionPruning</b>: <c>SELECT * WHERE region = 'US-East'</c> — skips 4/5 of data files.</item>
    ///   <item><b>PredicatePushdown</b>: <c>SELECT id, amount WHERE category AND is_active</c> — pushdown to Parquet.</item>
    ///   <item><b>AggregateGroupBy</b>: <c>SUM/AVG GROUP BY region, category</c> — aggregation + grouping.</item>
    ///   <item><b>DateRangeFilter</b>: <c>WHERE event_date BETWEEN</c> — data file skipping via min/max stats.</item>
    /// </list>
    /// </para>
    /// </summary>
    public class DeltaTableBenchmark
    {
        // ------------------------------------------------------------------ //
        //  Constants
        // ------------------------------------------------------------------ //

        private const int MaxHealthCheckAttempts = 60;
        private const int HealthCheckDelayMs = 2000;

        private const string V1TablePath = "/tmp/bench_v1";
        private const string V2TablePath = "/tmp/bench_v2";
        private const string TableName = "bench";

        /// <summary>Deterministic seed for reproducible data generation.</summary>
        private const int RandomSeed = 42;

        // ------------------------------------------------------------------ //
        //  Data generation constants
        // ------------------------------------------------------------------ //

        private static readonly string[] Regions = { "US-East", "US-West", "EU-West", "APAC", "SA-East" };
        private static readonly string[] Categories =
        {
            "Electronics", "Clothing", "Home", "Books", "Sports",
            "Toys", "Food", "Health", "Auto", "Garden",
            "Music", "Office", "Pet", "Beauty", "Tools",
            "Baby", "Shoes", "Jewelry", "Games", "Movies",
        };

        // ------------------------------------------------------------------ //
        //  Parameters
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Number of rows to seed in the benchmark table.
        /// BenchmarkDotNet runs all benchmark methods for each parameter value.
        /// </summary>
        [Params(1000000)]
        public int RowCount { get; set; }

        // ------------------------------------------------------------------ //
        //  Infrastructure
        // ------------------------------------------------------------------ //

        private DeltaTableContainer _v1Container = null!;
        private DeltaTableContainer _v2Container = null!;
        private DeltaTableServiceClient _v1Client = null!;
        private DeltaTableServiceClient _v2Client = null!;
        private string _dockerfilePath = null!;

        /// <summary>
        /// Rich benchmark schema:
        /// id (long PK), region (string, partition), category (string), amount (double),
        /// quantity (int), is_active (bool), event_date (date), created_at (timestamp),
        /// description (string).
        /// </summary>
        private static readonly TableSchema BenchSchema = new(new List<ColumnDefinition>
        {
            new("id", "long", nullable: false),
            new("region", "string", nullable: false),
            new("category", "string", nullable: false),
            new("amount", "double", nullable: false),
            new("quantity", "int32", nullable: false),
            new("is_active", "boolean", nullable: false),
            new("event_date", "date", nullable: false),
            new("created_at", "timestamp", nullable: false),
            new("description", "string", nullable: true),
        });

        private static readonly string[] PartitionColumns = { "region" };

        // ------------------------------------------------------------------ //
        //  Setup / Teardown
        // ------------------------------------------------------------------ //

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            _dockerfilePath = GetDockerfilePath();
            Logger.Info($"Docker build context: {_dockerfilePath}");
            Logger.Info($"Setting up benchmark with RowCount={RowCount}...");

            // Start both containers in parallel.
            _v1Container = new DeltaTableContainer();
            _v2Container = new DeltaTableContainer();

            var sw = Stopwatch.StartNew();

            var v1Start = _v1Container.BuildAndStartAsync(
                dockerfilePath: _dockerfilePath,
                imageName: "delta-table-service:bench",
                skipBuildIfExists: true);

            var v2Start = _v2Container.BuildAndStartAsync(
                dockerfilePath: _dockerfilePath,
                mode: ServiceMode.V2_DataFusion,
                imageName: "delta-table-service-v2:bench",
                skipBuildIfExists: true);

            await Task.WhenAll(v1Start, v2Start);
            Logger.Info($"Containers started in {sw.Elapsed.TotalSeconds:F1}s.");

            // Create clients.
            _v1Client = new DeltaTableServiceClient(_v1Container.GetFlightUri());
            _v2Client = new DeltaTableServiceClient(_v2Container.GetFlightUri(), ServiceMode.V2_DataFusion);

            // Wait for health checks.
            await WaitForHealthAsync(_v1Client, "V1");
            await WaitForHealthAsync(_v2Client, "V2");

            // Seed test data (identical rows, partitioned by region).
            object[][] rows = GenerateRows(RowCount);
            await SeedDataAsync(_v1Client, V1TablePath, rows);
            await SeedDataAsync(_v2Client, V2TablePath, rows);
            Logger.Info($"Data seeded ({RowCount} rows per backend, partitioned by region into {Regions.Length} partitions).");

            // Warmup reads — ensures JIT, Spark class-loading, and internal caching are done
            // before BenchmarkDotNet starts measuring.
            await _v1Client.ReadTableAsync(V1TablePath).ToListAsync();
            await _v2Client.ReadTableAsync(V2TablePath).ToListAsync();
            await RunSqlAsync(_v1Client, V1TablePath, $"SELECT COUNT(*) FROM {TableName}");
            await RunSqlAsync(_v2Client, V2TablePath, $"SELECT COUNT(*) FROM {TableName}");

            Logger.Info($"GlobalSetup complete in {sw.Elapsed.TotalSeconds:F1}s.");
        }

        [GlobalCleanup]
        public async Task GlobalCleanup()
        {
            Logger.Info("Cleaning up benchmark resources...");
            _v1Client?.Dispose();
            _v2Client?.Dispose();
            await (_v1Container?.DisposeAsync() ?? ValueTask.CompletedTask);
            await (_v2Container?.DisposeAsync() ?? ValueTask.CompletedTask);
            Logger.Info("Cleanup complete.");
        }

        // ------------------------------------------------------------------ //
        //  Benchmark: ReadTable (full table scan)
        // ------------------------------------------------------------------ //

        [Benchmark(Baseline = true, Description = "V1 ReadTable")]
        public async Task<List<RecordBatch>> V1_ReadTable()
        {
            return await _v1Client.ReadTableAsync(V1TablePath).ToListAsync();
        }

        [Benchmark(Description = "V2 ReadTable")]
        public async Task<List<RecordBatch>> V2_ReadTable()
        {
            return await _v2Client.ReadTableAsync(V2TablePath).ToListAsync();
        }

        // ------------------------------------------------------------------ //
        //  Benchmark: PartitionPruning
        //  SELECT * FROM bench WHERE region = 'US-East'
        //  Should skip 4/5 of data files.
        // ------------------------------------------------------------------ //

        [Benchmark(Description = "V1 PartitionPruning")]
        public async Task<List<RecordBatch>> V1_PartitionPruning()
        {
            return await RunSqlAsync(_v1Client, V1TablePath,
                $"SELECT * FROM {TableName} WHERE region = 'US-East'");
        }

        [Benchmark(Description = "V2 PartitionPruning")]
        public async Task<List<RecordBatch>> V2_PartitionPruning()
        {
            return await RunSqlAsync(_v2Client, V2TablePath,
                $"SELECT * FROM {TableName} WHERE region = 'US-East'");
        }

        // ------------------------------------------------------------------ //
        //  Benchmark: PredicatePushdown
        //  SELECT id, amount FROM bench WHERE category = 'Electronics' AND is_active = true
        //  Tests predicate pushdown to Parquet reader.
        // ------------------------------------------------------------------ //

        [Benchmark(Description = "V1 PredicatePushdown")]
        public async Task<List<RecordBatch>> V1_PredicatePushdown()
        {
            return await RunSqlAsync(_v1Client, V1TablePath,
                $"SELECT id, amount FROM {TableName} WHERE category = 'Electronics' AND is_active = true");
        }

        [Benchmark(Description = "V2 PredicatePushdown")]
        public async Task<List<RecordBatch>> V2_PredicatePushdown()
        {
            return await RunSqlAsync(_v2Client, V2TablePath,
                $"SELECT id, amount FROM {TableName} WHERE category = 'Electronics' AND is_active = true");
        }

        // ------------------------------------------------------------------ //
        //  Benchmark: AggregateGroupBy
        //  SELECT region, category, SUM(amount), AVG(quantity) FROM bench
        //  GROUP BY region, category
        // ------------------------------------------------------------------ //

        [Benchmark(Description = "V1 AggregateGroupBy")]
        public async Task<List<RecordBatch>> V1_AggregateGroupBy()
        {
            return await RunSqlAsync(_v1Client, V1TablePath,
                $"SELECT region, category, SUM(amount) AS total_amount, AVG(quantity) AS avg_qty FROM {TableName} GROUP BY region, category");
        }

        [Benchmark(Description = "V2 AggregateGroupBy")]
        public async Task<List<RecordBatch>> V2_AggregateGroupBy()
        {
            return await RunSqlAsync(_v2Client, V2TablePath,
                $"SELECT region, category, SUM(amount) AS total_amount, AVG(quantity) AS avg_qty FROM {TableName} GROUP BY region, category");
        }

        // ------------------------------------------------------------------ //
        //  Benchmark: DateRangeFilter
        //  SELECT id, amount, event_date FROM bench
        //  WHERE event_date BETWEEN '2024-01-01' AND '2024-06-30'
        //  Tests data file skipping via min/max stats.
        // ------------------------------------------------------------------ //

        [Benchmark(Description = "V1 DateRangeFilter")]
        public async Task<List<RecordBatch>> V1_DateRangeFilter()
        {
            return await RunSqlAsync(_v1Client, V1TablePath,
                $"SELECT id, amount, event_date FROM {TableName} WHERE event_date BETWEEN '2024-01-01' AND '2024-06-30'");
        }

        [Benchmark(Description = "V2 DateRangeFilter")]
        public async Task<List<RecordBatch>> V2_DateRangeFilter()
        {
            return await RunSqlAsync(_v2Client, V2TablePath,
                $"SELECT id, amount, event_date FROM {TableName} WHERE event_date BETWEEN '2024-01-01' AND '2024-06-30'");
        }

        // ------------------------------------------------------------------ //
        //  Helpers
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Executes a SQL query against the given backend, registering the table atomically.
        /// Both V1 and V2 support the <c>ExecuteQueryAsync(sql, tablePath, tableName)</c> overload.
        /// </summary>
        private static async Task<List<RecordBatch>> RunSqlAsync(
            DeltaTableServiceClient client, string tablePath, string sql)
        {
            return await client.ExecuteQueryAsync(sql, tablePath, TableName).ToListAsync();
        }

        /// <summary>
        /// Generates <paramref name="count"/> rows of deterministic benchmark data.
        /// </summary>
        private static object[][] GenerateRows(int count)
        {
            var rng = new Random(RandomSeed);
            var rows = new object[count][];
            var baseDate = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            for (int i = 0; i < count; i++)
            {
                string region = Regions[i % Regions.Length];
                string category = Categories[rng.Next(Categories.Length)];
                double amount = Math.Round(rng.NextDouble() * 1000.0, 2);
                int quantity = rng.Next(1, 100);
                bool isActive = rng.Next(2) == 1;
                // Spread event_date over ~2 years (730 days) from 2024-01-01.
                DateTime eventDate = baseDate.AddDays(rng.Next(730));
                DateTimeOffset createdAt = new DateTimeOffset(
                    baseDate.AddDays(rng.Next(730)).AddSeconds(rng.Next(86400)), TimeSpan.Zero);
                string description = $"Item {i + 1}: {category} in {region}";

                rows[i] = new object[]
                {
                    (long)(i + 1),   // id
                    region,           // region (partition column)
                    category,         // category
                    amount,           // amount
                    quantity,         // quantity
                    isActive,         // is_active
                    eventDate,        // event_date (Date32)
                    createdAt,        // created_at (Timestamp)
                    description,      // description
                };
            }

            return rows;
        }

        /// <summary>
        /// Seeds a backend with the given rows, using partition_by on region.
        /// </summary>
        private static async Task SeedDataAsync(
            DeltaTableServiceClient client, string tablePath, object[][] rows)
        {
            RecordBatch batch = ArrowConverter.FromRows(rows, BenchSchema);
            IAsyncEnumerable<RecordBatch> stream = ArrowConverter.ToAsyncEnumerable(batch);
            await client.InsertAsync(tablePath, batch.Schema, stream, SaveMode.Overwrite,
                partitionBy: PartitionColumns);
        }

        private static async Task WaitForHealthAsync(DeltaTableServiceClient client, string label)
        {
            Logger.Info($"Waiting for {label} to become healthy...");
            var sw = Stopwatch.StartNew();

            for (int attempt = 0; attempt < MaxHealthCheckAttempts; attempt++)
            {
                try
                {
                    bool healthy = await client.HealthCheckAsync();
                    if (healthy)
                    {
                        Logger.Info($"{label} healthy after {sw.Elapsed.TotalSeconds:F1}s ({attempt + 1} attempts).");
                        return;
                    }
                }
                catch
                {
                    // Server may not be ready yet — swallow and retry.
                }

                await Task.Delay(HealthCheckDelayMs);
            }

            throw new TimeoutException(
                $"{label} did not become healthy within {MaxHealthCheckAttempts * HealthCheckDelayMs / 1000}s.");
        }

        private static string GetDockerfilePath()
        {
            string assemblyDir = Path.GetDirectoryName(typeof(DeltaTableBenchmark).Assembly.Location)!;
            string path = Path.Combine(assemblyDir, "DeltaTableService.Server");

            if (!Directory.Exists(path))
            {
                throw new DirectoryNotFoundException(
                    $"Docker build context not found at '{path}'. " +
                    $"Ensure the DeltaTableService.Server content is copied to the output directory.");
            }

            return path;
        }
    }
}
