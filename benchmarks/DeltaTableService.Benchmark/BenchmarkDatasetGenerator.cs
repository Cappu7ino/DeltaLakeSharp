using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Microsoft.DI.DeltaTableService.Client;
using Microsoft.DI.DeltaTableService.Client.Models;

namespace DeltaTableService.Benchmark
{
    internal static class BenchmarkDatasetGenerator
    {
        private const string TableName = "bench_dataset";

        private static readonly string[] Regions =
        {
            "US-East", "US-West", "EU-West", "APAC", "SA-East",
        };

        private static readonly string[] Categories =
        {
            "Electronics", "Clothing", "Home", "Books", "Sports",
            "Toys", "Food", "Health", "Auto", "Garden",
        };

        private static readonly TableSchema DatasetSchema = new(new List<ColumnDefinition>
        {
            new("id", "long", nullable: false),
            new("tenant_id", "int32", nullable: false),
            new("event_ts", "timestamp", nullable: false),
            new("region", "string", nullable: false),
            new("category", "string", nullable: false),
            new("amount", "int32", nullable: false),
            new("quantity", "int32", nullable: false),
            new("is_active", "boolean", nullable: false),
            new("note", "string", nullable: true),
        });

        private static readonly TableSchema DecimalDatasetSchema = new(new List<ColumnDefinition>
        {
            new("id", "long", nullable: false),
            new("tenant_id", "int32", nullable: false),
            new("event_ts", "timestamp", nullable: false),
            new("region", "string", nullable: false),
            new("category", "string", nullable: false),
            new("amount", "int32", nullable: false),
            new("unit_price", "decimal(18,2)", nullable: false),
            new("quantity", "int32", nullable: false),
            new("is_active", "boolean", nullable: false),
            new("note", "string", nullable: true),
        });

        internal static async Task<int> RunAsync(string[] args)
        {
            DatasetGeneratorOptions options = Parse(args);

            if (options.ShowHelp)
            {
                PrintUsage();
                return 0;
            }

            await GenerateAsync(options);
            return 0;
        }

        private static DatasetGeneratorOptions Parse(string[] args)
        {
            var options = new DatasetGeneratorOptions();

            for (int i = 0; i < args.Length; i++)
            {
                string arg = args[i];
                string NextValue()
                {
                    if (i + 1 >= args.Length)
                    {
                        throw new ArgumentException($"Missing value for option '{arg}'.");
                    }

                    i++;
                    return args[i];
                }

                switch (arg)
                {
                    case "--help":
                    case "-h":
                        options.ShowHelp = true;
                        break;
                    case "--kind":
                        options.Kind = NextValue();
                        break;
                    case "--output":
                        options.OutputPath = NextValue();
                        break;
                    case "--rows":
                        options.InitialRowCount = ParseInt32(NextValue(), "rows");
                        break;
                    case "--batch-size":
                        options.BatchSize = ParseInt32(NextValue(), "batch-size");
                        break;
                    case "--versions":
                        options.VersionCount = ParseInt32(NextValue(), "versions");
                        break;
                    case "--rows-per-version":
                        options.RowsPerVersion = ParseInt32(NextValue(), "rows-per-version");
                        break;
                    case "--seed":
                        options.Seed = ParseInt32(NextValue(), "seed");
                        break;
                    case "--schema":
                        options.SchemaVariant = NextValue();
                        break;
                    case "--overwrite":
                        options.Overwrite = true;
                        break;
                    default:
                        throw new ArgumentException($"Unknown option '{arg}'. Use --help for usage.");
                }
            }

            if (options.ShowHelp)
            {
                return options;
            }

            if (string.IsNullOrWhiteSpace(options.Kind))
            {
                throw new ArgumentException("A dataset kind is required. Use --kind full-read|full-cdf.");
            }

            if (!string.Equals(options.Kind, "full-read", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(options.Kind, "full-cdf", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException("Dataset kind must be 'full-read' or 'full-cdf'.");
            }

            if (string.IsNullOrWhiteSpace(options.OutputPath))
            {
                throw new ArgumentException("An output path is required. Use --output <path>.");
            }

            if (options.InitialRowCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(options.InitialRowCount), "Initial row count must be > 0.");
            }

            if (options.BatchSize <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(options.BatchSize), "Batch size must be > 0.");
            }

            if (options.VersionCount < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(options.VersionCount), "Version count must be >= 0.");
            }

            if (options.RowsPerVersion < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(options.RowsPerVersion), "Rows per version must be >= 0.");
            }

            if (!string.Equals(options.SchemaVariant, "default", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(options.SchemaVariant, "decimal", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException("Schema variant must be 'default' or 'decimal'.");
            }

            return options;
        }

        private static int ParseInt32(string value, string name)
        {
            if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed))
            {
                throw new ArgumentException($"Option '{name}' expects an integer value.");
            }

            return parsed;
        }

        private static void PrintUsage()
        {
            Logger.Info("Dataset generator usage:");
            Console.WriteLine("  dotnet run --project benchmarks/DeltaTableService.Benchmark -- generate-dataset --kind <full-read|full-cdf> --output <path> [options]");
            Console.WriteLine();
            Console.WriteLine("Options:");
            Console.WriteLine("  --rows <n>               Initial row count (default: 1000000)");
            Console.WriteLine("  --batch-size <n>         Rows per insert batch/file target (default: 100000)");
            Console.WriteLine("  --versions <n>           Additional CDF versions to generate (default: 20)");
            Console.WriteLine("  --rows-per-version <n>   Rows appended in each extra CDF version (default: 50000)");
            Console.WriteLine("  --seed <n>               Deterministic random seed (default: 42)");
            Console.WriteLine("  --schema <name>          Schema variant: default or decimal (default: default)");
            Console.WriteLine("  --overwrite              Delete the output path first if it already exists");
            Console.WriteLine();
            Console.WriteLine("Examples:");
            Console.WriteLine("  dotnet run --project benchmarks/DeltaTableService.Benchmark -- generate-dataset --kind full-read --output C:\\data\\delta-full-read --rows 10000000 --batch-size 250000 --overwrite");
            Console.WriteLine("  dotnet run --project benchmarks/DeltaTableService.Benchmark -- generate-dataset --kind full-cdf --output C:\\data\\delta-full-cdf --rows 5000000 --batch-size 100000 --versions 24 --rows-per-version 25000 --overwrite");
        }

        private static async Task GenerateAsync(DatasetGeneratorOptions options)
        {
            string outputPath = Path.GetFullPath(options.OutputPath);
            PrepareOutputPath(outputPath, options.Overwrite);

            Logger.Info($"Generating {options.Kind} dataset at '{outputPath}'...");
            Logger.Info($"InitialRows={options.InitialRowCount}, BatchSize={options.BatchSize}, Versions={options.VersionCount}, RowsPerVersion={options.RowsPerVersion}, Seed={options.Seed}");

            using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
            bool healthy = await client.HealthCheckAsync();
            if (!healthy)
            {
                throw new InvalidOperationException("The V3 native client is not healthy.");
            }

            Dictionary<string, string> configuration = string.Equals(options.Kind, "full-cdf", StringComparison.OrdinalIgnoreCase)
                ? new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["delta.enableChangeDataFeed"] = "true",
                }
                : new Dictionary<string, string>(StringComparer.Ordinal);

            TableSchema schema = ResolveSchema(options);
            ExecuteResult createResult = await client.CreateTableAsync(outputPath, schema, configuration: configuration);
            if (!createResult.Success)
            {
                throw new InvalidOperationException($"CreateTableAsync failed: {createResult.Message}");
            }

            long nextId = 1;
            bool includeDecimal = IsDecimalSchema(options);
            nextId = await AppendRowsAsync(client, outputPath, schema, nextId, options.InitialRowCount, options.BatchSize, options.Seed, versionOrdinal: 0, includeDecimal: includeDecimal);

            if (string.Equals(options.Kind, "full-cdf", StringComparison.OrdinalIgnoreCase))
            {
                nextId = await GenerateCdfHistoryAsync(client, outputPath, schema, nextId, options, includeDecimal);
            }

            long latestRowCount = await CountRowsAsync(client, outputPath);
            Logger.Info($"Dataset generation complete. Latest visible row count: {latestRowCount:N0}");
        }

        private static void PrepareOutputPath(string outputPath, bool overwrite)
        {
            string? parent = Path.GetDirectoryName(outputPath);
            if (!string.IsNullOrEmpty(parent))
            {
                Directory.CreateDirectory(parent);
            }

            if (Directory.Exists(outputPath))
            {
                if (!overwrite)
                {
                    throw new IOException($"Output path '{outputPath}' already exists. Re-run with --overwrite to replace it.");
                }

                Directory.Delete(outputPath, recursive: true);
            }
        }

        private static async Task<long> GenerateCdfHistoryAsync(
            DeltaTableServiceClient client,
            string outputPath,
            TableSchema schema,
            long nextId,
            DatasetGeneratorOptions options,
            bool includeDecimal)
        {
            for (int version = 1; version <= options.VersionCount; version++)
            {
                Logger.Info($"Generating CDF version {version}/{options.VersionCount}...");

                if (options.RowsPerVersion > 0)
                {
                    nextId = await AppendRowsAsync(
                        client,
                        outputPath,
                        schema,
                        nextId,
                        options.RowsPerVersion,
                        options.BatchSize,
                        options.Seed + version * 7919,
                        version,
                        includeDecimal);
                }

                int updateModulo = 17;
                int updateRemainder = version % updateModulo;
                string setClause = includeDecimal
                    ? $"amount = amount + {version}, unit_price = unit_price + {version}.25, note = 'updated_v{version}'"
                    : $"amount = amount + {version}, note = 'updated_v{version}'";
                ExecuteResult updateResult = await client.UpdateAsync(
                    $"UPDATE {TableName} SET {setClause} WHERE id % {updateModulo} = {updateRemainder} AND is_active = true",
                    outputPath,
                    TableName);
                if (!updateResult.Success)
                {
                    throw new InvalidOperationException($"UpdateAsync failed for version {version}: {updateResult.Message}");
                }

                int deleteModulo = 29;
                int deleteRemainder = (version * 3) % deleteModulo;
                ExecuteResult deleteResult = await client.DeleteAsync(
                    $"DELETE FROM {TableName} WHERE id % {deleteModulo} = {deleteRemainder} AND tenant_id % 7 = 0",
                    outputPath,
                    TableName);
                if (!deleteResult.Success)
                {
                    throw new InvalidOperationException($"DeleteAsync failed for version {version}: {deleteResult.Message}");
                }
            }

            return nextId;
        }

        private static async Task<long> AppendRowsAsync(
            DeltaTableServiceClient client,
            string outputPath,
            TableSchema schema,
            long nextId,
            int rowCount,
            int batchSize,
            int seed,
            int versionOrdinal,
            bool includeDecimal)
        {
            int remaining = rowCount;
            int batchIndex = 0;

            while (remaining > 0)
            {
                int currentBatchSize = Math.Min(batchSize, remaining);
                RecordBatch batch = BuildBatch(schema, nextId, currentBatchSize, seed, versionOrdinal, batchIndex, includeDecimal);
                await client.InsertAsync(
                    outputPath,
                    batch.Schema,
                    SingleBatchAsync(batch),
                    SaveMode.Append);

                nextId += currentBatchSize;
                remaining -= currentBatchSize;
                batchIndex++;
            }

            return nextId;
        }

        private static async Task<long> CountRowsAsync(DeltaTableServiceClient client, string outputPath)
        {
            long rowCount = 0;
            await foreach (RecordBatch batch in client.ExecuteQueryAsync($"SELECT COUNT(*) AS cnt FROM {TableName}", outputPath, TableName))
            {
                if (batch.Length == 0 || batch.ColumnCount == 0)
                {
                    continue;
                }

                object? rawValue = ArrowConverter.ToDataTable(new[] { batch }).Rows[0][0];
                if (rawValue != null)
                {
                    rowCount = Convert.ToInt64(rawValue, CultureInfo.InvariantCulture);
                }
            }

            return rowCount;
        }

        private static RecordBatch BuildBatch(TableSchema schema, long startingId, int rowCount, int seed, int versionOrdinal, int batchIndex, bool includeDecimal)
        {
            var random = new Random(unchecked(seed + versionOrdinal * 48611 + batchIndex * 167));
            var baseTimestamp = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero).AddDays(versionOrdinal);
            var rows = new object[rowCount][];

            for (int i = 0; i < rowCount; i++)
            {
                long id = startingId + i;
                int tenantId = (int)((id % 997) + 1);
                DateTimeOffset eventTimestamp = baseTimestamp
                    .AddMinutes((id + versionOrdinal) % 10080)
                    .AddSeconds(random.Next(60));
                string region = Regions[(int)(id % Regions.Length)];
                string category = Categories[(int)((id / 3) % Categories.Length)];
                int amount = 10 + (int)((id * 13 + versionOrdinal * 7) % 5000);
                decimal unitPrice = decimal.Round(((amount * 100m) + ((id + versionOrdinal) % 100)) / 100m, 2, MidpointRounding.AwayFromZero);
                int quantity = 1 + (int)((id + versionOrdinal) % 25);
                bool isActive = ((id + versionOrdinal) % 5) != 0;
                string note = $"v{versionOrdinal}-tenant-{tenantId}-row-{id}";

                rows[i] = includeDecimal
                    ? new object[]
                    {
                        id,
                        tenantId,
                        eventTimestamp,
                        region,
                        category,
                        amount,
                        unitPrice,
                        quantity,
                        isActive,
                        note,
                    }
                    : new object[]
                    {
                        id,
                        tenantId,
                        eventTimestamp,
                        region,
                        category,
                        amount,
                        quantity,
                        isActive,
                        note,
                    };
            }

            return ArrowConverter.FromRows(rows, schema);
        }

        private static async IAsyncEnumerable<RecordBatch> SingleBatchAsync(RecordBatch batch)
        {
            yield return batch;
            await Task.CompletedTask;
        }

        private static TableSchema ResolveSchema(DatasetGeneratorOptions options)
        {
            return IsDecimalSchema(options) ? DecimalDatasetSchema : DatasetSchema;
        }

        private static bool IsDecimalSchema(DatasetGeneratorOptions options)
        {
            return string.Equals(options.SchemaVariant, "decimal", StringComparison.OrdinalIgnoreCase);
        }

        private sealed class DatasetGeneratorOptions
        {
            public string Kind { get; set; } = "full-read";

            public string OutputPath { get; set; } = string.Empty;

            public int InitialRowCount { get; set; } = 1_000_000;

            public int BatchSize { get; set; } = 100_000;

            public int VersionCount { get; set; } = 20;

            public int RowsPerVersion { get; set; } = 50_000;

            public int Seed { get; set; } = 42;

            public string SchemaVariant { get; set; } = "default";

            public bool Overwrite { get; set; }

            public bool ShowHelp { get; set; }
        }
    }
}
