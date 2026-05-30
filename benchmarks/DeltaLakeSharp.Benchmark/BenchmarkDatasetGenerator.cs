using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using DeltaLakeSharp.Client;
using DeltaLakeSharp.Client.Models;

namespace DeltaLakeSharp.Benchmark
{
    internal static class BenchmarkDatasetGenerator
    {
        private const string TableName = "bench_dataset";

        internal static readonly string[] V3ProfileNames =
        {
            "small",
            "many-files",
            "wide",
            "partitioned",
            "cdf",
        };

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

        private static readonly TableSchema WideDatasetSchema = new(
            new[] { new ColumnDefinition("id", "long", nullable: false) }
                .Concat(Enumerable.Range(1, 100).Select(i => new ColumnDefinition($"value_{i:D3}", "int32", nullable: false)))
                .ToList());

        internal static string GetDefaultV3DatasetRoot()
        {
            string? explicitRoot = Environment.GetEnvironmentVariable("DTS_BENCHMARK_DATASET_ROOT");
            if (!string.IsNullOrWhiteSpace(explicitRoot))
            {
                return Path.GetFullPath(explicitRoot);
            }

            string? repositoryRoot = FindRepositoryRoot();
            string basePath = repositoryRoot ?? AppContext.BaseDirectory;
            return Path.Combine(basePath, "BenchmarkDotNet.Artifacts", "datasets", "v3");
        }

        internal static string GetV3DatasetPath(string profileName, string? outputRoot = null)
        {
            string normalizedProfile = NormalizeV3ProfileName(profileName);
            string root = string.IsNullOrWhiteSpace(outputRoot)
                ? GetDefaultV3DatasetRoot()
                : Path.GetFullPath(outputRoot);
            return Path.Combine(root, normalizedProfile);
        }

        internal static async Task GenerateV3ProfilesAsync(string? outputRoot, string? profileName, bool overwrite)
        {
            string[] profiles = string.IsNullOrWhiteSpace(profileName)
                ? V3ProfileNames
                : new[] { NormalizeV3ProfileName(profileName!) };

            foreach (string profile in profiles)
            {
                DatasetGeneratorOptions options = CreateV3ProfileOptions(profile, GetV3DatasetPath(profile, outputRoot), overwrite);
                await GenerateAsync(options).ConfigureAwait(false);
            }
        }

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
                    case "--profile":
                        options.Profile = NextValue();
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
                throw new ArgumentException("A dataset kind is required. Use --kind full-read|full-cdf|v3-profile|v3-profiles.");
            }

            if (!string.Equals(options.Kind, "full-read", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(options.Kind, "full-cdf", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(options.Kind, "v3-profile", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(options.Kind, "v3-profiles", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException("Dataset kind must be 'full-read', 'full-cdf', 'v3-profile', or 'v3-profiles'.");
            }

            if (string.Equals(options.Kind, "v3-profile", StringComparison.OrdinalIgnoreCase)
                || string.Equals(options.Kind, "v3-profiles", StringComparison.OrdinalIgnoreCase))
            {
                options.OutputPath = string.IsNullOrWhiteSpace(options.OutputPath)
                    ? GetDefaultV3DatasetRoot()
                    : Path.GetFullPath(options.OutputPath);
                return options;
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
                && !string.Equals(options.SchemaVariant, "decimal", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(options.SchemaVariant, "wide", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException("Schema variant must be 'default', 'decimal', or 'wide'.");
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
            Console.WriteLine("  dotnet run --project benchmarks/DeltaLakeSharp.Benchmark -- generate-dataset --kind <full-read|full-cdf> --output <path> [options]");
            Console.WriteLine("  dotnet run --project benchmarks/DeltaLakeSharp.Benchmark -- generate-dataset --kind v3-profiles [--output <root>] [--overwrite]");
            Console.WriteLine("  dotnet run --project benchmarks/DeltaLakeSharp.Benchmark -- generate-dataset --kind v3-profile --profile <small|many-files|wide|partitioned|cdf> [--output <root>] [--overwrite]");
            Console.WriteLine();
            Console.WriteLine("Options:");
            Console.WriteLine("  --profile <name>         V3 dataset profile for --kind v3-profile");
            Console.WriteLine("  --rows <n>               Initial row count (default: 1000000)");
            Console.WriteLine("  --batch-size <n>         Rows per insert batch/file target (default: 100000)");
            Console.WriteLine("  --versions <n>           Additional CDF versions to generate (default: 20)");
            Console.WriteLine("  --rows-per-version <n>   Rows appended in each extra CDF version (default: 50000)");
            Console.WriteLine("  --seed <n>               Deterministic random seed (default: 42)");
            Console.WriteLine("  --schema <name>          Schema variant: default or decimal (default: default)");
            Console.WriteLine("  --overwrite              Delete the output path first if it already exists");
            Console.WriteLine();
            Console.WriteLine("Examples:");
            Console.WriteLine("  dotnet run --project benchmarks/DeltaLakeSharp.Benchmark -- generate-dataset --kind full-read --output C:\\data\\delta-full-read --rows 10000000 --batch-size 250000 --overwrite");
            Console.WriteLine("  dotnet run --project benchmarks/DeltaLakeSharp.Benchmark -- generate-dataset --kind full-cdf --output C:\\data\\delta-full-cdf --rows 5000000 --batch-size 100000 --versions 24 --rows-per-version 25000 --overwrite");
        }

        private static async Task GenerateAsync(DatasetGeneratorOptions options)
        {
            if (string.Equals(options.Kind, "v3-profile", StringComparison.OrdinalIgnoreCase)
                || string.Equals(options.Kind, "v3-profiles", StringComparison.OrdinalIgnoreCase))
            {
                await GenerateV3ProfilesAsync(
                    options.OutputPath,
                    string.Equals(options.Kind, "v3-profile", StringComparison.OrdinalIgnoreCase) ? options.Profile : null,
                    options.Overwrite).ConfigureAwait(false);
                return;
            }

            string outputPath = Path.GetFullPath(options.OutputPath);
            if (!PrepareOutputPath(outputPath, options.Overwrite, options.SkipExisting))
            {
                Logger.Info($"Dataset already exists at '{outputPath}'. Use --overwrite to regenerate it.");
                return;
            }

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
            ExecuteResult createResult = await client.CreateTableAsync(
                outputPath,
                schema,
                configuration: configuration,
                partitionBy: options.PartitionByRegion ? new[] { "region" } : null);
            if (!createResult.Success)
            {
                throw new InvalidOperationException($"CreateTableAsync failed: {createResult.Message}");
            }

            if (options.UpgradeForChangeDataFeed)
            {
                ExecuteResult upgradeResult = await client.UpgradeTableProtocolAsync(
                    outputPath,
                    readerVersion: 3,
                    writerVersion: 7,
                    writerFeatures: new[] { "changeDataFeed" }).ConfigureAwait(false);
                if (!upgradeResult.Success)
                {
                    throw new InvalidOperationException($"UpgradeTableProtocolAsync failed: {upgradeResult.Message}");
                }
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

        private static bool PrepareOutputPath(string outputPath, bool overwrite, bool skipExisting)
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
                    if (skipExisting && Directory.Exists(Path.Combine(outputPath, "_delta_log")))
                    {
                        return false;
                    }

                    throw new IOException($"Output path '{outputPath}' already exists. Re-run with --overwrite to replace it.");
                }

                Directory.Delete(outputPath, recursive: true);
            }

            return true;
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
            if (schema.Columns.Count > 0 && string.Equals(schema.Columns[0].Name, "id", StringComparison.Ordinal) && schema.Columns.Count > 20)
            {
                return BuildWideBatch(schema, startingId, rowCount, seed, versionOrdinal, batchIndex);
            }

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

        private static RecordBatch BuildWideBatch(TableSchema schema, long startingId, int rowCount, int seed, int versionOrdinal, int batchIndex)
        {
            var rows = new object[rowCount][];
            int offset = unchecked(seed + versionOrdinal * 48611 + batchIndex * 167);

            for (int i = 0; i < rowCount; i++)
            {
                long id = startingId + i;
                var row = new object[schema.Columns.Count];
                row[0] = id;
                for (int column = 1; column < row.Length; column++)
                {
                    row[column] = (int)((id * 31 + column * 17 + offset) % 10_000);
                }

                rows[i] = row;
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
            if (string.Equals(options.SchemaVariant, "wide", StringComparison.OrdinalIgnoreCase))
            {
                return WideDatasetSchema;
            }

            return IsDecimalSchema(options) ? DecimalDatasetSchema : DatasetSchema;
        }

        private static bool IsDecimalSchema(DatasetGeneratorOptions options)
        {
            return string.Equals(options.SchemaVariant, "decimal", StringComparison.OrdinalIgnoreCase);
        }

        private static DatasetGeneratorOptions CreateV3ProfileOptions(string profileName, string outputPath, bool overwrite)
        {
            string normalizedProfile = NormalizeV3ProfileName(profileName);
            var options = new DatasetGeneratorOptions
            {
                Kind = normalizedProfile == "cdf" ? "full-cdf" : "full-read",
                OutputPath = outputPath,
                InitialRowCount = 128,
                BatchSize = 64,
                VersionCount = 0,
                RowsPerVersion = 0,
                Seed = 42,
                Overwrite = overwrite,
                SkipExisting = true,
            };

            switch (normalizedProfile)
            {
                case "small":
                    return options;
                case "many-files":
                    options.InitialRowCount = 48;
                    options.BatchSize = 1;
                    return options;
                case "wide":
                    options.InitialRowCount = 64;
                    options.BatchSize = 16;
                    options.SchemaVariant = "wide";
                    return options;
                case "partitioned":
                    options.InitialRowCount = 200;
                    options.BatchSize = 20;
                    options.PartitionByRegion = true;
                    return options;
                case "cdf":
                    options.InitialRowCount = 32;
                    options.BatchSize = 16;
                    options.VersionCount = 2;
                    options.RowsPerVersion = 8;
                    options.UpgradeForChangeDataFeed = true;
                    return options;
                default:
                    throw new ArgumentException($"Unknown V3 dataset profile '{profileName}'.", nameof(profileName));
            }
        }

        private static string NormalizeV3ProfileName(string profileName)
        {
            string normalized = profileName.Trim().ToLowerInvariant();
            if (V3ProfileNames.Contains(normalized, StringComparer.OrdinalIgnoreCase))
            {
                return normalized;
            }

            throw new ArgumentException($"Unknown V3 dataset profile '{profileName}'. Supported profiles: {string.Join(", ", V3ProfileNames)}.");
        }

        private static string? FindRepositoryRoot()
        {
            string? dir = AppContext.BaseDirectory;
            while (dir != null)
            {
                if (File.Exists(Path.Combine(dir, "DeltaLakeSharp.sln")))
                {
                    return dir;
                }

                dir = Path.GetDirectoryName(dir);
            }

            return null;
        }

        private sealed class DatasetGeneratorOptions
        {
            public string Kind { get; set; } = "full-read";

            public string? Profile { get; set; }

            public string OutputPath { get; set; } = string.Empty;

            public int InitialRowCount { get; set; } = 1_000_000;

            public int BatchSize { get; set; } = 100_000;

            public int VersionCount { get; set; } = 20;

            public int RowsPerVersion { get; set; } = 50_000;

            public int Seed { get; set; } = 42;

            public string SchemaVariant { get; set; } = "default";

            public bool Overwrite { get; set; }

            public bool SkipExisting { get; set; }

            public bool PartitionByRegion { get; set; }

            public bool UpgradeForChangeDataFeed { get; set; }

            public bool ShowHelp { get; set; }
        }
    }
}
