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
    internal static class V3BenchmarkDatasetManager
    {
        internal const string SmallBaseline = "small";
        internal const string ManyFiles = "many-files";
        internal const string WideSchema = "wide";
        internal const string Partitioned = "partitioned";
        internal const string CdfEnabled = "cdf";

        private const string TableName = "bench";

        private static readonly string[] ProfileNames =
        {
            SmallBaseline,
            ManyFiles,
            WideSchema,
            Partitioned,
            CdfEnabled,
        };

        private static readonly TableSchema DefaultSchema = new(new List<ColumnDefinition>
        {
            new("id", "long", nullable: false),
            new("region", "string", nullable: false),
            new("category", "string", nullable: false),
            new("amount", "int32", nullable: false),
            new("quantity", "int32", nullable: false),
            new("is_active", "boolean", nullable: false),
            new("note", "string", nullable: true),
        });

        internal static IReadOnlyList<string> ResolveProfiles(string? profileFilter)
        {
            if (string.IsNullOrWhiteSpace(profileFilter) || string.Equals(profileFilter, "all", StringComparison.OrdinalIgnoreCase))
            {
                return ProfileNames;
            }

            string[] requested = profileFilter!
                .Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(p => p.Trim())
                .ToArray();
            var resolved = new List<string>(requested.Length);

            foreach (string profile in requested)
            {
                string? match = ProfileNames.FirstOrDefault(p => string.Equals(p, profile, StringComparison.OrdinalIgnoreCase));
                if (match == null)
                {
                    throw new ArgumentException($"Unknown V3 benchmark dataset profile '{profile}'. Valid values: {string.Join(", ", ProfileNames)}.");
                }

                resolved.Add(match);
            }

            return resolved;
        }

        internal static string ResolveDatasetRoot()
        {
            string? explicitRoot = Environment.GetEnvironmentVariable("DTS_BENCHMARK_V3_DATASET_ROOT");
            return string.IsNullOrWhiteSpace(explicitRoot)
                ? Path.Combine(AppContext.BaseDirectory, "TestData", "v3-phase9")
                : Path.GetFullPath(explicitRoot);
        }

        internal static string ResolveProfilePath(string profile)
        {
            return Path.Combine(ResolveDatasetRoot(), profile);
        }

        internal static async Task GenerateSelectedProfilesAsync(string? profileFilter, string? outputRoot, bool overwrite)
        {
            if (!string.IsNullOrWhiteSpace(outputRoot))
            {
                Environment.SetEnvironmentVariable("DTS_BENCHMARK_V3_DATASET_ROOT", Path.GetFullPath(outputRoot));
            }

            foreach (string profile in ResolveProfiles(profileFilter))
            {
                await EnsureProfileAsync(profile, overwrite).ConfigureAwait(false);
            }
        }

        internal static async Task<string> EnsureProfileAsync(string profile, bool overwrite = false)
        {
            string resolvedProfile = ResolveProfiles(profile).Single();
            string tablePath = ResolveProfilePath(resolvedProfile);

            if (Directory.Exists(Path.Combine(tablePath, "_delta_log")) && !overwrite)
            {
                Logger.Info($"Reusing V3 benchmark dataset '{resolvedProfile}' at '{tablePath}'.");
                return tablePath;
            }

            if (Directory.Exists(tablePath))
            {
                Directory.Delete(tablePath, recursive: true);
            }

            Directory.CreateDirectory(Path.GetDirectoryName(tablePath)!);
            Logger.Info($"Generating V3 benchmark dataset '{resolvedProfile}' at '{tablePath}'.");

            using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
            bool healthy = await client.HealthCheckAsync().ConfigureAwait(false);
            if (!healthy)
            {
                throw new InvalidOperationException("The DeltaLakeSharp V3 native backend is not healthy.");
            }

            switch (resolvedProfile)
            {
                case SmallBaseline:
                    await CreateAppendOnlyTableAsync(client, tablePath, DefaultSchema, rowCount: 128, batchSize: 64, partitionBy: null).ConfigureAwait(false);
                    break;
                case ManyFiles:
                    await CreateAppendOnlyTableAsync(client, tablePath, DefaultSchema, rowCount: 48, batchSize: 1, partitionBy: null).ConfigureAwait(false);
                    break;
                case WideSchema:
                    await CreateAppendOnlyTableAsync(client, tablePath, BuildWideSchema(columnCount: 100), rowCount: 64, batchSize: 32, partitionBy: null).ConfigureAwait(false);
                    break;
                case Partitioned:
                    await CreateAppendOnlyTableAsync(client, tablePath, DefaultSchema, rowCount: 240, batchSize: 24, partitionBy: new[] { "region" }).ConfigureAwait(false);
                    break;
                case CdfEnabled:
                    await CreateCdfTableAsync(client, tablePath).ConfigureAwait(false);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(profile), profile, "Unsupported V3 benchmark dataset profile.");
            }

            return tablePath;
        }

        private static async Task CreateAppendOnlyTableAsync(
            DeltaTableServiceClient client,
            string tablePath,
            TableSchema schema,
            int rowCount,
            int batchSize,
            IReadOnlyList<string>? partitionBy)
        {
            ExecuteResult createResult = await client.CreateTableAsync(tablePath, schema, partitionBy: partitionBy).ConfigureAwait(false);
            if (!createResult.Success)
            {
                throw new InvalidOperationException($"CreateTableAsync failed for '{tablePath}': {createResult.Message}");
            }

            await AppendRowsAsync(client, tablePath, schema, firstId: 1, rowCount, batchSize).ConfigureAwait(false);
        }

        private static async Task CreateCdfTableAsync(DeltaTableServiceClient client, string tablePath)
        {
            ExecuteResult createResult = await client.CreateTableAsync(
                tablePath,
                DefaultSchema,
                configuration: new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["delta.enableChangeDataFeed"] = "true",
                }).ConfigureAwait(false);
            if (!createResult.Success)
            {
                throw new InvalidOperationException($"CreateTableAsync failed for CDF dataset: {createResult.Message}");
            }

            ExecuteResult upgradeResult = await client.UpgradeTableProtocolAsync(
                tablePath,
                readerVersion: 3,
                writerVersion: 7,
                writerFeatures: new[] { "changeDataFeed" }).ConfigureAwait(false);
            if (!upgradeResult.Success)
            {
                throw new InvalidOperationException($"UpgradeTableProtocolAsync failed for CDF dataset: {upgradeResult.Message}");
            }

            await AppendRowsAsync(client, tablePath, DefaultSchema, firstId: 1, rowCount: 64, batchSize: 32).ConfigureAwait(false);

            ExecuteResult updateResult = await client.UpdateAsync(
                $"UPDATE {TableName} SET amount = amount + 10, note = 'updated' WHERE id % 5 = 0",
                tablePath,
                TableName).ConfigureAwait(false);
            if (!updateResult.Success)
            {
                throw new InvalidOperationException($"UpdateAsync failed for CDF dataset: {updateResult.Message}");
            }

            ExecuteResult deleteResult = await client.DeleteAsync(
                $"DELETE FROM {TableName} WHERE id % 11 = 0",
                tablePath,
                TableName).ConfigureAwait(false);
            if (!deleteResult.Success)
            {
                throw new InvalidOperationException($"DeleteAsync failed for CDF dataset: {deleteResult.Message}");
            }

            await AppendRowsAsync(client, tablePath, DefaultSchema, firstId: 65, rowCount: 16, batchSize: 16).ConfigureAwait(false);
        }

        private static async Task AppendRowsAsync(DeltaTableServiceClient client, string tablePath, TableSchema schema, long firstId, int rowCount, int batchSize)
        {
            int remaining = rowCount;
            long nextId = firstId;

            while (remaining > 0)
            {
                int currentBatchSize = Math.Min(batchSize, remaining);
                RecordBatch batch = ArrowConverter.FromRows(BuildRows(schema, nextId, currentBatchSize), schema);
                await client.InsertAsync(
                    tablePath,
                    batch.Schema,
                    ArrowConverter.ToAsyncEnumerable(batch),
                    SaveMode.Append).ConfigureAwait(false);

                nextId += currentBatchSize;
                remaining -= currentBatchSize;
            }
        }

        private static TableSchema BuildWideSchema(int columnCount)
        {
            var columns = new List<ColumnDefinition>(columnCount + 1)
            {
                new("id", "long", nullable: false),
            };

            for (int column = 0; column < columnCount; column++)
            {
                columns.Add(new ColumnDefinition($"metric_{column.ToString("D3", CultureInfo.InvariantCulture)}", "int32", nullable: false));
            }

            return new TableSchema(columns);
        }

        private static object[][] BuildRows(TableSchema schema, long firstId, int rowCount)
        {
            var rows = new object[rowCount][];

            for (int row = 0; row < rowCount; row++)
            {
                long id = firstId + row;
                var values = new object[schema.Columns.Count];

                for (int column = 0; column < schema.Columns.Count; column++)
                {
                    ColumnDefinition definition = schema.Columns[column];
                    values[column] = definition.Name switch
                    {
                        "id" => id,
                        "region" => RegionFor(id),
                        "category" => CategoryFor(id),
                        "amount" => 10 + (int)((id * 17) % 10_000),
                        "quantity" => 1 + (int)(id % 25),
                        "is_active" => id % 4 != 0,
                        "note" => $"row-{id}",
                        _ when definition.DataType.Equals("int32", StringComparison.OrdinalIgnoreCase) => (int)((id + column) % 10_000),
                        _ when definition.DataType.Equals("long", StringComparison.OrdinalIgnoreCase) => id + column,
                        _ when definition.DataType.Equals("string", StringComparison.OrdinalIgnoreCase) => $"value-{id}-{column}",
                        _ => throw new NotSupportedException($"Unsupported benchmark column '{definition.Name}' with type '{definition.DataType}'."),
                    };
                }

                rows[row] = values;
            }

            return rows;
        }

        private static string RegionFor(long id)
        {
            string[] regions = { "US", "EU", "APAC", "LATAM" };
            return regions[(int)(id % regions.Length)];
        }

        private static string CategoryFor(long id)
        {
            string[] categories = { "A", "B", "C", "D", "E" };
            return categories[(int)((id / 3) % categories.Length)];
        }
    }
}