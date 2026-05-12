// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using DeltaLakeSharp.Client;
using DeltaLakeSharp.Client.Models;

namespace DeltaLakeSharp.Client.Examples
{
    internal static class Program
    {
        public static async Task Main(string[] args)
        {
            string tablePath = args.Length > 0
                ? args[0]
                : Path.Combine(Path.GetTempPath(), $"delta_client_example_{Guid.NewGuid():N}");
            bool deleteWhenDone = args.Length == 0;

            try
            {
                using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);

                await CreatePeopleTableAsync(client, tablePath);
                await StreamArrowBatchesAsync(client, tablePath);
                await ReadWithDataReaderAsync(client, tablePath);
                await QueryWithSqlAsync(client, tablePath);
            }
            finally
            {
                if (deleteWhenDone && Directory.Exists(tablePath))
                {
                    Directory.Delete(tablePath, recursive: true);
                }
            }
        }

        private static async Task CreatePeopleTableAsync(DeltaTableServiceClient client, string tablePath)
        {
            if (Directory.Exists(tablePath))
            {
                Directory.Delete(tablePath, recursive: true);
            }

            var tableSchema = new TableSchema(new[]
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
                new ColumnDefinition("active", "boolean"),
            });

            ExecuteResult createResult = await client.CreateTableAsync(tablePath, tableSchema);
            EnsureSuccess(createResult, "create table");

            RecordBatch batch = ArrowConverter.FromRows(new[]
            {
                new object[] { 1, "Ada", true },
                new object[] { 2, "Grace", true },
                new object[] { 3, "Katherine", false },
            }, tableSchema);

            await client.InsertAsync(
                tablePath,
                batch.Schema,
                ToAsyncEnumerable(batch),
                SaveMode.Append);
        }

        private static async Task StreamArrowBatchesAsync(DeltaTableServiceClient client, string tablePath)
        {
            await foreach (RecordBatch batch in client.ReadTableAsync(tablePath, batchSize: 2))
            {
                IReadOnlyList<string> names = ReadStringColumn(batch, columnIndex: 1);
                Console.WriteLine($"Read Arrow batch with {batch.Length} rows: {string.Join(", ", names)}");
            }
        }

        private static async Task ReadWithDataReaderAsync(DeltaTableServiceClient client, string tablePath)
        {
            using DbDataReader reader = await client.ReadTableAsDataReaderAsync(tablePath);
            while (reader.Read())
            {
                Console.WriteLine($"DbDataReader row: {reader.GetInt32(0)} {reader.GetString(1)} active={reader.GetBoolean(2)}");
            }
        }

        private static async Task QueryWithSqlAsync(DeltaTableServiceClient client, string tablePath)
        {
            await foreach (RecordBatch batch in client.ExecuteQueryAsync(
                "SELECT id, name FROM people WHERE active = true ORDER BY id",
                tablePath,
                "people"))
            {
                IReadOnlyList<string> names = ReadStringColumn(batch, columnIndex: 1);
                Console.WriteLine($"SQL query returned: {string.Join(", ", names)}");
            }
        }

        private static async IAsyncEnumerable<RecordBatch> ToAsyncEnumerable(RecordBatch batch)
        {
            yield return batch;
            await Task.CompletedTask;
        }

        private static IReadOnlyList<string> ReadStringColumn(RecordBatch batch, int columnIndex)
        {
            IArrowArray column = batch.Column(columnIndex);
            var values = new List<string>(batch.Length);

            for (int rowIndex = 0; rowIndex < batch.Length; rowIndex++)
            {
                values.Add(column switch
                {
                    StringArray array => array.GetString(rowIndex) ?? string.Empty,
                    StringViewArray array => array.GetString(rowIndex) ?? string.Empty,
                    LargeStringArray array => array.GetString(rowIndex) ?? string.Empty,
                    _ => Convert.ToString(column.GetType().Name) ?? string.Empty,
                });
            }

            return values;
        }

        private static void EnsureSuccess(ExecuteResult result, string operation)
        {
            if (!result.Success)
            {
                throw new InvalidOperationException($"Failed to {operation}: {result.Message}");
            }
        }
    }
}
