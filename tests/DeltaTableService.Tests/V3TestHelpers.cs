// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Types;
using Apache.Arrow.Arrays;
using Microsoft.DI.DeltaTableService.Client;
using Microsoft.DI.DeltaTableService.Client.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.DI.DeltaTableService.Tests
{
    internal static class V3TestHelpers
    {
        internal sealed class IntegrationContext : IDisposable
        {
            private readonly List<string> _trackedTablePaths = new();
            private readonly string _tempDir;

            internal IntegrationContext(
                DeltaTableServiceClient client,
                string tempDir,
                string testTablePath,
                string partitionedTablePath,
                string timeTravelTablePath,
                string? fixtureDataDir)
            {
                Client = client;
                _tempDir = tempDir;
                TestTablePath = testTablePath;
                PartitionedTablePath = partitionedTablePath;
                TimeTravelTablePath = timeTravelTablePath;
                FixtureDataDir = fixtureDataDir;
            }

            internal DeltaTableServiceClient Client { get; }

            internal string TestTablePath { get; }

            internal string PartitionedTablePath { get; }

            internal string TimeTravelTablePath { get; }

            internal string? FixtureDataDir { get; }

            internal string CreateWriteTestTablePath()
            {
                string tablePath = Path.Combine(_tempDir, $"write_test_{Guid.NewGuid():N}");
                _trackedTablePaths.Add(tablePath);
                return tablePath;
            }

            public void Dispose()
            {
                Client.Dispose();

                foreach (string tablePath in _trackedTablePaths)
                {
                    try { CleanupTablePath(tablePath); }
                    catch { }
                }

                if (Directory.Exists(_tempDir))
                {
                    try { Directory.Delete(_tempDir, recursive: true); }
                    catch { }
                }
            }
        }

        internal static readonly Uri DummyServerUri = new("http://localhost:1");

        internal static async Task<IntegrationContext> CreateIntegrationContextAsync()
        {
            string? binaryPath = FindRustFixtureBinary();
            if (binaryPath == null)
            {
                Assert.Inconclusive(
                    "Rust binary not found. Build it first: " +
                    "cd src/DeltaTableService.Server/v3 && cargo build");
            }

            string tempDir = Path.Combine(Path.GetTempPath(), $"v3_test_{Guid.NewGuid():N}");
            string? fixtureDataDir = FindFixtureDataDir();

            string testTablePath = Path.Combine(tempDir, "test_table");
            CreateTestDeltaTable(binaryPath!, testTablePath);

            string partitionedTablePath = Path.Combine(tempDir, "partitioned_table");
            CreateTestDeltaTable(binaryPath!, partitionedTablePath, "partitioned");

            string timeTravelTablePath = Path.Combine(tempDir, "time_travel_table");
            CreateTestDeltaTable(binaryPath!, timeTravelTablePath, "time-travel");

            var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
            bool healthy = await client.HealthCheckAsync();
            Assert.IsTrue(healthy, "Delta Table Service V3 did not become healthy.");

            return new IntegrationContext(
                client,
                tempDir,
                testTablePath,
                partitionedTablePath,
                timeTravelTablePath,
                fixtureDataDir);
        }

        internal static string? FindRustFixtureBinary()
        {
            string? dir = AppContext.BaseDirectory;
            while (dir != null)
            {
                string solutionFile = Path.Combine(dir, "DeltaTableService.sln");
                if (File.Exists(solutionFile))
                {
                    string binaryPath = Path.Combine(
                        dir,
                        "src",
                        "DeltaTableService.Server",
                        "v3",
                        "target",
                        "debug",
                        "delta-table-service-v3-fixture.exe");
                    return File.Exists(binaryPath) ? binaryPath : null;
                }

                dir = Path.GetDirectoryName(dir);
            }

            return null;
        }

        internal static string? FindFixtureDataDir()
        {
            string? dir = AppContext.BaseDirectory;
            while (dir != null)
            {
                string solutionFile = Path.Combine(dir, "DeltaTableService.sln");
                if (File.Exists(solutionFile))
                {
                    string dataDir = Path.Combine(dir, "tests", "DeltaTableService.Tests", "data");
                    return Directory.Exists(dataDir) ? dataDir : null;
                }

                dir = Path.GetDirectoryName(dir);
            }

            return null;
        }

        internal static string CreateTempTablePath(string prefix)
        {
            string tablePath = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}");
            Directory.CreateDirectory(tablePath);
            return tablePath;
        }

        internal static void CleanupTablePath(string tablePath)
        {
            if (Directory.Exists(tablePath))
            {
                Directory.Delete(tablePath, recursive: true);
            }
        }

        internal static void CreateTestDeltaTable(string binaryPath, string tablePath, string fixtureType = "basic")
        {
            var psi = new System.Diagnostics.ProcessStartInfo
            {
                FileName = binaryPath,
                Arguments = $"create \"{tablePath}\" --fixture-type {fixtureType}",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            using var proc = System.Diagnostics.Process.Start(psi)
                ?? throw new InvalidOperationException("Failed to start create-test-fixture process.");

            string stdout = proc.StandardOutput.ReadToEnd();
            string stderr = proc.StandardError.ReadToEnd();
            proc.WaitForExit(30_000);

            if (proc.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"create-test-fixture failed (exit code {proc.ExitCode}).\nstdout: {stdout}\nstderr: {stderr}");
            }

            if (!stdout.Contains("TEST_FIXTURE_CREATED", StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"create-test-fixture did not print expected sentinel.\nstdout: {stdout}\nstderr: {stderr}");
            }
        }

        internal static Schema BuildIdNameSchema()
        {
            return new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: true))
                .Field(new Field("name", StringType.Default, nullable: true))
                .Build();
        }

        internal static RecordBatch BuildIdNameBatch(int[] ids, string[] names)
        {
            return new RecordBatch.Builder()
                .Append("id", nullable: true, new Int32Array.Builder().AppendRange(ids).Build())
                .Append("name", nullable: true, new StringArray.Builder().AppendRange(names).Build())
                .Build();
        }

        internal static Schema BuildIdCityActiveSchema()
        {
            return new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: true))
                .Field(new Field("city", StringType.Default, nullable: true))
                .Field(new Field("active", BooleanType.Default, nullable: true))
                .Build();
        }

        internal static RecordBatch BuildIdCityActiveBatch(int[] ids, string[] cities, bool[] active)
        {
            return new RecordBatch.Builder()
                .Append("id", nullable: true, new Int32Array.Builder().AppendRange(ids).Build())
                .Append("city", nullable: true, new StringArray.Builder().AppendRange(cities).Build())
                .Append("active", nullable: true, new BooleanArray.Builder().AppendRange(active).Build())
                .Build();
        }

        internal static Schema BuildIdRegionNameSchema()
        {
            return new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: true))
                .Field(new Field("region", StringType.Default, nullable: true))
                .Field(new Field("name", StringType.Default, nullable: true))
                .Build();
        }

        internal static RecordBatch BuildIdRegionNameBatch(int[] ids, string[] regions, string[] names)
        {
            return new RecordBatch.Builder()
                .Append("id", nullable: true, new Int32Array.Builder().AppendRange(ids).Build())
                .Append("region", nullable: true, new StringArray.Builder().AppendRange(regions).Build())
                .Append("name", nullable: true, new StringArray.Builder().AppendRange(names).Build())
                .Build();
        }

        internal static Schema BuildIdAmountNameSchema()
        {
            return new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: true))
                .Field(new Field("amount", Int32Type.Default, nullable: true))
                .Field(new Field("name", StringType.Default, nullable: true))
                .Build();
        }

        internal static RecordBatch BuildIdAmountNameBatch(int[] ids, int[] amounts, string[] names)
        {
            return new RecordBatch.Builder()
                .Append("id", nullable: true, new Int32Array.Builder().AppendRange(ids).Build())
                .Append("amount", nullable: true, new Int32Array.Builder().AppendRange(amounts).Build())
                .Append("name", nullable: true, new StringArray.Builder().AppendRange(names).Build())
                .Build();
        }

        internal static async IAsyncEnumerable<RecordBatch> ToAsyncEnumerable(RecordBatch batch)
        {
            yield return batch;
            await Task.CompletedTask;
        }

        internal static string ReadStringValue(IArrowArray array, int index)
        {
            return array switch
            {
                StringArray sa => sa.GetString(index),
                StringViewArray sva => sva.GetString(index),
                LargeStringArray lsa => lsa.GetString(index),
                DictionaryArray da => ReadDictionaryStringValue(da, index),
                _ => throw new AssertFailedException(
                    $"Unexpected string column type: {array.GetType().FullName}")
            } ?? string.Empty;
        }

        private static string? ReadDictionaryStringValue(DictionaryArray array, int index)
        {
            IArrowArray dictionary = array.Dictionary;
            int dictionaryIndex = Convert.ToInt32(ReadValue(array.Indices, index));
            return dictionary switch
            {
                StringArray sa => sa.GetString(dictionaryIndex),
                StringViewArray sva => sva.GetString(dictionaryIndex),
                LargeStringArray lsa => lsa.GetString(dictionaryIndex),
                _ => throw new AssertFailedException(
                    $"Unexpected dictionary value type: {dictionary.GetType().FullName}")
            };
        }

        internal static async Task<List<(int id, string? name)>> ReadAllRowsSorted(
            DeltaTableServiceClient client,
            string tablePath)
        {
            var ids = new List<int>();
            var names = new List<string?>();

            await foreach (RecordBatch batch in client.ReadTableAsync(tablePath))
            {
                var idArray = (Int32Array)batch.Column(0);
                IArrowArray nameCol = batch.Column(1);

                for (int i = 0; i < batch.Length; i++)
                {
                    ids.Add(idArray.GetValue(i)!.Value);
                    names.Add(ReadStringValue(nameCol, i));
                }
            }

            return ids.Zip(names, (id, name) => (id, name))
                .OrderBy(x => x.id)
                .ToList();
        }

        internal static void AssertNativeFailure(Exception ex)
        {
            Assert.IsTrue(
                ex.Message.Contains("Native V3 backend operation", StringComparison.Ordinal)
                || ex.Message.Contains("Native error:", StringComparison.Ordinal),
                $"Expected native backend failure details, got: {ex.Message}");
        }

        internal static void AssertExecuteResultContainsLong(
            ExecuteResult result,
            string key,
            long minimumValue = long.MinValue)
        {
            Assert.IsTrue(result.Result.Count > 0, "Expected result payload.");
            Assert.IsTrue(result.Result[0].ContainsKey(key), $"Expected '{key}' in result payload.");
            object rawValue = result.Result[0][key];
            Assert.IsTrue(rawValue is long, $"Expected '{key}' to be parsed as a long.");
            long value = (long)rawValue;
            Assert.IsTrue(value >= minimumValue, $"Expected '{key}' >= {minimumValue}, got {value}.");
        }

        internal static async Task<List<Dictionary<string, object?>>> ReadAllChangeDataRowsAsync(
            DeltaTableServiceClient client,
            string tablePath,
            long startingVersion,
            long? endingVersion = null)
        {
            var rows = new List<Dictionary<string, object?>>();

            await foreach (RecordBatch batch in client.ReadChangeDataAsync(tablePath, startingVersion, endingVersion))
            {
                for (int rowIndex = 0; rowIndex < batch.Length; rowIndex++)
                {
                    var row = new Dictionary<string, object?>();
                    for (int colIndex = 0; colIndex < batch.ColumnCount; colIndex++)
                    {
                        string columnName = batch.Schema.GetFieldByIndex(colIndex).Name;
                        row[columnName] = ReadValue(batch.Column(colIndex), rowIndex);
                    }

                    rows.Add(row);
                }
            }

            return rows;
        }

        internal static async Task<List<Dictionary<string, object?>>> ExecuteChangeDataQueryRowsAsync(
            DeltaTableServiceClient client,
            string sql,
            string tablePath,
            long startingVersion,
            long? endingVersion = null)
        {
            var rows = new List<Dictionary<string, object?>>();

            await foreach (RecordBatch batch in client.ExecuteChangeDataQueryAsync(sql, tablePath, startingVersion, endingVersion))
            {
                for (int rowIndex = 0; rowIndex < batch.Length; rowIndex++)
                {
                    var row = new Dictionary<string, object?>();
                    for (int colIndex = 0; colIndex < batch.ColumnCount; colIndex++)
                    {
                        string columnName = batch.Schema.GetFieldByIndex(colIndex).Name;
                        row[columnName] = ReadValue(batch.Column(colIndex), rowIndex);
                    }

                    rows.Add(row);
                }
            }

            return rows;
        }

        internal static object? ReadValue(IArrowArray array, int index)
        {
            return array switch
            {
                UInt8Array a => a.GetValue(index),
                UInt16Array a => a.GetValue(index),
                UInt32Array a => a.GetValue(index),
                UInt64Array a => a.GetValue(index),
                Int32Array a => a.GetValue(index),
                Int64Array a => a.GetValue(index),
                BooleanArray a => a.GetValue(index),
                StringArray a => a.GetString(index),
                StringViewArray a => a.GetString(index),
                LargeStringArray a => a.GetString(index),
                BinaryArray a => a.GetBytes(index).ToArray(),
                BinaryViewArray a => a.GetBytes(index).ToArray(),
                LargeBinaryArray a => a.GetBytes(index).ToArray(),
                TimestampArray a => a.GetTimestamp(index),
                _ => throw new AssertFailedException(
                    $"Unexpected array type: {array.GetType().FullName}")
            };
        }
    }
}
