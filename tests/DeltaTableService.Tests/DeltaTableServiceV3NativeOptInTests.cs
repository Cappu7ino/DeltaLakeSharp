// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Microsoft.ADMS.Testing.DeltaTableService.Client;
using Microsoft.ADMS.Testing.DeltaTableService.Client.Internal;
using Microsoft.ADMS.Testing.DeltaTableService.Client.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.ADMS.Testing.DeltaTableService.Tests
{
    /// <summary>
    /// Exercises the public client with <see cref="ServiceMode.V3_Rust"/> while
    /// opting into the new native backend on a per-test basis.
    ///
    /// This lets us validate parity slices through the existing client surface
    /// without flipping all V3 callers away from the legacy Flight/process path.
    /// </summary>
    [TestClass]
    [TestCategory("Integration")]
    [TestCategory("V3")]
    [TestCategory("V3Native")]
    public class DeltaTableServiceV3NativeOptInTests
    {
        private static readonly Uri DummyServerUri = new("http://localhost:1");

        private static string CreateTempTablePath(string prefix)
        {
            string tablePath = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}");
            Directory.CreateDirectory(tablePath);
            return tablePath;
        }

        private static void CleanupTablePath(string tablePath)
        {
            if (Directory.Exists(tablePath))
            {
                Directory.Delete(tablePath, recursive: true);
            }
        }

        [TestMethod]
        public async Task V3_NativeOptIn_HealthCheck_UsesNativeBackend()
        {
            using IDisposable nativeScope = V3BackendSelection.PushOverride(useNativeBackend: true);
            using var client = new DeltaTableServiceClient(DummyServerUri, ServiceMode.V3_Rust);

            bool healthy = await client.HealthCheckAsync();

            Assert.AreEqual(ServiceMode.V3_Rust, client.Mode);
            Assert.IsTrue(healthy, "Expected V3 native opt-in health check to succeed without a Flight server.");
        }

        [TestMethod]
        public async Task V3_NativeOptIn_CreateInsertReadAndSchema_RoundTrip()
        {
            string tablePath = CreateTempTablePath("v3_native_optin_roundtrip");
            using IDisposable nativeScope = V3BackendSelection.PushOverride(useNativeBackend: true);
            using var client = new DeltaTableServiceClient(DummyServerUri, ServiceMode.V3_Rust);

            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("name", "string"),
                });

                ExecuteResult createResult = await client.CreateTableAsync(tablePath, tableSchema);
                Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

                RecordBatch batch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 1, "Alice" },
                    new object[] { 2, "Bob" },
                    new object[] { 3, "Charlie" },
                }, tableSchema);

                await client.InsertAsync(
                    tablePath,
                    batch.Schema,
                    ArrowConverter.ToAsyncEnumerable(batch),
                    SaveMode.Append);

                TableSchema readSchema = await client.GetSchemaAsync(tablePath);
                Assert.AreEqual(2, readSchema.Columns.Count, "Expected 2 schema columns.");
                Assert.AreEqual("id", readSchema.Columns[0].Name);
                Assert.AreEqual("name", readSchema.Columns[1].Name);

                var rowsByName = new Dictionary<string, int>();
                await foreach (RecordBatch readBatch in client.ReadTableAsync(tablePath))
                {
                    var idArray = (Int32Array)readBatch.Column(0);
                    for (int i = 0; i < readBatch.Length; i++)
                    {
                        string name = V3TestHelpers.ReadStringValue(readBatch.Column(1), i);

                        rowsByName[name] = idArray.GetValue(i) ?? -1;
                    }
                }

                Assert.AreEqual(3, rowsByName.Count, "Expected 3 rows after native V3 insert.");
                Assert.AreEqual(1, rowsByName["Alice"]);
                Assert.AreEqual(2, rowsByName["Bob"]);
                Assert.AreEqual(3, rowsByName["Charlie"]);
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task V3_NativeOptIn_ExecuteQuery_ReturnsFilteredRows()
        {
            string tablePath = CreateTempTablePath("v3_native_optin_query");
            using IDisposable nativeScope = V3BackendSelection.PushOverride(useNativeBackend: true);
            using var client = new DeltaTableServiceClient(DummyServerUri, ServiceMode.V3_Rust);

            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("name", "string"),
                });

                ExecuteResult createResult = await client.CreateTableAsync(tablePath, tableSchema);
                Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

                RecordBatch batch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 1, "Alice" },
                    new object[] { 2, "Bob" },
                    new object[] { 3, "Charlie" },
                }, tableSchema);

                await client.InsertAsync(
                    tablePath,
                    batch.Schema,
                    ArrowConverter.ToAsyncEnumerable(batch),
                    SaveMode.Append);

                var names = new List<string>();
                await foreach (RecordBatch resultBatch in client.ExecuteQueryAsync(
                    sql: "SELECT name FROM tbl WHERE id >= 2 ORDER BY id",
                    tablePath: tablePath,
                    tableName: "tbl"))
                {
                    for (int i = 0; i < resultBatch.Length; i++)
                    {
                        string name = V3TestHelpers.ReadStringValue(resultBatch.Column(0), i);
                        names.Add(name);
                    }
                }

                CollectionAssert.AreEqual(new[] { "Bob", "Charlie" }, names);
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task V3_NativeOptIn_MergeAsync_ThrowsNotSupported()
        {
            using IDisposable nativeScope = V3BackendSelection.PushOverride(useNativeBackend: true);
            using var client = new DeltaTableServiceClient(DummyServerUri, ServiceMode.V3_Rust);

            var ex = await Assert.ThrowsExceptionAsync<NotSupportedException>(() =>
                client.MergeAsync(
                    "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.name = source.name",
                    "dummy-path",
                    "target"));

            StringAssert.Contains(ex.Message, "MergeDataAsync");
        }

        [TestMethod]
        public async Task V3_NativeOptIn_DeleteAndUpdate_RoundTrip()
        {
            string tablePath = CreateTempTablePath("v3_native_optin_dml");
            using IDisposable nativeScope = V3BackendSelection.PushOverride(useNativeBackend: true);
            using var client = new DeltaTableServiceClient(DummyServerUri, ServiceMode.V3_Rust);

            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("name", "string"),
                });

                await client.CreateTableAsync(tablePath, tableSchema);

                RecordBatch batch = V3TestHelpers.BuildIdNameBatch(
                    new[] { 1, 2, 3 },
                    new[] { "a", "b", "c" });

                await client.InsertAsync(
                    tablePath,
                    batch.Schema,
                    V3TestHelpers.ToAsyncEnumerable(batch),
                    SaveMode.Overwrite);

                ExecuteResult updateResult = await client.UpdateAsync(
                    "UPDATE tbl SET name = 'updated' WHERE id = 2",
                    tablePath,
                    "tbl");
                Assert.IsTrue(updateResult.Success, $"UpdateAsync failed: {updateResult.Message}");

                ExecuteResult deleteResult = await client.DeleteAsync(
                    "DELETE FROM tbl WHERE id = 3",
                    tablePath,
                    "tbl");
                Assert.IsTrue(deleteResult.Success, $"DeleteAsync failed: {deleteResult.Message}");
                Assert.IsTrue(deleteResult.Result.Count > 0, "Expected delete metrics in result.");
                Assert.IsTrue(deleteResult.Result[0].ContainsKey("num_deleted_rows"));

                var rows = await V3TestHelpers.ReadAllRowsSorted(client, tablePath);
                Assert.AreEqual(2, rows.Count);
                Assert.AreEqual((1, "a"), rows[0]);
                Assert.AreEqual((2, "updated"), rows[1]);
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task V3_NativeOptIn_MergeData_UpsertAndMetrics()
        {
            string tablePath = CreateTempTablePath("v3_native_optin_merge");
            using IDisposable nativeScope = V3BackendSelection.PushOverride(useNativeBackend: true);
            using var client = new DeltaTableServiceClient(DummyServerUri, ServiceMode.V3_Rust);

            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("name", "string"),
                });

                await client.CreateTableAsync(tablePath, tableSchema);

                Schema arrowSchema = V3TestHelpers.BuildIdNameSchema();
                RecordBatch initialBatch = V3TestHelpers.BuildIdNameBatch(
                    new[] { 1, 2, 3 },
                    new[] { "a", "b", "c" });
                await client.InsertAsync(
                    tablePath,
                    arrowSchema,
                    V3TestHelpers.ToAsyncEnumerable(initialBatch),
                    SaveMode.Overwrite);

                RecordBatch sourceBatch = V3TestHelpers.BuildIdNameBatch(
                    new[] { 2, 4 },
                    new[] { "updated_b", "d" });

                var mergeOptions = new MergeOptions("target.id = source.id")
                {
                    WhenMatchedUpdateAll = true,
                    WhenNotMatchedInsertAll = true,
                };

                ExecuteResult mergeResult = await client.MergeDataAsync(
                    tablePath,
                    arrowSchema,
                    V3TestHelpers.ToAsyncEnumerable(sourceBatch),
                    mergeOptions);

                Assert.IsTrue(mergeResult.Success, $"MergeDataAsync failed: {mergeResult.Message}");
                Assert.IsTrue(mergeResult.Result.Count > 0, "Expected merge metrics in result.");
                Assert.IsTrue(mergeResult.Result[0].ContainsKey("num_source_rows"));
                Assert.IsTrue(mergeResult.Result[0].ContainsKey("num_target_rows_inserted"));
                Assert.IsTrue(mergeResult.Result[0].ContainsKey("num_target_rows_updated"));

                var rows = await V3TestHelpers.ReadAllRowsSorted(client, tablePath);
                Assert.AreEqual(4, rows.Count);
                Assert.AreEqual((1, "a"), rows[0]);
                Assert.AreEqual((2, "updated_b"), rows[1]);
                Assert.AreEqual((3, "c"), rows[2]);
                Assert.AreEqual((4, "d"), rows[3]);
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task V3_NativeOptIn_UpgradeProtocol_ReturnsExpectedVersions()
        {
            string tablePath = CreateTempTablePath("v3_native_optin_protocol");
            using IDisposable nativeScope = V3BackendSelection.PushOverride(useNativeBackend: true);
            using var client = new DeltaTableServiceClient(DummyServerUri, ServiceMode.V3_Rust);

            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                });

                await client.CreateTableAsync(tablePath, tableSchema);

                ExecuteResult result = await client.UpgradeTableProtocolAsync(
                    tablePath,
                    readerVersion: 3,
                    writerVersion: 7,
                    writerFeatures: new[] { "changeDataFeed" });

                Assert.IsTrue(result.Success, $"UpgradeTableProtocolAsync failed: {result.Message}");
                Assert.IsTrue(result.Result.Count > 0, "Expected protocol result payload.");
                Assert.IsTrue(result.Result[0].ContainsKey("minReaderVersion"));
                Assert.IsTrue(result.Result[0].ContainsKey("minWriterVersion"));
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }
    }
}
