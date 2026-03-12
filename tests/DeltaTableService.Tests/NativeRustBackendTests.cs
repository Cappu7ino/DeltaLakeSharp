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
    /// Focused tests for the in-process native Rust backend scaffold.
    ///
    /// These tests validate the new transport directly, without depending on the
    /// legacy Flight/process path or flipping <see cref="ServiceMode.V3_Rust"/>
    /// globally before native parity is proven.
    /// </summary>
    [TestClass]
    public class NativeRustBackendTests
    {
        /// <summary>
        /// Creates a unique local directory for a native Delta table test.
        /// Each test gets its own isolated folder so cleanup can safely remove
        /// the entire directory tree at the end of the test.
        /// </summary>
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
        public async Task NativeBackend_HealthCheck_ReturnsTrue()
        {
            using var backend = new NativeRustBackend();

            bool healthy = await backend.HealthCheckAsync();

            Assert.IsTrue(healthy, "Expected native backend health check to succeed.");
        }

        [TestMethod]
        public async Task NativeBackend_CreateTable_Insert_ReadAndSchema_RoundTrip()
        {
            string tablePath = CreateTempTablePath("native_v3");
            var backend = new NativeRustBackend();
            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("name", "string"),
                });

                ExecuteResult createResult = await backend.CreateEmptyTableAsync(tablePath, tableSchema);
                Assert.IsTrue(createResult.Success, $"CreateEmptyTableAsync failed: {createResult.Message}");

                TableSchema readSchema = await backend.GetSchemaAsync(tablePath);
                Assert.AreEqual(2, readSchema.Columns.Count, "Expected 2 schema columns.");
                Assert.AreEqual("id", readSchema.Columns[0].Name);
                Assert.AreEqual("name", readSchema.Columns[1].Name);

                var rows = new[]
                {
                    new object[] { 1, "Alice" },
                    new object[] { 2, "Bob" },
                    new object[] { 3, "Charlie" },
                };
                RecordBatch batch = ArrowConverter.FromRows(rows, tableSchema);
                await backend.InsertAsync(
                    tablePath,
                    batch.Schema,
                    ArrowConverter.ToAsyncEnumerable(batch),
                    mode: "append");

                var readBatches = new List<RecordBatch>();
                await foreach (RecordBatch readBatch in backend.ReadTableAsync(tablePath))
                {
                    readBatches.Add(readBatch);
                }

                int totalRows = readBatches.Sum(b => b.Length);
                Assert.AreEqual(3, totalRows, "Expected 3 rows after native insert.");
                Assert.IsTrue(readBatches.Count > 0, "Expected at least one returned batch.");
                Assert.AreEqual(2, readBatches[0].ColumnCount, "Expected 2 columns after read back.");

                var rowsByName = new Dictionary<string, int>();
                foreach (RecordBatch readBatch in readBatches)
                {
                    var idArray = (Int32Array)readBatch.Column(0);
                    for (int i = 0; i < readBatch.Length; i++)
                    {
                        string name = readBatch.Column(1) switch
                        {
                            StringArray sa => sa.GetString(i),
                            StringViewArray sva => sva.GetString(i),
                            LargeStringArray lsa => lsa.GetString(i),
                            _ => throw new AssertFailedException(
                                $"Unexpected string column type: {readBatch.Column(1).GetType().FullName}")
                        } ?? string.Empty;

                        rowsByName[name] = idArray.GetValue(i) ?? -1;
                    }
                }

                string observedNames = string.Join(", ", rowsByName.Keys);
                Assert.IsTrue(rowsByName.ContainsKey("Alice"), $"Missing Alice. Observed names: {observedNames}");
                Assert.IsTrue(rowsByName.ContainsKey("Bob"), $"Missing Bob. Observed names: {observedNames}");
                Assert.IsTrue(rowsByName.ContainsKey("Charlie"), $"Missing Charlie. Observed names: {observedNames}");
                Assert.AreEqual(1, rowsByName["Alice"]);
                Assert.AreEqual(2, rowsByName["Bob"]);
                Assert.AreEqual(3, rowsByName["Charlie"]);
            }
            finally
            {
                backend.Dispose();
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_ExecuteQuery_ReturnsProjectedRows()
        {
            string tablePath = CreateTempTablePath("native_v3_query");
            var backend = new NativeRustBackend();
            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("name", "string"),
                });

                ExecuteResult createResult = await backend.CreateEmptyTableAsync(tablePath, tableSchema);
                Assert.IsTrue(createResult.Success, $"CreateEmptyTableAsync failed: {createResult.Message}");

                RecordBatch batch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 1, "Alice" },
                    new object[] { 2, "Bob" },
                    new object[] { 3, "Charlie" },
                }, tableSchema);
                await backend.InsertAsync(
                    tablePath,
                    batch.Schema,
                    ArrowConverter.ToAsyncEnumerable(batch),
                    mode: "append");

                var resultBatches = new List<RecordBatch>();
                await foreach (RecordBatch resultBatch in backend.ExecuteQueryAsync(
                    "SELECT name FROM tbl WHERE id >= 2",
                    tablePath,
                    "tbl"))
                {
                    resultBatches.Add(resultBatch);
                }

                Assert.AreEqual(2, resultBatches.Sum(b => b.Length), "Expected 2 filtered rows.");

                var names = new List<string>();
                foreach (RecordBatch resultBatch in resultBatches)
                {
                    for (int i = 0; i < resultBatch.Length; i++)
                    {
                        string name = resultBatch.Column(0) switch
                        {
                            StringArray sa => sa.GetString(i),
                            StringViewArray sva => sva.GetString(i),
                            LargeStringArray lsa => lsa.GetString(i),
                            _ => throw new AssertFailedException(
                                $"Unexpected query column type: {resultBatch.Column(0).GetType().FullName}")
                        } ?? string.Empty;
                        names.Add(name);
                    }
                }

                CollectionAssert.AreEqual(new[] { "Bob", "Charlie" }, names);
            }
            finally
            {
                backend.Dispose();
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_UpgradeProtocol_EnablesChangeDataFeed()
        {
            string tablePath = CreateTempTablePath("native_v3_upgrade");
            var backend = new NativeRustBackend();
            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                });

                ExecuteResult createResult = await backend.CreateEmptyTableAsync(tablePath, tableSchema);
                Assert.IsTrue(createResult.Success, $"CreateEmptyTableAsync failed: {createResult.Message}");

                ExecuteResult upgradeResult = await backend.UpgradeTableProtocolAsync(
                    tablePath,
                    readerVersion: 1,
                    writerVersion: 5,
                    writerFeatures: new[] { "changeDataFeed" });

                Assert.IsTrue(upgradeResult.Success, $"UpgradeTableProtocolAsync failed: {upgradeResult.Message}");
                Assert.IsNotNull(upgradeResult.Result, "Expected result payload from native upgrade protocol.");
                Assert.IsTrue(upgradeResult.Result.Count > 0, "Expected at least one result row.");

                string resultText = string.Join(" ", upgradeResult.Result[0].Values);
                Assert.IsTrue(
                    resultText.Contains("changeDataFeed", StringComparison.OrdinalIgnoreCase),
                    $"Expected protocol result to mention changeDataFeed. Actual: {resultText}");
            }
            finally
            {
                backend.Dispose();
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_DeleteAsync_RemovesMatchingRows()
        {
            string tablePath = CreateTempTablePath("native_v3_delete");
            var backend = new NativeRustBackend();
            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("value", "string"),
                });

                ExecuteResult createResult = await backend.CreateEmptyTableAsync(
                    tablePath,
                    tableSchema,
                    configuration: new Dictionary<string, string>
                    {
                        ["delta.enableDeletionVectors"] = "true",
                    });
                Assert.IsTrue(createResult.Success, $"CreateEmptyTableAsync failed: {createResult.Message}");

                RecordBatch batch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 1, "keep" },
                    new object[] { 2, "delete" },
                    new object[] { 3, "keep" },
                }, tableSchema);
                await backend.InsertAsync(
                    tablePath,
                    batch.Schema,
                    ArrowConverter.ToAsyncEnumerable(batch),
                    mode: "append");

                ExecuteResult deleteResult = await backend.DeleteAsync(
                    "DELETE FROM native_tbl WHERE id = 2",
                    tablePath,
                    "native_tbl");
                Assert.IsTrue(deleteResult.Success, $"DeleteAsync failed: {deleteResult.Message}");

                var readBatches = new List<RecordBatch>();
                await foreach (RecordBatch readBatch in backend.ReadTableAsync(tablePath))
                {
                    readBatches.Add(readBatch);
                }

                var ids = new List<int>();
                foreach (RecordBatch readBatch in readBatches)
                {
                    var idArray = (Int32Array)readBatch.Column(0);
                    for (int i = 0; i < readBatch.Length; i++)
                    {
                        ids.Add(idArray.GetValue(i) ?? -1);
                    }
                }

                CollectionAssert.AreEquivalent(new[] { 1, 3 }, ids);
            }
            finally
            {
                backend.Dispose();
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_UpdateAsync_ModifiesMatchingRows()
        {
            string tablePath = CreateTempTablePath("native_v3_update");
            var backend = new NativeRustBackend();
            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("status", "string"),
                });

                ExecuteResult createResult = await backend.CreateEmptyTableAsync(tablePath, tableSchema);
                Assert.IsTrue(createResult.Success, $"CreateEmptyTableAsync failed: {createResult.Message}");

                RecordBatch batch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 1, "active" },
                    new object[] { 2, "active" },
                    new object[] { 3, "inactive" },
                }, tableSchema);
                await backend.InsertAsync(
                    tablePath,
                    batch.Schema,
                    ArrowConverter.ToAsyncEnumerable(batch),
                    mode: "append");

                ExecuteResult updateResult = await backend.UpdateAsync(
                    "UPDATE native_tbl SET status = 'updated' WHERE id <= 2",
                    tablePath,
                    "native_tbl");
                Assert.IsTrue(updateResult.Success, $"UpdateAsync failed: {updateResult.Message}");

                var readBatches = new List<RecordBatch>();
                await foreach (RecordBatch readBatch in backend.ReadTableAsync(tablePath))
                {
                    readBatches.Add(readBatch);
                }

                var rowsById = new Dictionary<int, string>();
                foreach (RecordBatch readBatch in readBatches)
                {
                    var idArray = (Int32Array)readBatch.Column(0);
                    for (int i = 0; i < readBatch.Length; i++)
                    {
                        string status = readBatch.Column(1) switch
                        {
                            StringArray sa => sa.GetString(i),
                            StringViewArray sva => sva.GetString(i),
                            LargeStringArray lsa => lsa.GetString(i),
                            _ => throw new AssertFailedException(
                                $"Unexpected string column type: {readBatch.Column(1).GetType().FullName}")
                        } ?? string.Empty;

                        rowsById[idArray.GetValue(i) ?? -1] = status;
                    }
                }

                Assert.AreEqual("updated", rowsById[1]);
                Assert.AreEqual("updated", rowsById[2]);
                Assert.AreEqual("inactive", rowsById[3]);
            }
            finally
            {
                backend.Dispose();
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_MergeDataAsync_UpdatesAndInsertsRows()
        {
            string tablePath = CreateTempTablePath("native_v3_merge");
            var backend = new NativeRustBackend();
            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("name", "string"),
                });

                ExecuteResult createResult = await backend.CreateEmptyTableAsync(tablePath, tableSchema);
                Assert.IsTrue(createResult.Success, $"CreateEmptyTableAsync failed: {createResult.Message}");

                RecordBatch targetBatch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 1, "a" },
                    new object[] { 2, "b" },
                    new object[] { 3, "c" },
                }, tableSchema);
                await backend.InsertAsync(
                    tablePath,
                    targetBatch.Schema,
                    ArrowConverter.ToAsyncEnumerable(targetBatch),
                    mode: "append");

                RecordBatch mergeBatch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 2, "updated_b" },
                    new object[] { 4, "d" },
                }, tableSchema);

                var mergeOptions = new MergeOptions("target.id = source.id")
                {
                    WhenMatchedUpdateAll = true,
                    WhenNotMatchedInsertAll = true,
                };

                ExecuteResult mergeResult = await backend.MergeDataAsync(
                    tablePath,
                    mergeBatch.Schema,
                    ArrowConverter.ToAsyncEnumerable(mergeBatch),
                    mergeOptions);
                Assert.IsTrue(mergeResult.Success, $"MergeDataAsync failed: {mergeResult.Message}");

                var readBatches = new List<RecordBatch>();
                await foreach (RecordBatch readBatch in backend.ReadTableAsync(tablePath))
                {
                    readBatches.Add(readBatch);
                }

                var rowsById = new Dictionary<int, string>();
                foreach (RecordBatch readBatch in readBatches)
                {
                    var idArray = (Int32Array)readBatch.Column(0);
                    for (int i = 0; i < readBatch.Length; i++)
                    {
                        string name = readBatch.Column(1) switch
                        {
                            StringArray sa => sa.GetString(i),
                            StringViewArray sva => sva.GetString(i),
                            LargeStringArray lsa => lsa.GetString(i),
                            _ => throw new AssertFailedException(
                                $"Unexpected string column type: {readBatch.Column(1).GetType().FullName}")
                        } ?? string.Empty;

                        rowsById[idArray.GetValue(i) ?? -1] = name;
                    }
                }

                Assert.AreEqual(4, rowsById.Count);
                Assert.AreEqual("a", rowsById[1]);
                Assert.AreEqual("updated_b", rowsById[2]);
                Assert.AreEqual("c", rowsById[3]);
                Assert.AreEqual("d", rowsById[4]);
            }
            finally
            {
                backend.Dispose();
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_MergeAsync_ThrowsNotSupported()
        {
            using var backend = new NativeRustBackend();

            var ex = await Assert.ThrowsExceptionAsync<NotSupportedException>(() =>
                backend.MergeAsync(
                    "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.name = source.name",
                    "dummy-path",
                    "target"));

            StringAssert.Contains(ex.Message, "MergeDataAsync");
        }
    }
}
