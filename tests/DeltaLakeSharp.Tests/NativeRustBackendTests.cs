// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLakeSharp.Client;
using DeltaLakeSharp.Client.Internal;
using DeltaLakeSharp.Client.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DeltaLakeSharp.Tests
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
        public async Task NativeBackend_ReadTableAsDataReader_ReturnsRowsForwardOnly()
        {
            string tablePath = CreateTempTablePath("native_v3_reader");
            using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
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

                await client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch), SaveMode.Append);

                using DbDataReader reader = await client.ReadTableAsDataReaderAsync(tablePath);
                Assert.AreEqual(2, reader.FieldCount);
                Assert.AreEqual("id", reader.GetName(0));
                Assert.AreEqual("name", reader.GetName(1));

                var rows = new List<(int id, string name)>();
                while (reader.Read())
                {
                    rows.Add((reader.GetInt32(0), reader.GetString(1)));
                }

                rows = rows.OrderBy(r => r.id).ToList();
                CollectionAssert.AreEqual(new[] { (1, "Alice"), (2, "Bob"), (3, "Charlie") }, rows);
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_ExecuteQueryAsDataReader_ReturnsProjectedRows()
        {
            string tablePath = CreateTempTablePath("native_v3_query_reader");
            using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
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

                await client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch), SaveMode.Append);

                using DbDataReader reader = await client.ExecuteQueryAsDataReaderAsync(
                    "SELECT id FROM tbl WHERE id >= 2 ORDER BY id",
                    tablePath,
                    "tbl");

                var ids = new List<int>();
                while (reader.Read())
                {
                    ids.Add(reader.GetInt32(0));
                }

                CollectionAssert.AreEqual(new[] { 2, 3 }, ids);
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_ReadChangeDataAsDataReader_ReturnsCdfMetadataColumns()
        {
            string tablePath = CreateTempTablePath("native_v3_cdf_reader");
            using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("name", "string"),
                });

                ExecuteResult createResult = await client.CreateTableAsync(
                    tablePath,
                    tableSchema,
                    configuration: new Dictionary<string, string>
                    {
                        ["delta.enableChangeDataFeed"] = "true",
                    });
                Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

                RecordBatch initialBatch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 1, "Alice" },
                    new object[] { 2, "Bob" },
                }, tableSchema);
                await client.InsertAsync(tablePath, initialBatch.Schema, ArrowConverter.ToAsyncEnumerable(initialBatch), SaveMode.Append);

                RecordBatch appendBatch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 3, "Charlie" },
                }, tableSchema);
                await client.InsertAsync(tablePath, appendBatch.Schema, ArrowConverter.ToAsyncEnumerable(appendBatch), SaveMode.Append);

                using DbDataReader reader = await client.ReadChangeDataAsDataReaderAsync(tablePath, startingVersion: 0);

                int changeTypeOrdinal = reader.GetOrdinal("_change_type");
                int commitVersionOrdinal = reader.GetOrdinal("_commit_version");
                int rowCount = 0;
                while (reader.Read())
                {
                    Assert.IsFalse(reader.IsDBNull(changeTypeOrdinal));
                    Assert.IsFalse(reader.IsDBNull(commitVersionOrdinal));
                    rowCount++;
                }

                Assert.IsTrue(rowCount > 0, "Expected at least one CDF row.");
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_ReadTableAsDataReader_DefaultDecimalBehavior_UsesSqlDecimal()
        {
            string tablePath = CreateTempTablePath("native_v3_decimal_reader");
            using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("unit_price", "decimal(18,2)"),
                });

                ExecuteResult createResult = await client.CreateTableAsync(tablePath, tableSchema);
                Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

                RecordBatch batch = new RecordBatch.Builder()
                    .Append("id", nullable: false, new Int32Array.Builder().Append(1).Build())
                    .Append("unit_price", nullable: false, new Decimal128Array.Builder(new Apache.Arrow.Types.Decimal128Type(18, 2)).Append(12.34m).Build())
                    .Build();

                await client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch), SaveMode.Append);

                using DbDataReader reader = await client.ReadTableAsDataReaderAsync(tablePath);
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(typeof(SqlDecimal), reader.GetFieldType(1));
                Assert.AreEqual(new SqlDecimal(12.34m), ((ArrowStreamDataReader)reader).GetSqlDecimal(1));
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_ReadTableAsDataReader_ReturnsTimestampValues()
        {
            string tablePath = CreateTempTablePath("native_v3_reader_timestamp");
            using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("ts_aware", "timestamp"),
                    new ColumnDefinition("ts_naive", "timestamp_ntz"),
                });

                ExecuteResult createResult = await client.CreateTableAsync(tablePath, tableSchema);
                Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

                DateTimeOffset awareTimestamp = new DateTimeOffset(2025, 7, 4, 14, 30, 0, TimeSpan.Zero);
                DateTime naiveTimestamp = new DateTime(2025, 7, 4, 14, 30, 0, DateTimeKind.Unspecified);

                RecordBatch batch = new RecordBatch.Builder()
                    .Append("id", nullable: false, new Int32Array.Builder().Append(1).Build())
                    .Append("ts_aware", nullable: false, new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC")).Append(awareTimestamp).Build())
                    .Append("ts_naive", nullable: false, new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, (string)null)).Append(new DateTimeOffset(naiveTimestamp, TimeSpan.Zero)).Build())
                    .Build();

                await client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch), SaveMode.Append);

                using DbDataReader reader = await client.ReadTableAsDataReaderAsync(tablePath);
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(typeof(DateTimeOffset), reader.GetFieldType(1));
                Assert.AreEqual(awareTimestamp, reader.GetValue(1));
                Assert.AreEqual(awareTimestamp.UtcDateTime, reader.GetDateTime(1));
                Assert.AreEqual(typeof(DateTime), reader.GetFieldType(2));
                Assert.AreEqual(naiveTimestamp, reader.GetValue(2));
                Assert.AreEqual(naiveTimestamp, reader.GetDateTime(2));
                Assert.IsFalse(reader.Read());
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_ReadTableAsDataReader_OverflowDecimalAsString_ReturnsStringForPrecision38()
        {
            string tablePath = CreateTempTablePath("native_v3_table_decimal_string");
            using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
            try
            {
                await CreateSingleRowDecimalTableAsync(client, tablePath, SqlDecimal.Parse("123456789012345678901234567890123456.78")).ConfigureAwait(false);

                using DbDataReader reader = await client.ReadTableAsDataReaderAsync(
                    tablePath,
                    options: new DeltaDataReaderOptions
                    {
                        DecimalBehavior = DeltaDataReaderDecimalBehavior.OverflowDecimalAsString,
                    }).ConfigureAwait(false);

                Assert.IsTrue(reader.Read());
                Assert.AreEqual(typeof(string), reader.GetFieldType(1));
                Assert.AreEqual("123456789012345678901234567890123456.78", reader.GetValue(1));
                Assert.AreEqual("123456789012345678901234567890123456.78", reader.GetString(1));
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_ReadTableAsDataReader_ThrowOnOverflow_ThrowsForPrecision38()
        {
            string tablePath = CreateTempTablePath("native_v3_table_decimal_throw");
            using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
            try
            {
                await CreateSingleRowDecimalTableAsync(client, tablePath, SqlDecimal.Parse("123456789012345678901234567890123456.78")).ConfigureAwait(false);

                using DbDataReader reader = await client.ReadTableAsDataReaderAsync(
                    tablePath,
                    options: new DeltaDataReaderOptions
                    {
                        DecimalBehavior = DeltaDataReaderDecimalBehavior.ThrowOnOverflow,
                    }).ConfigureAwait(false);

                Assert.IsTrue(reader.Read());
                Assert.AreEqual(typeof(decimal), reader.GetFieldType(1));
                Assert.ThrowsExactly<OverflowException>(() => reader.GetValue(1));
                Assert.ThrowsExactly<OverflowException>(() => reader.GetDecimal(1));
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_ReadTableAsync_BinaryColumnRoundTripsThroughArrowBatches()
        {
            string tablePath = CreateTempTablePath("native_v3_binary_arrow_roundtrip");
            using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("payload", "binary"),
                });

                ExecuteResult createResult = await client.CreateTableAsync(tablePath, tableSchema);
                Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

                byte[] payload = new byte[] { 1, 2, 3, 4 };
                RecordBatch batch = new RecordBatch.Builder()
                    .Append("id", nullable: false, new Int32Array.Builder().Append(1).Build())
                    .Append("payload", nullable: false, new BinaryArray.Builder().Append(payload).Build())
                    .Build();

                await client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch), SaveMode.Append);

                var resultBatches = new List<RecordBatch>();
                await foreach (RecordBatch readBatch in client.ReadTableAsync(tablePath))
                {
                    resultBatches.Add(readBatch);
                }

                Assert.AreEqual(1, resultBatches.Count);
                Assert.AreEqual(1, resultBatches[0].Length);
                CollectionAssert.AreEqual(payload, (byte[])V3TestHelpers.ReadValue(resultBatches[0].Column(1), 0));
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_ExecuteQueryAsDataReader_UseDecimal_ReturnsDecimalForSupportedPrecision()
        {
            string tablePath = CreateTempTablePath("native_v3_decimal_query_decimal");
            using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
            try
            {
                await CreateSingleRowIntTableAsync(client, tablePath).ConfigureAwait(false);

                using DbDataReader reader = await client.ExecuteQueryAsDataReaderAsync(
                    "SELECT CAST('12.34' AS DECIMAL(18,2)) AS amount FROM tbl LIMIT 1",
                    tablePath,
                    "tbl",
                    options: new DeltaDataReaderOptions
                    {
                        DecimalBehavior = DeltaDataReaderDecimalBehavior.UseDecimal,
                    }).ConfigureAwait(false);

                Assert.IsTrue(reader.Read());
                Assert.AreEqual(typeof(decimal), reader.GetFieldType(0));
                Assert.AreEqual(12.34m, reader.GetDecimal(0));
                Assert.AreEqual(12.34m, reader.GetValue(0));
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_ExecuteQueryAsDataReader_OverflowDecimalAsString_ReturnsStringForPrecision38()
        {
            string tablePath = CreateTempTablePath("native_v3_decimal_query_string");
            using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
            try
            {
                await CreateSingleRowIntTableAsync(client, tablePath).ConfigureAwait(false);

                using DbDataReader reader = await client.ExecuteQueryAsDataReaderAsync(
                    "SELECT CAST('123456789012345678901234567890123456.78' AS DECIMAL(38,2)) AS amount FROM tbl LIMIT 1",
                    tablePath,
                    "tbl",
                    options: new DeltaDataReaderOptions
                    {
                        DecimalBehavior = DeltaDataReaderDecimalBehavior.OverflowDecimalAsString,
                    }).ConfigureAwait(false);

                Assert.IsTrue(reader.Read());
                Assert.AreEqual(typeof(string), reader.GetFieldType(0));
                Assert.AreEqual("123456789012345678901234567890123456.78", reader.GetString(0));
                Assert.AreEqual("123456789012345678901234567890123456.78", reader.GetValue(0));
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_ExecuteQueryAsDataReader_ThrowOnOverflow_ThrowsForPrecision38()
        {
            string tablePath = CreateTempTablePath("native_v3_decimal_query_throw");
            using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
            try
            {
                await CreateSingleRowIntTableAsync(client, tablePath).ConfigureAwait(false);

                using DbDataReader reader = await client.ExecuteQueryAsDataReaderAsync(
                    "SELECT CAST('123456789012345678901234567890123456.78' AS DECIMAL(38,2)) AS amount FROM tbl LIMIT 1",
                    tablePath,
                    "tbl",
                    options: new DeltaDataReaderOptions
                    {
                        DecimalBehavior = DeltaDataReaderDecimalBehavior.ThrowOnOverflow,
                    }).ConfigureAwait(false);

                Assert.IsTrue(reader.Read());
                Assert.AreEqual(typeof(decimal), reader.GetFieldType(0));
                Assert.ThrowsExactly<OverflowException>(() => reader.GetValue(0));
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_InsertAsync_ToMissingTable_CreatesTableAndWritesData()
        {
            string tablePath = CreateTempTablePath("native_v3_implicit_create");
            CleanupTablePath(tablePath);

            var backend = new NativeRustBackend();
            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("name", "string"),
                });

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
                    mode: "overwrite");

                TableSchema readSchema = await backend.GetSchemaAsync(tablePath);
                CollectionAssert.AreEqual(
                    new[] { "id", "name" },
                    readSchema.Columns.Select(c => c.Name).ToArray());

                var rows = new List<(int id, string name)>();
                await foreach (RecordBatch readBatch in backend.ReadTableAsync(tablePath))
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

                        rows.Add((idArray.GetValue(i) ?? -1, name));
                    }
                }

                rows = rows.OrderBy(r => r.id).ToList();
                Assert.AreEqual(3, rows.Count);
                Assert.AreEqual((1, "Alice"), rows[0]);
                Assert.AreEqual((2, "Bob"), rows[1]);
                Assert.AreEqual((3, "Charlie"), rows[2]);
            }
            finally
            {
                backend.Dispose();
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_InsertAsync_ToMissingPartitionedTable_CreatesPartitionedTableAndWritesData()
        {
            string tablePath = CreateTempTablePath("native_v3_implicit_partitioned");
            CleanupTablePath(tablePath);

            var backend = new NativeRustBackend();
            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("region", "string"),
                    new ColumnDefinition("name", "string"),
                });

                RecordBatch batch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 1, "US", "Alice" },
                    new object[] { 2, "EU", "Bob" },
                    new object[] { 3, "US", "Charlie" },
                }, tableSchema);

                await backend.InsertAsync(
                    tablePath,
                    batch.Schema,
                    ArrowConverter.ToAsyncEnumerable(batch),
                    mode: "overwrite",
                    partitionBy: new[] { "region" });

                TableSchema readSchema = await backend.GetSchemaAsync(tablePath);
                CollectionAssert.AreEqual(
                    new[] { "id", "region", "name" },
                    readSchema.Columns.Select(c => c.Name).ToArray());

                var rows = new List<(int id, string region, string name)>();
                await foreach (RecordBatch readBatch in backend.ReadTableAsync(tablePath))
                {
                    var idArray = (Int32Array)readBatch.Column(0);
                    for (int i = 0; i < readBatch.Length; i++)
                    {
                        string region = V3TestHelpers.ReadStringValue(readBatch.Column(1), i);
                        string name = V3TestHelpers.ReadStringValue(readBatch.Column(2), i);
                        rows.Add((idArray.GetValue(i) ?? -1, region, name));
                    }
                }

                rows = rows.OrderBy(r => r.id).ToList();
                Assert.AreEqual(3, rows.Count);
                Assert.AreEqual((1, "US", "Alice"), rows[0]);
                Assert.AreEqual((2, "EU", "Bob"), rows[1]);
                Assert.AreEqual((3, "US", "Charlie"), rows[2]);
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
                    resultText.IndexOf("changeDataFeed", StringComparison.OrdinalIgnoreCase) >= 0,
                    $"Expected protocol result to mention changeDataFeed. Actual: {resultText}");
            }
            finally
            {
                backend.Dispose();
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_ReadChangeDataAsync_ReturnsExpectedChanges()
        {
            string tablePath = CreateTempTablePath("native_v3_cdf");
            var backend = new NativeRustBackend();
            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("name", "string"),
                });

                ExecuteResult createResult = await backend.CreateEmptyTableAsync(
                    tablePath,
                    tableSchema,
                    configuration: new Dictionary<string, string>
                    {
                        ["delta.enableChangeDataFeed"] = "true",
                    });
                Assert.IsTrue(createResult.Success, $"CreateEmptyTableAsync failed: {createResult.Message}");

                ExecuteResult upgradeResult = await backend.UpgradeTableProtocolAsync(
                    tablePath,
                    readerVersion: 3,
                    writerVersion: 7,
                    writerFeatures: new[] { "changeDataFeed" });
                Assert.IsTrue(upgradeResult.Success, $"UpgradeTableProtocolAsync failed: {upgradeResult.Message}");

                RecordBatch initialBatch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 1, "a" },
                    new object[] { 2, "b" },
                }, tableSchema);
                await backend.InsertAsync(
                    tablePath,
                    initialBatch.Schema,
                    ArrowConverter.ToAsyncEnumerable(initialBatch),
                    mode: "append");

                ExecuteResult updateResult = await backend.UpdateAsync(
                    "UPDATE native_tbl SET name = 'b2' WHERE id = 2",
                    tablePath,
                    "native_tbl");
                Assert.IsTrue(updateResult.Success, $"UpdateAsync failed: {updateResult.Message}");

                ExecuteResult deleteResult = await backend.DeleteAsync(
                    "DELETE FROM native_tbl WHERE id = 1",
                    tablePath,
                    "native_tbl");
                Assert.IsTrue(deleteResult.Success, $"DeleteAsync failed: {deleteResult.Message}");

                RecordBatch appendBatch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 3, "c" },
                }, tableSchema);
                await backend.InsertAsync(
                    tablePath,
                    appendBatch.Schema,
                    ArrowConverter.ToAsyncEnumerable(appendBatch),
                    mode: "append");

                using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
                List<Dictionary<string, object?>> cdfRows = await V3TestHelpers.ReadAllChangeDataRowsAsync(
                    client,
                    tablePath,
                    startingVersion: 2);

                Assert.IsTrue(cdfRows.Count >= 5, $"Expected multiple CDF rows, got {cdfRows.Count}.");
                Assert.IsTrue(cdfRows.All(r => r.ContainsKey("_change_type")), "Expected _change_type column in all CDF rows.");
                Assert.IsTrue(cdfRows.All(r => r.ContainsKey("_commit_version")), "Expected _commit_version column in all CDF rows.");
                Assert.IsTrue(cdfRows.All(r => r.ContainsKey("_commit_timestamp")), "Expected _commit_timestamp column in all CDF rows.");

                CollectionAssert.Contains(cdfRows.Select(r => r["_change_type"]?.ToString()).ToList(), "insert");
                CollectionAssert.Contains(cdfRows.Select(r => r["_change_type"]?.ToString()).ToList(), "delete");
                CollectionAssert.Contains(cdfRows.Select(r => r["_change_type"]?.ToString()).ToList(), "update_postimage");

                Assert.IsTrue(cdfRows.Any(r => Equals(r["id"], 1) && Equals(r["_change_type"], "delete")),
                    "Expected delete CDF row for id=1.");
                Assert.IsTrue(cdfRows.Any(r => Equals(r["id"], 2) && Equals(r["name"], "b2") && Equals(r["_change_type"], "update_postimage")),
                    "Expected update_postimage CDF row for id=2.");
                Assert.IsTrue(cdfRows.Any(r => Equals(r["id"], 3) && Equals(r["name"], "c") && Equals(r["_change_type"], "insert")),
                    "Expected insert CDF row for id=3.");
            }
            finally
            {
                backend.Dispose();
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_ExecuteChangeDataQueryAsync_FiltersAndProjectsRows()
        {
            string tablePath = CreateTempTablePath("native_v3_cdf_query");
            var backend = new NativeRustBackend();
            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("name", "string"),
                });

                ExecuteResult createResult = await backend.CreateEmptyTableAsync(
                    tablePath,
                    tableSchema,
                    configuration: new Dictionary<string, string>
                    {
                        ["delta.enableChangeDataFeed"] = "true",
                    });
                Assert.IsTrue(createResult.Success, $"CreateEmptyTableAsync failed: {createResult.Message}");

                RecordBatch initialBatch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 1, "a" },
                    new object[] { 2, "b" },
                }, tableSchema);
                await backend.InsertAsync(
                    tablePath,
                    initialBatch.Schema,
                    ArrowConverter.ToAsyncEnumerable(initialBatch),
                    mode: "append");

                ExecuteResult updateResult = await backend.UpdateAsync(
                    "UPDATE native_tbl SET name = 'b2' WHERE id = 2",
                    tablePath,
                    "native_tbl");
                Assert.IsTrue(updateResult.Success, $"UpdateAsync failed: {updateResult.Message}");

                var resultBatches = new List<RecordBatch>();
                await foreach (RecordBatch batch in backend.ExecuteChangeDataQueryAsync(
                    "SELECT id, name, _change_type FROM _cdf WHERE _change_type <> 'update_preimage' ORDER BY id, _change_type",
                    tablePath,
                    startingVersion: 1))
                {
                    resultBatches.Add(batch);
                }

                List<Dictionary<string, object?>> rows = FlattenRows(resultBatches);
                Assert.IsTrue(rows.Count >= 3, $"Expected at least 3 projected CDF rows, got {rows.Count}.");
                Assert.IsFalse(rows.Any(r => r.ContainsKey("_commit_version")), "Expected projection to exclude _commit_version.");
                Assert.IsFalse(rows.Any(r => Equals(r["_change_type"], "update_preimage")), "Expected filter to exclude update_preimage rows.");
                Assert.IsTrue(rows.Any(r => Equals(r["id"], 1) && Equals(r["name"], "a") && Equals(r["_change_type"], "insert")));
                Assert.IsTrue(rows.Any(r => Equals(r["id"], 2) && Equals(r["name"], "b2") && Equals(r["_change_type"], "update_postimage")));
            }
            finally
            {
                backend.Dispose();
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_ExecuteChangeDataQueryAsDataReader_FiltersAndProjectsRows()
        {
            string tablePath = CreateTempTablePath("native_v3_cdf_query_reader");
            using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
            try
            {
                var tableSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("name", "string"),
                });

                ExecuteResult createResult = await client.CreateTableAsync(
                    tablePath,
                    tableSchema,
                    configuration: new Dictionary<string, string>
                    {
                        ["delta.enableChangeDataFeed"] = "true",
                    });
                Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

                RecordBatch initialBatch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 1, "a" },
                    new object[] { 2, "b" },
                }, tableSchema);
                await client.InsertAsync(tablePath, initialBatch.Schema, ArrowConverter.ToAsyncEnumerable(initialBatch), SaveMode.Append);

                ExecuteResult updateResult = await client.UpdateAsync(
                    "UPDATE native_tbl SET name = 'b2' WHERE id = 2",
                    tablePath,
                    "native_tbl");
                Assert.IsTrue(updateResult.Success, $"UpdateAsync failed: {updateResult.Message}");

                using DbDataReader reader = await client.ExecuteChangeDataQueryAsDataReaderAsync(
                    "SELECT id, name, _change_type FROM _cdf WHERE _change_type <> 'update_preimage' ORDER BY id, _change_type",
                    tablePath,
                    startingVersion: 1);

                Assert.AreEqual(3, reader.FieldCount);
                Assert.AreEqual("id", reader.GetName(0));
                Assert.AreEqual("name", reader.GetName(1));
                Assert.AreEqual("_change_type", reader.GetName(2));

                var rows = new List<(int id, string name, string changeType)>();
                while (reader.Read())
                {
                    rows.Add((reader.GetInt32(0), reader.GetString(1), reader.GetString(2)));
                }

                Assert.IsTrue(rows.Count >= 3, $"Expected at least 3 projected CDF rows, got {rows.Count}.");
                Assert.IsFalse(rows.Any(r => r.changeType == "update_preimage"), "Expected filter to exclude update_preimage rows.");
                Assert.IsTrue(rows.Any(r => r == (1, "a", "insert")));
                Assert.IsTrue(rows.Any(r => r == (2, "b2", "update_postimage")));
            }
            finally
            {
                CleanupTablePath(tablePath);
            }
        }

        private static async Task CreateSingleRowIntTableAsync(DeltaTableServiceClient client, string tablePath)
        {
            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
            });

            ExecuteResult createResult = await client.CreateTableAsync(tablePath, tableSchema).ConfigureAwait(false);
            Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

            RecordBatch batch = ArrowConverter.FromRows(new[]
            {
                new object[] { 1 },
            }, tableSchema);

            await client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch), SaveMode.Append).ConfigureAwait(false);
        }

        private static async Task CreateSingleRowDecimalTableAsync(DeltaTableServiceClient client, string tablePath, SqlDecimal value)
        {
            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("unit_price", "decimal(38,2)"),
            });

            ExecuteResult createResult = await client.CreateTableAsync(tablePath, tableSchema).ConfigureAwait(false);
            Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

            RecordBatch batch = new RecordBatch.Builder()
                .Append("id", nullable: false, new Int32Array.Builder().Append(1).Build())
                .Append("unit_price", nullable: false, new Decimal128Array.Builder(new Apache.Arrow.Types.Decimal128Type(38, 2)).Append(value).Build())
                .Build();

            await client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch), SaveMode.Append).ConfigureAwait(false);
        }

        private static List<Dictionary<string, object?>> FlattenRows(IEnumerable<RecordBatch> batches)
        {
            var rows = new List<Dictionary<string, object?>>();
            foreach (RecordBatch batch in batches)
            {
                for (int rowIndex = 0; rowIndex < batch.Length; rowIndex++)
                {
                    var row = new Dictionary<string, object?>();
                    for (int colIndex = 0; colIndex < batch.ColumnCount; colIndex++)
                    {
                        string columnName = batch.Schema.GetFieldByIndex(colIndex).Name;
                        row[columnName] = V3TestHelpers.ReadValue(batch.Column(colIndex), rowIndex);
                    }

                    rows.Add(row);
                }
            }

            return rows;
        }

        [TestMethod]
        public async Task NativeBackend_InsertAsync_WithSchemaModeOverwrite_ReplacesSchemaAndData()
        {
            string tablePath = CreateTempTablePath("native_v3_schema_overwrite");
            var backend = new NativeRustBackend();
            try
            {
                var initialSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("name", "string"),
                });

                ExecuteResult createResult = await backend.CreateEmptyTableAsync(tablePath, initialSchema);
                Assert.IsTrue(createResult.Success, $"CreateEmptyTableAsync failed: {createResult.Message}");

                RecordBatch initialBatch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 1, "Alice" },
                    new object[] { 2, "Bob" },
                }, initialSchema);
                await backend.InsertAsync(
                    tablePath,
                    initialBatch.Schema,
                    ArrowConverter.ToAsyncEnumerable(initialBatch),
                    mode: "overwrite");

                var replacementSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("city", "string"),
                    new ColumnDefinition("active", "boolean"),
                });
                RecordBatch replacementBatch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 10, "Seattle", true },
                    new object[] { 20, "Portland", false },
                }, replacementSchema);
                await backend.InsertAsync(
                    tablePath,
                    replacementBatch.Schema,
                    ArrowConverter.ToAsyncEnumerable(replacementBatch),
                    mode: "overwrite",
                    schemaMode: WriteSchemaMode.Overwrite);

                TableSchema readSchema = await backend.GetSchemaAsync(tablePath);
                CollectionAssert.AreEqual(
                    new[] { "id", "city", "active" },
                    readSchema.Columns.Select(c => c.Name).ToArray());

                var rows = new List<(int id, string city, bool active)>();
                await foreach (RecordBatch readBatch in backend.ReadTableAsync(tablePath))
                {
                    var idArray = (Int32Array)readBatch.Column(0);
                    for (int i = 0; i < readBatch.Length; i++)
                    {
                        string city = readBatch.Column(1) switch
                        {
                            StringArray sa => sa.GetString(i),
                            StringViewArray sva => sva.GetString(i),
                            LargeStringArray lsa => lsa.GetString(i),
                            _ => throw new AssertFailedException(
                                $"Unexpected string column type: {readBatch.Column(1).GetType().FullName}")
                        } ?? string.Empty;

                        rows.Add((
                            idArray.GetValue(i) ?? -1,
                            city,
                            ((BooleanArray)readBatch.Column(2)).GetValue(i)!.Value));
                    }
                }

                rows = rows.OrderBy(r => r.id).ToList();
                Assert.AreEqual(2, rows.Count);
                Assert.AreEqual((10, "Seattle", true), rows[0]);
                Assert.AreEqual((20, "Portland", false), rows[1]);
            }
            finally
            {
                backend.Dispose();
                CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public async Task NativeBackend_InsertAsync_WithNewSchemaWithoutSchemaMode_FailsAndPreservesExistingTable()
        {
            string tablePath = CreateTempTablePath("native_v3_schema_overwrite_fail");
            var backend = new NativeRustBackend();
            try
            {
                var initialSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("name", "string"),
                });

                ExecuteResult createResult = await backend.CreateEmptyTableAsync(tablePath, initialSchema);
                Assert.IsTrue(createResult.Success, $"CreateEmptyTableAsync failed: {createResult.Message}");

                RecordBatch initialBatch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 1, "Alice" },
                    new object[] { 2, "Bob" },
                }, initialSchema);
                await backend.InsertAsync(
                    tablePath,
                    initialBatch.Schema,
                    ArrowConverter.ToAsyncEnumerable(initialBatch),
                    mode: "overwrite");

                var replacementSchema = new TableSchema(new List<ColumnDefinition>
                {
                    new ColumnDefinition("id", "int32"),
                    new ColumnDefinition("city", "string"),
                    new ColumnDefinition("active", "boolean"),
                });
                RecordBatch replacementBatch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 10, "Seattle", true },
                    new object[] { 20, "Portland", false },
                }, replacementSchema);

                var ex = await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
                {
                    await backend.InsertAsync(
                        tablePath,
                        replacementBatch.Schema,
                        ArrowConverter.ToAsyncEnumerable(replacementBatch),
                        mode: "overwrite");
                });

                StringAssert.Contains(ex.Message.ToLowerInvariant(), "schema");

                TableSchema readSchema = await backend.GetSchemaAsync(tablePath);
                CollectionAssert.AreEqual(
                    new[] { "id", "name" },
                    readSchema.Columns.Select(c => c.Name).ToArray());

                var rows = new List<(int id, string name)>();
                await foreach (RecordBatch readBatch in backend.ReadTableAsync(tablePath))
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

                        rows.Add((idArray.GetValue(i) ?? -1, name));
                    }
                }

                rows = rows.OrderBy(r => r.id).ToList();
                Assert.AreEqual(2, rows.Count);
                Assert.AreEqual((1, "Alice"), rows[0]);
                Assert.AreEqual((2, "Bob"), rows[1]);
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

            var ex = await Assert.ThrowsExactlyAsync<NotSupportedException>(() =>
                backend.MergeAsync(
                    "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.name = source.name",
                    "dummy-path",
                    "target"));

            StringAssert.Contains(ex.Message, "MergeDataAsync");
        }
    }
}
