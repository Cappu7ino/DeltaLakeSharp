// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.ADMS.Testing.DeltaTableService.Client;
using Microsoft.ADMS.Testing.DeltaTableService.Client.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.ADMS.Testing.DeltaTableService.Tests
{
    /// <summary>
    /// Integration tests for the Delta Table Service V3 native backend.
    ///
    /// The public client still exercises the full V3 surface, but the runtime is
    /// now in-process native Rust instead of a spawned Flight server.
    ///
    /// Run with: dotnet test --filter "TestCategory=Integration&amp;TestCategory=V3Native"
    /// </summary>
    [TestClass]
    [TestCategory("Integration")]
    [TestCategory("V3")]
    [TestCategory("V3Native")]
    public class DeltaTableServiceV3IntegrationTests
    {
        private V3TestHelpers.IntegrationContext? _context;

        [TestInitialize]
        public async Task TestInitialize()
        {
            _context = await V3TestHelpers.CreateIntegrationContextAsync();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            _context?.Dispose();
        }

        private DeltaTableServiceClient Client => _context!.Client;

        private string TestTablePath => _context!.TestTablePath;

        private string PartitionedTablePath => _context!.PartitionedTablePath;

        private string TimeTravelTablePath => _context!.TimeTravelTablePath;

        private string? FixtureDataDir => _context!.FixtureDataDir;

        private string NewWriteTestTablePath() => _context!.CreateWriteTestTablePath();

        // ================================================================== //
        //  Phase 1: Health check
        // ================================================================== //

        [TestMethod]
        public async Task V3_HealthCheck_ReturnsTrue()
        {
            bool healthy = await Client.HealthCheckAsync();
            Assert.IsTrue(healthy, "Expected the V3 server to report healthy.");
        }

        // ================================================================== //
        //  Phase 1: Service mode
        // ================================================================== //

        [TestMethod]
        public void V3_Client_ReportsCorrectServiceMode()
        {
            Assert.AreEqual(ServiceMode.V3_Rust, Client.Mode,
                "Expected client to report V3_Rust service mode.");
        }

        // ================================================================== //
        //  Phase 2: GetSchema
        // ================================================================== //

        [TestMethod]
        public async Task V3_GetSchema_ReturnsCorrectSchema()
        {
            TableSchema schema = await Client.GetSchemaAsync(TestTablePath);

            Assert.IsNotNull(schema, "Schema should not be null.");
            Assert.AreEqual(2, schema.Columns.Count,
                $"Expected 2 columns, got {schema.Columns.Count}.");

            // Column 0: id (Int32)
            Assert.AreEqual("id", schema.Columns[0].Name);
            Assert.AreEqual("int32", schema.Columns[0].DataType,
                $"Expected int32 type for 'id', got '{schema.Columns[0].DataType}'.");

            // Column 1: name (Utf8 → "string" per ArrowConverter)
            Assert.AreEqual("name", schema.Columns[1].Name);
            Assert.AreEqual("string", schema.Columns[1].DataType,
                $"Expected string type for 'name', got '{schema.Columns[1].DataType}'.");
        }

        // ================================================================== //
        //  Phase 2: ReadTable
        // ================================================================== //

        [TestMethod]
        public async Task V3_ReadTable_ReturnsAllRows()
        {
            var batches = new List<RecordBatch>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(TestTablePath))
            {
                batches.Add(batch);
            }

            Assert.IsTrue(batches.Count > 0, "Expected at least one RecordBatch.");

            long totalRows = batches.Sum(b => b.Length);
            Assert.AreEqual(3, totalRows,
                $"Expected 3 rows total, got {totalRows}.");

            // Verify schema of the returned data.
            var firstBatch = batches[0];
            Assert.AreEqual(2, firstBatch.ColumnCount,
                $"Expected 2 columns, got {firstBatch.ColumnCount}.");
            Assert.AreEqual("id", firstBatch.Schema.FieldsList[0].Name);
            Assert.AreEqual("name", firstBatch.Schema.FieldsList[1].Name);
        }

        [TestMethod]
        public async Task V3_ReadTable_ReturnsCorrectData()
        {
            var ids = new List<int>();
            var names = new List<string?>();

            await foreach (RecordBatch batch in Client.ReadTableAsync(TestTablePath))
            {
                var idArray = (Int32Array)batch.Column(0);
                // delta-rs 0.31 writes Utf8View (Arrow 57), which the C# client
                // deserializes as StringViewArray instead of StringArray.
                // Handle both types for forward-compatibility.
                IArrowArray nameCol = batch.Column(1);

                for (int i = 0; i < batch.Length; i++)
                {
                    ids.Add(idArray.GetValue(i)!.Value);
                    names.Add(nameCol switch
                    {
                        StringArray sa => sa.GetString(i),
                        StringViewArray sva => sva.GetString(i),
                        _ => throw new InvalidOperationException(
                            $"Unexpected array type for 'name' column: {nameCol.GetType().Name}")
                    });
                }
            }

            // Sort by id to ensure deterministic comparison
            var sorted = ids.Zip(names, (id, name) => (id, name))
                .OrderBy(x => x.id)
                .ToList();

            Assert.AreEqual(3, sorted.Count, "Expected 3 rows.");
            Assert.AreEqual((1, "a"), (sorted[0].id, sorted[0].name));
            Assert.AreEqual((2, "b"), (sorted[1].id, sorted[1].name));
            Assert.AreEqual((3, "c"), (sorted[2].id, sorted[2].name));
        }

        // ================================================================== //
        //  Phase 2: ExecuteQuery — SQL without table registration
        // ================================================================== //

        [TestMethod]
        public async Task V3_ExecuteQuery_SelectLiteral()
        {
            var batches = new List<RecordBatch>();
            await foreach (RecordBatch batch in Client.ExecuteQueryAsync(
                "SELECT 42 AS answer"))
            {
                batches.Add(batch);
            }

            Assert.IsTrue(batches.Count > 0, "Expected at least one RecordBatch.");

            long totalRows = batches.Sum(b => b.Length);
            Assert.AreEqual(1, totalRows, $"Expected 1 row, got {totalRows}.");

            // Verify the column name and value.
            var firstBatch = batches[0];
            Assert.AreEqual("answer", firstBatch.Schema.FieldsList[0].Name);
        }

        // ================================================================== //
        //  Phase 2: ExecuteQuery — SQL with table registration
        // ================================================================== //

        [TestMethod]
        public async Task V3_ExecuteQuery_WithRegisteredTable()
        {
            var ids = new List<int>();

            await foreach (RecordBatch batch in Client.ExecuteQueryAsync(
                sql: "SELECT id FROM tbl WHERE id > 1",
                tablePath: TestTablePath,
                tableName: "tbl"))
            {
                var idArray = (Int32Array)batch.Column(0);
                for (int i = 0; i < batch.Length; i++)
                {
                    ids.Add(idArray.GetValue(i)!.Value);
                }
            }

            ids.Sort();
            Assert.AreEqual(2, ids.Count, $"Expected 2 rows, got {ids.Count}.");
            Assert.AreEqual(2, ids[0]);
            Assert.AreEqual(3, ids[1]);
        }

        [TestMethod]
        public async Task V3_ExecuteQuery_SelectAllWithLimit()
        {
            var batches = new List<RecordBatch>();
            await foreach (RecordBatch batch in Client.ExecuteQueryAsync(
                sql: "SELECT * FROM tbl LIMIT 2",
                tablePath: TestTablePath,
                tableName: "tbl"))
            {
                batches.Add(batch);
            }

            long totalRows = batches.Sum(b => b.Length);
            Assert.AreEqual(2, totalRows,
                $"Expected 2 rows with LIMIT 2, got {totalRows}.");
        }

        // ================================================================== //
        //  Phase 2: Error handling — invalid path
        // ================================================================== //

        [TestMethod]
        public async Task V3_ReadTable_InvalidPath_ReturnsError()
        {
            var ex = await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
            {
                await foreach (RecordBatch _ in Client.ReadTableAsync(
                    "/nonexistent/path/to/table"))
                {
                    // Should not reach here.
                }
            });

            V3TestHelpers.AssertNativeFailure(ex);
        }

        [TestMethod]
        public async Task V3_GetSchema_InvalidPath_ReturnsError()
        {
            var ex = await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
            {
                await Client.GetSchemaAsync("/nonexistent/path/to/table");
            });

            V3TestHelpers.AssertNativeFailure(ex);
        }

        [TestMethod]
        public async Task V3_ExecuteQuery_InvalidSql_ReturnsError()
        {
            var ex = await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
            {
                await foreach (RecordBatch _ in Client.ExecuteQueryAsync(
                    "SELECT * FROM nonexistent_table_xyz"))
                {
                    // Should not reach here.
                }
            });

            V3TestHelpers.AssertNativeFailure(ex);
        }

        // ================================================================== //
        //  Phase 2: Partitioned tables
        // ================================================================== //

        [TestMethod]
        public async Task V3_ReadTable_Partitioned_ReturnsAllRows()
        {
            var batches = new List<RecordBatch>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(PartitionedTablePath))
            {
                batches.Add(batch);
            }

            long totalRows = batches.Sum(b => b.Length);
            Assert.AreEqual(5, totalRows,
                $"Expected 5 rows from partitioned table, got {totalRows}.");
        }

        [TestMethod]
        public async Task V3_ReadTable_Partitioned_HasPartitionColumn()
        {
            RecordBatch? firstBatch = null;
            await foreach (RecordBatch batch in Client.ReadTableAsync(PartitionedTablePath))
            {
                firstBatch ??= batch;
            }

            Assert.IsNotNull(firstBatch, "Expected at least one RecordBatch.");
            var fieldNames = firstBatch.Schema.FieldsList.Select(f => f.Name).ToList();
            CollectionAssert.Contains(fieldNames, "id",
                $"Missing 'id' column. Got: [{string.Join(", ", fieldNames)}]");
            CollectionAssert.Contains(fieldNames, "name",
                $"Missing 'name' column. Got: [{string.Join(", ", fieldNames)}]");
            CollectionAssert.Contains(fieldNames, "region",
                $"Missing 'region' column. Got: [{string.Join(", ", fieldNames)}]");
        }

        [TestMethod]
        public async Task V3_GetSchema_Partitioned_IncludesPartitionColumn()
        {
            TableSchema schema = await Client.GetSchemaAsync(PartitionedTablePath);

            Assert.IsNotNull(schema, "Schema should not be null.");
            Assert.AreEqual(3, schema.Columns.Count,
                $"Expected 3 columns, got {schema.Columns.Count}.");

            var columnNames = schema.Columns.Select(c => c.Name).ToList();
            CollectionAssert.Contains(columnNames, "id");
            CollectionAssert.Contains(columnNames, "name");
            CollectionAssert.Contains(columnNames, "region");
        }

        [TestMethod]
        public async Task V3_ExecuteQuery_Partitioned_FilterByRegion()
        {
            var ids = new List<int>();

            await foreach (RecordBatch batch in Client.ExecuteQueryAsync(
                sql: "SELECT id FROM tbl WHERE region = 'us' ORDER BY id",
                tablePath: PartitionedTablePath,
                tableName: "tbl"))
            {
                var idArray = (Int32Array)batch.Column(0);
                for (int i = 0; i < batch.Length; i++)
                {
                    ids.Add(idArray.GetValue(i)!.Value);
                }
            }

            // Partitioned data: (1,"a","us"), (3,"c","us") → ids 1, 3
            Assert.AreEqual(2, ids.Count,
                $"Expected 2 rows for region='us', got {ids.Count}.");
            Assert.AreEqual(1, ids[0]);
            Assert.AreEqual(3, ids[1]);
        }

        // ================================================================== //
        //  Phase 2: Time travel
        // ================================================================== //

        [TestMethod]
        public async Task V3_ReadTable_TimeTravel_Version0_Returns2Rows()
        {
            var batches = new List<RecordBatch>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(
                TimeTravelTablePath, version: 0))
            {
                batches.Add(batch);
            }

            long totalRows = batches.Sum(b => b.Length);
            Assert.AreEqual(2, totalRows,
                $"Expected 2 rows at version 0, got {totalRows}.");
        }

        [TestMethod]
        public async Task V3_ReadTable_TimeTravel_Latest_Returns4Rows()
        {
            var batches = new List<RecordBatch>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(TimeTravelTablePath))
            {
                batches.Add(batch);
            }

            long totalRows = batches.Sum(b => b.Length);
            Assert.AreEqual(4, totalRows,
                $"Expected 4 rows at latest version, got {totalRows}.");
        }

        [TestMethod]
        public async Task V3_ReadTable_TimeTravel_Version0_HasCorrectData()
        {
            var names = new List<string?>();

            await foreach (RecordBatch batch in Client.ReadTableAsync(
                TimeTravelTablePath, version: 0))
            {
                IArrowArray nameCol = batch.Column(1);
                for (int i = 0; i < batch.Length; i++)
                {
                    names.Add(nameCol switch
                    {
                        StringArray sa => sa.GetString(i),
                        StringViewArray sva => sva.GetString(i),
                        _ => throw new InvalidOperationException(
                            $"Unexpected array type: {nameCol.GetType().Name}")
                    });
                }
            }

            names.Sort();
            Assert.AreEqual(2, names.Count, "Expected 2 rows at version 0.");
            Assert.AreEqual("v0_a", names[0]);
            Assert.AreEqual("v0_b", names[1]);
        }

        [TestMethod]
        public async Task V3_GetSchema_TimeTravel_Version0()
        {
            TableSchema schema = await Client.GetSchemaAsync(
                TimeTravelTablePath, version: 0);

            Assert.IsNotNull(schema, "Schema should not be null.");
            Assert.AreEqual(2, schema.Columns.Count,
                $"Expected 2 columns, got {schema.Columns.Count}.");
            Assert.AreEqual("id", schema.Columns[0].Name);
            Assert.AreEqual("name", schema.Columns[1].Name);
        }

        // ================================================================== //
        //  Phase 2: numRows limit
        // ================================================================== //

        [TestMethod]
        public async Task V3_ReadTable_NumRows_LimitsResults()
        {
            var batches = new List<RecordBatch>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(
                TestTablePath, numRows: 1))
            {
                batches.Add(batch);
            }

            long totalRows = batches.Sum(b => b.Length);
            Assert.AreEqual(1, totalRows,
                $"Expected 1 row with numRows=1, got {totalRows}.");
        }

        [TestMethod]
        public async Task V3_ReadTable_NumRows_Zero_ReturnsEmpty()
        {
            var batches = new List<RecordBatch>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(
                TestTablePath, numRows: 0))
            {
                batches.Add(batch);
            }

            long totalRows = batches.Sum(b => b.Length);
            Assert.AreEqual(0, totalRows,
                $"Expected 0 rows with numRows=0, got {totalRows}.");
        }

        [TestMethod]
        public async Task V3_ReadTable_NumRows_LargerThanTable_ReturnsAll()
        {
            var batches = new List<RecordBatch>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(
                TestTablePath, numRows: 100))
            {
                batches.Add(batch);
            }

            long totalRows = batches.Sum(b => b.Length);
            Assert.AreEqual(3, totalRows,
                $"Expected 3 rows (all) with numRows=100, got {totalRows}.");
        }

        [TestMethod]
        public async Task V3_ReadTable_Partitioned_NumRows_LimitsResults()
        {
            var batches = new List<RecordBatch>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(
                PartitionedTablePath, numRows: 2))
            {
                batches.Add(batch);
            }

            long totalRows = batches.Sum(b => b.Length);
            Assert.AreEqual(2, totalRows,
                $"Expected 2 rows with numRows=2 on partitioned table, got {totalRows}.");
        }

        // ================================================================== //
        //  Phase 2: Column Mapping (checked-in PySpark fixture)
        // ================================================================== //

        [TestMethod]
        public async Task V3_ReadTable_ColumnMapping_ReturnsLogicalSchema()
        {
            if (FixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(FixtureDataDir, "delta_test_column_mapping_name");
            TableSchema schema = await Client.GetSchemaAsync(tablePath);

            Assert.IsNotNull(schema, "Schema should not be null.");
            Assert.AreEqual(2, schema.Columns.Count,
                $"Expected 2 columns, got {schema.Columns.Count}.");

            // Schema should use logical column names, not physical UUIDs.
            Assert.AreEqual("id", schema.Columns[0].Name);
            Assert.AreEqual("int32", schema.Columns[0].DataType,
                $"Expected int32 for 'id', got '{schema.Columns[0].DataType}'.");
            Assert.AreEqual("city", schema.Columns[1].Name);
            Assert.AreEqual("string", schema.Columns[1].DataType,
                $"Expected string for 'city', got '{schema.Columns[1].DataType}'.");
        }

        [TestMethod]
        public async Task V3_ReadTable_ColumnMapping_Returns3Rows()
        {
            if (FixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(FixtureDataDir, "delta_test_column_mapping_name");
            var batches = new List<RecordBatch>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(tablePath))
            {
                batches.Add(batch);
            }

            long totalRows = batches.Sum(b => b.Length);
            Assert.AreEqual(3, totalRows,
                $"Expected 3 rows from column mapping fixture, got {totalRows}.");
        }

        [TestMethod]
        public async Task V3_ReadTable_ColumnMapping_ReturnsCorrectData()
        {
            if (FixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(FixtureDataDir, "delta_test_column_mapping_name");
            var ids = new List<int>();
            var cities = new List<string?>();

            await foreach (RecordBatch batch in Client.ReadTableAsync(tablePath))
            {
                var idArray = (Int32Array)batch.Column(0);
                IArrowArray cityCol = batch.Column(1);

                for (int i = 0; i < batch.Length; i++)
                {
                    ids.Add(idArray.GetValue(i)!.Value);
                    cities.Add(cityCol switch
                    {
                        StringArray sa => sa.GetString(i),
                        StringViewArray sva => sva.GetString(i),
                        _ => throw new InvalidOperationException(
                            $"Unexpected array type for 'city' column: {cityCol.GetType().Name}")
                    });
                }
            }

            var sorted = ids.Zip(cities, (id, city) => (id, city))
                .OrderBy(x => x.id)
                .ToList();

            Assert.AreEqual(3, sorted.Count, "Expected 3 rows.");
            Assert.AreEqual((1, "Seattle"), (sorted[0].id, sorted[0].city));
            Assert.AreEqual((2, "Portland"), (sorted[1].id, sorted[1].city));
            Assert.AreEqual((3, "Denver"), (sorted[2].id, sorted[2].city));
        }

        [TestMethod]
        public async Task V3_ExecuteQuery_ColumnMapping_SqlFilterWorks()
        {
            if (FixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(FixtureDataDir, "delta_test_column_mapping_name");
            var ids = new List<int>();

            await foreach (RecordBatch batch in Client.ExecuteQueryAsync(
                sql: "SELECT id FROM tbl WHERE id >= 2 ORDER BY id",
                tablePath: tablePath,
                tableName: "tbl"))
            {
                var idArray = (Int32Array)batch.Column(0);
                for (int i = 0; i < batch.Length; i++)
                {
                    ids.Add(idArray.GetValue(i)!.Value);
                }
            }

            Assert.AreEqual(2, ids.Count, $"Expected 2 rows for id >= 2, got {ids.Count}.");
            Assert.AreEqual(2, ids[0]);
            Assert.AreEqual(3, ids[1]);
        }

        // ================================================================== //
        //  Phase 2: Deletion Vectors (checked-in PySpark fixture)
        // ================================================================== //

        [TestMethod]
        public async Task V3_ReadTable_DeletionVector_ReturnsSchema()
        {
            if (FixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(FixtureDataDir, "delta_test_deletion_vector");
            TableSchema schema = await Client.GetSchemaAsync(tablePath);

            Assert.IsNotNull(schema, "Schema should not be null.");
            Assert.AreEqual(2, schema.Columns.Count,
                $"Expected 2 columns, got {schema.Columns.Count}.");
            Assert.AreEqual("id", schema.Columns[0].Name);
            Assert.AreEqual("int32", schema.Columns[0].DataType);
            Assert.AreEqual("value", schema.Columns[1].Name);
            Assert.AreEqual("string", schema.Columns[1].DataType);
        }

        [TestMethod]
        public async Task V3_ReadTable_DeletionVector_Returns4Rows()
        {
            if (FixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(FixtureDataDir, "delta_test_deletion_vector");
            var batches = new List<RecordBatch>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(tablePath))
            {
                batches.Add(batch);
            }

            long totalRows = batches.Sum(b => b.Length);
            Assert.AreEqual(4, totalRows,
                $"Expected 4 rows (id=3 deleted), got {totalRows}.");
        }

        [TestMethod]
        public async Task V3_ReadTable_DeletionVector_ExcludesDeletedRow()
        {
            if (FixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(FixtureDataDir, "delta_test_deletion_vector");
            var ids = new List<int>();
            var values = new List<string?>();

            await foreach (RecordBatch batch in Client.ReadTableAsync(tablePath))
            {
                var idArray = (Int32Array)batch.Column(0);
                IArrowArray valueCol = batch.Column(1);

                for (int i = 0; i < batch.Length; i++)
                {
                    ids.Add(idArray.GetValue(i)!.Value);
                    values.Add(valueCol switch
                    {
                        StringArray sa => sa.GetString(i),
                        StringViewArray sva => sva.GetString(i),
                        _ => throw new InvalidOperationException(
                            $"Unexpected array type for 'value' column: {valueCol.GetType().Name}")
                    });
                }
            }

            var sorted = ids.Zip(values, (id, value) => (id, value))
                .OrderBy(x => x.id)
                .ToList();

            Assert.AreEqual(4, sorted.Count, "Expected 4 rows (id=3 deleted).");
            Assert.AreEqual((1, "one"), (sorted[0].id, sorted[0].value));
            Assert.AreEqual((2, "two"), (sorted[1].id, sorted[1].value));
            Assert.AreEqual((4, "four"), (sorted[2].id, sorted[2].value));
            Assert.AreEqual((5, "five"), (sorted[3].id, sorted[3].value));

            // id=3 should NOT be present.
            Assert.IsFalse(ids.Contains(3),
                "id=3 should be excluded by deletion vector.");
        }

        [TestMethod]
        public async Task V3_ExecuteQuery_DeletionVector_SqlFilterWorks()
        {
            if (FixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(FixtureDataDir, "delta_test_deletion_vector");
            var ids = new List<int>();

            await foreach (RecordBatch batch in Client.ExecuteQueryAsync(
                sql: "SELECT id FROM tbl WHERE id > 2 ORDER BY id",
                tablePath: tablePath,
                tableName: "tbl"))
            {
                var idArray = (Int32Array)batch.Column(0);
                for (int i = 0; i < batch.Length; i++)
                {
                    ids.Add(idArray.GetValue(i)!.Value);
                }
            }

            // id=3 is deleted, so only id=4 and id=5 match id > 2.
            Assert.AreEqual(2, ids.Count,
                $"Expected 2 rows for id > 2 (id=3 deleted), got {ids.Count}.");
            Assert.AreEqual(4, ids[0]);
            Assert.AreEqual(5, ids[1]);
        }

        // ================================================================== //
        //  Phase 3: Write path — helpers
        // ================================================================== //

        /// <summary>
        /// Builds an Arrow schema with (id: Int32, name: Utf8).
        /// </summary>
        private static Schema BuildIdNameSchema() => V3TestHelpers.BuildIdNameSchema();

        /// <summary>
        /// Creates a RecordBatch with the given (id, name) rows.
        /// </summary>
        private static RecordBatch BuildIdNameBatch(int[] ids, string[] names) =>
            V3TestHelpers.BuildIdNameBatch(ids, names);

        /// <summary>
        /// Wraps a single RecordBatch as an IAsyncEnumerable for InsertAsync/MergeDataAsync.
        /// </summary>
        private static IAsyncEnumerable<RecordBatch> ToAsyncEnumerable(RecordBatch batch) =>
            V3TestHelpers.ToAsyncEnumerable(batch);

        /// <summary>
        /// Reads all rows from a table and returns (ids, names) sorted by id.
        /// Handles both StringArray and StringViewArray from delta-rs.
        /// </summary>
        private static Task<List<(int id, string? name)>> ReadAllRowsSorted(
            DeltaTableServiceClient client,
            string tablePath) =>
            V3TestHelpers.ReadAllRowsSorted(client, tablePath);

        // ================================================================== //
        //  Phase 3: CreateTableAsync
        // ================================================================== //

        [TestMethod]
        public async Task V3_CreateTable_CreatesEmptyTable()
        {
            string tablePath = NewWriteTestTablePath();
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });

            ExecuteResult result = await Client.CreateTableAsync(tablePath, schema);

            Assert.IsTrue(result.Success, $"CreateTable failed: {result.Message}");
            Assert.IsTrue(result.Message.Contains("created"),
                $"Expected 'created' in message, got: {result.Message}");

            // Verify schema via GetSchema.
            TableSchema readBackSchema = await Client.GetSchemaAsync(tablePath);
            Assert.AreEqual(2, readBackSchema.Columns.Count);
            Assert.AreEqual("id", readBackSchema.Columns[0].Name);
            Assert.AreEqual("int32", readBackSchema.Columns[0].DataType);
            Assert.AreEqual("name", readBackSchema.Columns[1].Name);
            Assert.AreEqual("string", readBackSchema.Columns[1].DataType);
        }

        [TestMethod]
        public async Task V3_CreateTable_WithConfiguration()
        {
            string tablePath = NewWriteTestTablePath();
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("value", "string"),
            });
            var config = new Dictionary<string, string>
            {
                ["delta.appendOnly"] = "true",
            };

            ExecuteResult result = await Client.CreateTableAsync(tablePath, schema, configuration: config);

            Assert.IsTrue(result.Success, $"CreateTable with config failed: {result.Message}");

            // Verify table is readable.
            TableSchema readBackSchema = await Client.GetSchemaAsync(tablePath);
            Assert.AreEqual(2, readBackSchema.Columns.Count);
        }

        // ================================================================== //
        //  Phase 3: InsertAsync (DoPut write)
        // ================================================================== //

        [TestMethod]
        public async Task V3_Insert_Overwrite_WritesData()
        {
            string tablePath = NewWriteTestTablePath();

            // Create the table first.
            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });
            await Client.CreateTableAsync(tablePath, tableSchema);

            // Insert data via overwrite.
            Schema arrowSchema = BuildIdNameSchema();
            RecordBatch batch = BuildIdNameBatch(
                new[] { 10, 20, 30 },
                new[] { "ten", "twenty", "thirty" });

            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(batch), SaveMode.Overwrite);

            // Read back and verify.
            var rows = await ReadAllRowsSorted(Client, tablePath);
            Assert.AreEqual(3, rows.Count, $"Expected 3 rows, got {rows.Count}.");
            Assert.AreEqual((10, "ten"), (rows[0].id, rows[0].name));
            Assert.AreEqual((20, "twenty"), (rows[1].id, rows[1].name));
            Assert.AreEqual((30, "thirty"), (rows[2].id, rows[2].name));
        }

        [TestMethod]
        public async Task V3_Insert_Append_AddsRows()
        {
            string tablePath = NewWriteTestTablePath();

            // Create and insert initial data.
            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });
            await Client.CreateTableAsync(tablePath, tableSchema);

            Schema arrowSchema = BuildIdNameSchema();
            RecordBatch batch1 = BuildIdNameBatch(
                new[] { 1, 2 }, new[] { "a", "b" });
            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(batch1), SaveMode.Overwrite);

            // Append more rows.
            RecordBatch batch2 = BuildIdNameBatch(
                new[] { 3, 4 }, new[] { "c", "d" });
            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(batch2), SaveMode.Append);

            // Verify total: 2 + 2 = 4 rows.
            var rows = await ReadAllRowsSorted(Client, tablePath);
            Assert.AreEqual(4, rows.Count, $"Expected 4 rows after append, got {rows.Count}.");
            Assert.AreEqual(1, rows[0].id);
            Assert.AreEqual(4, rows[3].id);
        }

        [TestMethod]
        public async Task V3_Insert_Overwrite_ReplacesExistingData()
        {
            string tablePath = NewWriteTestTablePath();

            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });
            await Client.CreateTableAsync(tablePath, tableSchema);

            Schema arrowSchema = BuildIdNameSchema();

            // Write initial data.
            RecordBatch batch1 = BuildIdNameBatch(
                new[] { 1, 2, 3 }, new[] { "a", "b", "c" });
            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(batch1), SaveMode.Overwrite);

            // Overwrite with different data.
            RecordBatch batch2 = BuildIdNameBatch(
                new[] { 100, 200 }, new[] { "x", "y" });
            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(batch2), SaveMode.Overwrite);

            // Verify only the overwrite data remains.
            var rows = await ReadAllRowsSorted(Client, tablePath);
            Assert.AreEqual(2, rows.Count, $"Expected 2 rows after overwrite, got {rows.Count}.");
            Assert.AreEqual((100, "x"), (rows[0].id, rows[0].name));
            Assert.AreEqual((200, "y"), (rows[1].id, rows[1].name));
        }

        // ================================================================== //
        //  Phase 3: DeleteAsync (DoAction execute_dml)
        // ================================================================== //

        [TestMethod]
        public async Task V3_Delete_WithPredicate_RemovesMatchingRows()
        {
            string tablePath = NewWriteTestTablePath();

            // Create table and insert data: (1,a), (2,b), (3,c).
            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });
            await Client.CreateTableAsync(tablePath, tableSchema);

            Schema arrowSchema = BuildIdNameSchema();
            RecordBatch batch = BuildIdNameBatch(
                new[] { 1, 2, 3 }, new[] { "a", "b", "c" });
            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(batch), SaveMode.Overwrite);

            // Delete rows where id > 1.
            ExecuteResult deleteResult = await Client.DeleteAsync(
                "DELETE FROM tbl WHERE id > 1", tablePath, "tbl");

            Assert.IsTrue(deleteResult.Success, $"Delete failed: {deleteResult.Message}");

            // Verify only id=1 remains.
            var rows = await ReadAllRowsSorted(Client, tablePath);
            Assert.AreEqual(1, rows.Count, $"Expected 1 row after delete, got {rows.Count}.");
            Assert.AreEqual((1, "a"), (rows[0].id, rows[0].name));
        }

        [TestMethod]
        public async Task V3_Delete_AllRows_LeavesEmptyTable()
        {
            string tablePath = NewWriteTestTablePath();

            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });
            await Client.CreateTableAsync(tablePath, tableSchema);

            Schema arrowSchema = BuildIdNameSchema();
            RecordBatch batch = BuildIdNameBatch(
                new[] { 1, 2, 3 }, new[] { "a", "b", "c" });
            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(batch), SaveMode.Overwrite);

            // Delete all rows.
            ExecuteResult deleteResult = await Client.DeleteAsync(
                "DELETE FROM tbl WHERE true", tablePath, "tbl");

            Assert.IsTrue(deleteResult.Success, $"Delete all failed: {deleteResult.Message}");

            // Verify zero rows.
            var rows = await ReadAllRowsSorted(Client, tablePath);
            Assert.AreEqual(0, rows.Count, $"Expected 0 rows after delete all, got {rows.Count}.");
        }

        [TestMethod]
        public async Task V3_Delete_ReturnsMetrics()
        {
            string tablePath = NewWriteTestTablePath();

            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });
            await Client.CreateTableAsync(tablePath, tableSchema);

            Schema arrowSchema = BuildIdNameSchema();
            RecordBatch batch = BuildIdNameBatch(
                new[] { 1, 2, 3 }, new[] { "a", "b", "c" });
            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(batch), SaveMode.Overwrite);

            ExecuteResult deleteResult = await Client.DeleteAsync(
                "DELETE FROM tbl WHERE id = 2", tablePath, "tbl");

            Assert.IsTrue(deleteResult.Success);
            // Verify metrics are returned in result.
            Assert.IsTrue(deleteResult.Result.Count > 0,
                "Expected delete metrics in result.");
            Assert.IsTrue(deleteResult.Result[0].ContainsKey("num_deleted_rows"),
                "Expected 'num_deleted_rows' in delete metrics.");
        }

        // ================================================================== //
        //  Phase 3: MergeDataAsync (DoPut merge)
        // ================================================================== //

        [TestMethod]
        public async Task V3_MergeData_Upsert_UpdatesAndInserts()
        {
            string tablePath = NewWriteTestTablePath();

            // Create table and insert initial data: (1,a), (2,b), (3,c).
            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });
            await Client.CreateTableAsync(tablePath, tableSchema);

            Schema arrowSchema = BuildIdNameSchema();
            RecordBatch initialBatch = BuildIdNameBatch(
                new[] { 1, 2, 3 }, new[] { "a", "b", "c" });
            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(initialBatch), SaveMode.Overwrite);

            // Merge source: (2, "B_updated"), (4, "d_new").
            RecordBatch sourceBatch = BuildIdNameBatch(
                new[] { 2, 4 }, new[] { "B_updated", "d_new" });

            var mergeOptions = new MergeOptions(
                predicate: "target.id = source.id",
                sourceAlias: "source",
                targetAlias: "target")
            {
                WhenMatchedUpdateAll = true,
                WhenNotMatchedInsertAll = true,
            };

            ExecuteResult mergeResult = await Client.MergeDataAsync(
                tablePath, arrowSchema, ToAsyncEnumerable(sourceBatch), mergeOptions);

            Assert.IsTrue(mergeResult.Success, $"Merge failed: {mergeResult.Message}");

            // Verify: 4 rows — (1,a), (2,B_updated), (3,c), (4,d_new).
            var rows = await ReadAllRowsSorted(Client, tablePath);
            Assert.AreEqual(4, rows.Count, $"Expected 4 rows after merge, got {rows.Count}.");
            Assert.AreEqual((1, "a"), (rows[0].id, rows[0].name));
            Assert.AreEqual((2, "B_updated"), (rows[1].id, rows[1].name));
            Assert.AreEqual((3, "c"), (rows[2].id, rows[2].name));
            Assert.AreEqual((4, "d_new"), (rows[3].id, rows[3].name));
        }

        [TestMethod]
        public async Task V3_MergeData_ReturnsMetrics()
        {
            string tablePath = NewWriteTestTablePath();

            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });
            await Client.CreateTableAsync(tablePath, tableSchema);

            Schema arrowSchema = BuildIdNameSchema();
            RecordBatch initialBatch = BuildIdNameBatch(
                new[] { 1, 2 }, new[] { "a", "b" });
            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(initialBatch), SaveMode.Overwrite);

            // Merge source: (2, "B"), (3, "c").
            RecordBatch sourceBatch = BuildIdNameBatch(
                new[] { 2, 3 }, new[] { "B", "c" });

            var mergeOptions = new MergeOptions("target.id = source.id")
            {
                WhenMatchedUpdateAll = true,
                WhenNotMatchedInsertAll = true,
            };

            ExecuteResult mergeResult = await Client.MergeDataAsync(
                tablePath, arrowSchema, ToAsyncEnumerable(sourceBatch), mergeOptions);

            Assert.IsTrue(mergeResult.Success);
            Assert.IsTrue(mergeResult.Result.Count > 0,
                "Expected merge metrics in result.");
            Assert.IsTrue(mergeResult.Result[0].ContainsKey("num_source_rows"),
                "Expected 'num_source_rows' in merge metrics.");
            Assert.IsTrue(mergeResult.Result[0].ContainsKey("num_target_rows_inserted"),
                "Expected 'num_target_rows_inserted' in merge metrics.");
            Assert.IsTrue(mergeResult.Result[0].ContainsKey("num_target_rows_updated"),
                "Expected 'num_target_rows_updated' in merge metrics.");
        }

        [TestMethod]
        public async Task V3_MergeData_MatchedDelete_RemovesMatchedRows()
        {
            string tablePath = NewWriteTestTablePath();

            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });
            await Client.CreateTableAsync(tablePath, tableSchema);

            Schema arrowSchema = BuildIdNameSchema();
            RecordBatch initialBatch = BuildIdNameBatch(
                new[] { 1, 2, 3 }, new[] { "a", "b", "c" });
            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(initialBatch), SaveMode.Overwrite);

            // Merge source: id=2 — should delete the matched row.
            RecordBatch sourceBatch = BuildIdNameBatch(
                new[] { 2 }, new[] { "ignored" });

            var mergeOptions = new MergeOptions("target.id = source.id")
            {
                WhenMatchedDeletePredicate = "true",
            };

            ExecuteResult mergeResult = await Client.MergeDataAsync(
                tablePath, arrowSchema, ToAsyncEnumerable(sourceBatch), mergeOptions);

            Assert.IsTrue(mergeResult.Success, $"Merge delete failed: {mergeResult.Message}");

            // Verify: 2 rows remain — (1,a), (3,c).
            var rows = await ReadAllRowsSorted(Client, tablePath);
            Assert.AreEqual(2, rows.Count, $"Expected 2 rows after merge-delete, got {rows.Count}.");
            Assert.AreEqual((1, "a"), (rows[0].id, rows[0].name));
            Assert.AreEqual((3, "c"), (rows[1].id, rows[1].name));
        }

        // ================================================================== //
        //  Phase 3: UpgradeTableProtocolAsync
        // ================================================================== //

        [TestMethod]
        public async Task V3_UpgradeProtocol_BumpsVersions()
        {
            string tablePath = NewWriteTestTablePath();

            // Create a basic table.
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
            });
            await Client.CreateTableAsync(tablePath, schema);

            // Upgrade protocol.
            ExecuteResult result = await Client.UpgradeTableProtocolAsync(
                tablePath, readerVersion: 2, writerVersion: 5);

            Assert.IsTrue(result.Success, $"UpgradeProtocol failed: {result.Message}");
            V3TestHelpers.AssertExecuteResultContainsLong(result, "minReaderVersion", 2);
            V3TestHelpers.AssertExecuteResultContainsLong(result, "minWriterVersion", 5);
        }

        [TestMethod]
        public async Task V3_UpgradeProtocol_WithFeatures()
        {
            string tablePath = NewWriteTestTablePath();

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
            });
            await Client.CreateTableAsync(tablePath, schema);

            // Upgrade with changeDataFeed feature.
            ExecuteResult result = await Client.UpgradeTableProtocolAsync(
                tablePath,
                readerVersion: 3,
                writerVersion: 7,
                writerFeatures: new[] { "changeDataFeed" });

            Assert.IsTrue(result.Success, $"UpgradeProtocol with features failed: {result.Message}");

            V3TestHelpers.AssertExecuteResultContainsLong(result, "minWriterVersion", 7);
        }
    }
}
