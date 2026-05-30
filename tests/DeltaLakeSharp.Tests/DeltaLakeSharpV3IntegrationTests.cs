using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLakeSharp.Client;
using DeltaLakeSharp.Client.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DeltaLakeSharp.Tests
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
    public class DeltaLakeSharpV3IntegrationTests
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

        [TestMethod]
        public async Task V3_ReadTablePartition_Partitioned_CanReadPlannedPartitions()
        {
            IReadOnlyList<DeltaReadPartition> partitions = await Client.GetReadPartitionsAsync(PartitionedTablePath);
            Assert.IsTrue(partitions.Count >= 1, "Expected at least one planned partition.");

            var rows = new List<(int id, string region, string name)>();
            foreach (DeltaReadPartition partition in partitions)
            {
                await foreach (RecordBatch batch in Client.ReadTablePartitionAsync(PartitionedTablePath, partition))
                {
                    var idArray = (Int32Array)batch.Column(0);
                    IArrowArray nameArray = batch.Column(1);
                    IArrowArray regionArray = batch.Column(2);

                    for (int i = 0; i < batch.Length; i++)
                    {
                        rows.Add((
                            idArray.GetValue(i) ?? -1,
                            V3TestHelpers.ReadStringValue(regionArray, i),
                            V3TestHelpers.ReadStringValue(nameArray, i)));
                    }
                }
            }

            rows = rows.OrderBy(r => r.id).ToList();
            Assert.AreEqual(5, rows.Count, $"Expected 5 rows across planned partitions, got {rows.Count}.");
            Assert.AreEqual((1, "us", "a"), rows[0]);
            Assert.AreEqual((2, "eu", "b"), rows[1]);
            Assert.AreEqual((3, "us", "c"), rows[2]);
            Assert.AreEqual((4, "eu", "d"), rows[3]);
            Assert.AreEqual((5, "apac", "e"), rows[4]);
        }

        [TestMethod]
        public async Task V3_ReadTablePartition_NonPartitioned_CanReadPlannedPartitions()
        {
            IReadOnlyList<DeltaReadPartition> partitions = await Client.GetReadPartitionsAsync(TestTablePath);
            Assert.IsTrue(partitions.Count >= 1, "Expected at least one planned partition.");

            var rows = new List<(int id, string name)>();
            foreach (DeltaReadPartition partition in partitions)
            {
                await foreach (RecordBatch batch in Client.ReadTablePartitionAsync(TestTablePath, partition))
                {
                    var idArray = (Int32Array)batch.Column(0);
                    IArrowArray nameArray = batch.Column(1);

                    for (int i = 0; i < batch.Length; i++)
                    {
                        rows.Add((
                            idArray.GetValue(i) ?? -1,
                            V3TestHelpers.ReadStringValue(nameArray, i)));
                    }
                }
            }

            rows = rows.OrderBy(r => r.id).ToList();
            Assert.AreEqual(3, rows.Count, $"Expected 3 rows across planned partitions, got {rows.Count}.");
            Assert.AreEqual((1, "a"), rows[0]);
            Assert.AreEqual((2, "b"), rows[1]);
            Assert.AreEqual((3, "c"), rows[2]);
        }

        [TestMethod]
        public async Task V3_ReadTablePartition_NonPartitionedMultiFile_SplitsAndReadsAllRows()
        {
            string tablePath = NewWriteTestTablePath();
            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });

            ExecuteResult createResult = await Client.CreateTableAsync(tablePath, tableSchema);
            Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

            Schema arrowSchema = BuildIdNameSchema();
            for (int i = 1; i <= 8; i++)
            {
                RecordBatch batch = BuildIdNameBatch(new[] { i }, new[] { $"row_{i}" });
                await Client.InsertAsync(tablePath, arrowSchema, ToAsyncEnumerable(batch), SaveMode.Append);
            }

            IReadOnlyList<DeltaReadPartition> partitions = await Client.GetReadPartitionsAsync(tablePath);
            Assert.IsTrue(partitions.Count > 1, "Expected multiple planned partitions for multi-file non-partitioned table.");

            var rows = new List<(int id, string name)>();
            foreach (DeltaReadPartition partition in partitions)
            {
                await foreach (RecordBatch batch in Client.ReadTablePartitionAsync(tablePath, partition))
                {
                    var idArray = (Int32Array)batch.Column(0);
                    IArrowArray nameArray = batch.Column(1);

                    for (int i = 0; i < batch.Length; i++)
                    {
                        rows.Add((
                            idArray.GetValue(i) ?? -1,
                            V3TestHelpers.ReadStringValue(nameArray, i)));
                    }
                }
            }

            rows = rows.OrderBy(r => r.id).ToList();
            Assert.AreEqual(8, rows.Count, $"Expected 8 rows across planned partitions, got {rows.Count}.");
            Assert.AreEqual((1, "row_1"), rows[0]);
            Assert.AreEqual((8, "row_8"), rows[7]);
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

        [TestMethod]
        public async Task V3_ReadTablePartition_PartitionedDeletionVector_CanReadPlannedPartitions()
        {
            if (FixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(FixtureDataDir, "delta_test_partitioned_deletion_vector");
            IReadOnlyList<DeltaReadPartition> partitions = await Client.GetReadPartitionsAsync(tablePath);
            Assert.IsTrue(partitions.Count >= 1, "Expected at least one planned partition.");

            var rows = new List<(long id, string region, string value)>();
            foreach (DeltaReadPartition partition in partitions)
            {
                await foreach (RecordBatch batch in Client.ReadTablePartitionAsync(tablePath, partition))
                {
                    IArrowArray idArray = batch.Column(0);
                    IArrowArray valueArray = batch.Column(1);
                    IArrowArray regionArray = batch.Column(2);

                    for (int i = 0; i < batch.Length; i++)
                    {
                        rows.Add((
                            Convert.ToInt64(V3TestHelpers.ReadValue(idArray, i)! ),
                            V3TestHelpers.ReadStringValue(regionArray, i),
                            V3TestHelpers.ReadStringValue(valueArray, i)));
                    }
                }
            }

            rows = rows.OrderBy(r => r.id).ToList();
            Assert.AreEqual(4, rows.Count, $"Expected 4 rows across planned partitions, got {rows.Count}.");
            Assert.AreEqual((1L, "us", "one"), rows[0]);
            Assert.AreEqual((2L, "eu", "two"), rows[1]);
            Assert.AreEqual((4L, "eu", "four"), rows[2]);
            Assert.AreEqual((5L, "apac", "five"), rows[3]);
            Assert.IsFalse(rows.Any(r => r.id == 3), "id=3 should be excluded by deletion vector.");
        }

        // ================================================================== //
        //  Phase 2: Type Widening (checked-in Spark 4 / Delta 4 fixture)
        // ================================================================== //

        [TestMethod]
        public async Task V3_ReadTable_TypeWidening_ReturnsWidenedSchemaAndData()
        {
            if (FixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(FixtureDataDir, "delta_test_type_widening");

            TableSchema schema = await Client.GetSchemaAsync(tablePath);
            Assert.IsNotNull(schema, "Schema should not be null.");
            Assert.AreEqual(2, schema.Columns.Count,
                $"Expected 2 columns, got {schema.Columns.Count}.");
            Assert.AreEqual("id", schema.Columns[0].Name);
            Assert.AreEqual("int64", schema.Columns[0].DataType,
                $"Expected widened id column to be int64, got '{schema.Columns[0].DataType}'.");
            Assert.AreEqual("name", schema.Columns[1].Name);
            Assert.AreEqual("string", schema.Columns[1].DataType,
                $"Expected name column to be string, got '{schema.Columns[1].DataType}'.");

            var rows = new List<(long id, string name)>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(tablePath))
            {
                var idArray = (Int64Array)batch.Column(0);
                IArrowArray nameArray = batch.Column(1);

                for (int i = 0; i < batch.Length; i++)
                {
                    rows.Add((
                        idArray.GetValue(i)!.Value,
                        V3TestHelpers.ReadStringValue(nameArray, i)));
                }
            }

            rows = rows.OrderBy(x => x.id).ToList();
            Assert.AreEqual(2, rows.Count, $"Expected 2 rows, got {rows.Count}.");
            Assert.AreEqual((1L, "Alice"), rows[0]);
            Assert.AreEqual((2L, "Bob"), rows[1]);
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

        private static Schema BuildIdCityActiveSchema() =>
            V3TestHelpers.BuildIdCityActiveSchema();

        private static RecordBatch BuildIdCityActiveBatch(
            int[] ids,
            string[] cities,
            bool[] active) =>
            V3TestHelpers.BuildIdCityActiveBatch(ids, cities, active);

        private static Schema BuildIdRegionNameSchema() =>
            V3TestHelpers.BuildIdRegionNameSchema();

        private static RecordBatch BuildIdRegionNameBatch(
            int[] ids,
            string[] regions,
            string[] names) =>
            V3TestHelpers.BuildIdRegionNameBatch(ids, regions, names);

        private static Schema BuildIdAmountNameSchema() =>
            V3TestHelpers.BuildIdAmountNameSchema();

        private static RecordBatch BuildIdAmountNameBatch(
            int[] ids,
            int[] amounts,
            string[] names) =>
            V3TestHelpers.BuildIdAmountNameBatch(ids, amounts, names);

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

        private static void CopyDirectory(string sourceDir, string destinationDir)
        {
            Directory.CreateDirectory(destinationDir);

            foreach (string directory in Directory.GetDirectories(sourceDir, "*", SearchOption.AllDirectories))
            {
                string relativePath = GetRelativePathCompat(sourceDir, directory);
                Directory.CreateDirectory(Path.Combine(destinationDir, relativePath));
            }

            foreach (string file in Directory.GetFiles(sourceDir, "*", SearchOption.AllDirectories))
            {
                string relativePath = GetRelativePathCompat(sourceDir, file);
                string destinationFile = Path.Combine(destinationDir, relativePath);
                string? destinationFileDir = Path.GetDirectoryName(destinationFile);
                if (!string.IsNullOrEmpty(destinationFileDir))
                {
                    Directory.CreateDirectory(destinationFileDir);
                }

                File.Copy(file, destinationFile, overwrite: true);
            }
        }

        private static string GetRelativePathCompat(string basePath, string fullPath)
        {
            Uri baseUri = new Uri(AppendDirectorySeparatorChar(basePath));
            Uri fullUri = new Uri(fullPath);
            return Uri.UnescapeDataString(baseUri.MakeRelativeUri(fullUri).ToString())
                .Replace('/', Path.DirectorySeparatorChar);
        }

        private static string AppendDirectorySeparatorChar(string path)
        {
            if (!path.EndsWith(Path.DirectorySeparatorChar.ToString(), StringComparison.Ordinal)
                && !path.EndsWith(Path.AltDirectorySeparatorChar.ToString(), StringComparison.Ordinal))
            {
                return path + Path.DirectorySeparatorChar;
            }

            return path;
        }

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
        public async Task V3_Insert_Overwrite_ToMissingTable_CreatesTableAndWritesData()
        {
            string tablePath = NewWriteTestTablePath();

            Schema arrowSchema = BuildIdNameSchema();
            RecordBatch batch = BuildIdNameBatch(
                new[] { 10, 20, 30 },
                new[] { "ten", "twenty", "thirty" });

            await Client.InsertAsync(
                tablePath,
                arrowSchema,
                ToAsyncEnumerable(batch),
                SaveMode.Overwrite);

            TableSchema readBackSchema = await Client.GetSchemaAsync(tablePath);
            CollectionAssert.AreEqual(
                new[] { "id", "name" },
                readBackSchema.Columns.Select(c => c.Name).ToArray());

            var rows = await ReadAllRowsSorted(Client, tablePath);
            Assert.AreEqual(3, rows.Count, $"Expected 3 rows, got {rows.Count}.");
            Assert.AreEqual((10, "ten"), (rows[0].id, rows[0].name));
            Assert.AreEqual((20, "twenty"), (rows[1].id, rows[1].name));
            Assert.AreEqual((30, "thirty"), (rows[2].id, rows[2].name));
        }

        [TestMethod]
        public async Task V3_Insert_Append_ToMissingTable_CreatesTableAndWritesData()
        {
            string tablePath = NewWriteTestTablePath();

            Schema arrowSchema = BuildIdNameSchema();
            RecordBatch batch = BuildIdNameBatch(
                new[] { 1, 2 },
                new[] { "a", "b" });

            await Client.InsertAsync(
                tablePath,
                arrowSchema,
                ToAsyncEnumerable(batch),
                SaveMode.Append);

            var rows = await ReadAllRowsSorted(Client, tablePath);
            Assert.AreEqual(2, rows.Count, $"Expected 2 rows, got {rows.Count}.");
            Assert.AreEqual((1, "a"), (rows[0].id, rows[0].name));
            Assert.AreEqual((2, "b"), (rows[1].id, rows[1].name));
        }

        [TestMethod]
        public async Task V3_Insert_Overwrite_ToMissingPartitionedTable_CreatesPartitionedTableAndWritesData()
        {
            string tablePath = NewWriteTestTablePath();

            Schema arrowSchema = BuildIdRegionNameSchema();
            RecordBatch batch = BuildIdRegionNameBatch(
                new[] { 1, 2, 3 },
                new[] { "US", "EU", "US" },
                new[] { "Alice", "Bob", "Charlie" });

            await Client.InsertAsync(
                tablePath,
                arrowSchema,
                ToAsyncEnumerable(batch),
                SaveMode.Overwrite,
                partitionBy: new[] { "region" });

            TableSchema readBackSchema = await Client.GetSchemaAsync(tablePath);
            CollectionAssert.AreEqual(
                new[] { "id", "region", "name" },
                readBackSchema.Columns.Select(c => c.Name).ToArray());

            var rows = new List<(int id, string region, string name)>();
            await foreach (RecordBatch readBatch in Client.ReadTableAsync(tablePath))
            {
                var idArray = (Int32Array)readBatch.Column(0);
                IArrowArray regionArray = readBatch.Column(1);
                IArrowArray nameArray = readBatch.Column(2);

                for (int i = 0; i < readBatch.Length; i++)
                {
                    rows.Add((
                        idArray.GetValue(i)!.Value,
                        V3TestHelpers.ReadStringValue(regionArray, i),
                        V3TestHelpers.ReadStringValue(nameArray, i)));
                }
            }

            rows = rows.OrderBy(r => r.id).ToList();
            Assert.AreEqual(3, rows.Count, $"Expected 3 rows, got {rows.Count}.");
            Assert.AreEqual((1, "US", "Alice"), rows[0]);
            Assert.AreEqual((2, "EU", "Bob"), rows[1]);
            Assert.AreEqual((3, "US", "Charlie"), rows[2]);
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
        public async Task V3_Insert_Append_PartitionedTable_AddsRowsAcrossPartitions()
        {
            string tablePath = NewWriteTestTablePath();

            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("region", "string"),
                new ColumnDefinition("name", "string"),
            });

            ExecuteResult createResult = await Client.CreateTableAsync(
                tablePath,
                tableSchema,
                partitionBy: new[] { "region" });
            Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

            Schema arrowSchema = BuildIdRegionNameSchema();
            RecordBatch batch1 = BuildIdRegionNameBatch(
                new[] { 1, 2 },
                new[] { "US", "EU" },
                new[] { "Alice", "Bob" });
            await Client.InsertAsync(
                tablePath,
                arrowSchema,
                ToAsyncEnumerable(batch1),
                SaveMode.Overwrite);

            RecordBatch batch2 = BuildIdRegionNameBatch(
                new[] { 3, 4 },
                new[] { "US", "APAC" },
                new[] { "Charlie", "Diana" });
            await Client.InsertAsync(
                tablePath,
                arrowSchema,
                ToAsyncEnumerable(batch2),
                SaveMode.Append);

            TableSchema readBackSchema = await Client.GetSchemaAsync(tablePath);
            CollectionAssert.AreEqual(
                new[] { "id", "region", "name" },
                readBackSchema.Columns.Select(c => c.Name).ToArray());

            var rows = new List<(int id, string region, string name)>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(tablePath))
            {
                var idArray = (Int32Array)batch.Column(0);
                IArrowArray regionArray = batch.Column(1);
                IArrowArray nameArray = batch.Column(2);

                for (int i = 0; i < batch.Length; i++)
                {
                    rows.Add((
                        idArray.GetValue(i)!.Value,
                        V3TestHelpers.ReadStringValue(regionArray, i),
                        V3TestHelpers.ReadStringValue(nameArray, i)));
                }
            }

            rows = rows.OrderBy(r => r.id).ToList();
            Assert.AreEqual(4, rows.Count, $"Expected 4 rows after append, got {rows.Count}.");
            Assert.AreEqual((1, "US", "Alice"), rows[0]);
            Assert.AreEqual((2, "EU", "Bob"), rows[1]);
            Assert.AreEqual((3, "US", "Charlie"), rows[2]);
            Assert.AreEqual((4, "APAC", "Diana"), rows[3]);
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
                ToAsyncEnumerable(batch1), SaveMode.Append);

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

        [TestMethod]
        public async Task V3_Insert_Overwrite_WithSchemaModeOverwrite_ReplacesSchemaAndData()
        {
            string tablePath = NewWriteTestTablePath();

            var initialSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });
            await Client.CreateTableAsync(tablePath, initialSchema);

            Schema initialArrowSchema = BuildIdNameSchema();
            RecordBatch initialBatch = BuildIdNameBatch(
                new[] { 1, 2 },
                new[] { "alice", "bob" });
            await Client.InsertAsync(tablePath, initialArrowSchema,
                ToAsyncEnumerable(initialBatch), SaveMode.Overwrite);

            Schema replacementSchema = BuildIdCityActiveSchema();
            RecordBatch replacementBatch = BuildIdCityActiveBatch(
                new[] { 10, 20 },
                new[] { "Seattle", "Portland" },
                new[] { true, false });
            await Client.InsertAsync(
                tablePath,
                replacementSchema,
                ToAsyncEnumerable(replacementBatch),
                SaveMode.Overwrite,
                schemaMode: WriteSchemaMode.Overwrite);

            TableSchema readBackSchema = await Client.GetSchemaAsync(tablePath);
            Assert.AreEqual(3, readBackSchema.Columns.Count,
                $"Expected 3 columns after schema overwrite, got {readBackSchema.Columns.Count}.");
            CollectionAssert.AreEqual(
                new[] { "id", "city", "active" },
                readBackSchema.Columns.Select(c => c.Name).ToArray());
            CollectionAssert.AreEqual(
                new[] { "int32", "string", "boolean" },
                readBackSchema.Columns.Select(c => c.DataType).ToArray());

            var rows = new List<(int id, string city, bool active)>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(tablePath))
            {
                var idArray = (Int32Array)batch.Column(0);
                IArrowArray cityArray = batch.Column(1);
                var activeArray = (BooleanArray)batch.Column(2);

                for (int i = 0; i < batch.Length; i++)
                {
                    rows.Add((
                        idArray.GetValue(i)!.Value,
                        V3TestHelpers.ReadStringValue(cityArray, i),
                        activeArray.GetValue(i)!.Value));
                }
            }

            rows = rows.OrderBy(r => r.id).ToList();
            Assert.AreEqual(2, rows.Count, $"Expected 2 rows after schema overwrite, got {rows.Count}.");
            Assert.AreEqual((10, "Seattle", true), rows[0]);
            Assert.AreEqual((20, "Portland", false), rows[1]);
        }

        [TestMethod]
        public async Task V3_Insert_Append_WithSchemaModeMerge_AddsColumnsAndPreservesExistingRows()
        {
            string tablePath = NewWriteTestTablePath();

            var initialSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });
            await Client.CreateTableAsync(tablePath, initialSchema);

            Schema initialArrowSchema = BuildIdNameSchema();
            RecordBatch initialBatch = BuildIdNameBatch(
                new[] { 1, 2 },
                new[] { "alice", "bob" });
            await Client.InsertAsync(
                tablePath,
                initialArrowSchema,
                ToAsyncEnumerable(initialBatch),
                SaveMode.Append);

            Schema mergedSchema = BuildIdCityActiveSchema();
            RecordBatch mergedBatch = BuildIdCityActiveBatch(
                new[] { 3, 4 },
                new[] { "Seattle", "Portland" },
                new[] { true, false });
            await Client.InsertAsync(
                tablePath,
                mergedSchema,
                ToAsyncEnumerable(mergedBatch),
                SaveMode.Append,
                schemaMode: WriteSchemaMode.Merge);

            TableSchema readBackSchema = await Client.GetSchemaAsync(tablePath);
            Assert.AreEqual(4, readBackSchema.Columns.Count,
                $"Expected 4 columns after schema merge, got {readBackSchema.Columns.Count}.");
            CollectionAssert.AreEqual(
                new[] { "id", "name", "city", "active" },
                readBackSchema.Columns.Select(c => c.Name).ToArray());
            CollectionAssert.AreEqual(
                new[] { "int32", "string", "string", "boolean" },
                readBackSchema.Columns.Select(c => c.DataType).ToArray());

            var rows = new List<(int id, string name, string city, bool? active)>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(tablePath))
            {
                var idArray = (Int32Array)batch.Column(0);
                IArrowArray nameArray = batch.Column(1);
                IArrowArray cityArray = batch.Column(2);
                var activeArray = (BooleanArray)batch.Column(3);

                for (int i = 0; i < batch.Length; i++)
                {
                    rows.Add((
                        idArray.GetValue(i)!.Value,
                        V3TestHelpers.ReadStringValue(nameArray, i),
                        V3TestHelpers.ReadStringValue(cityArray, i),
                        activeArray.GetValue(i)));
                }
            }

            rows = rows.OrderBy(r => r.id).ToList();
            Assert.AreEqual(4, rows.Count, $"Expected 4 rows after schema merge, got {rows.Count}.");
            Assert.AreEqual((1, "alice", string.Empty, (bool?)null), rows[0]);
            Assert.AreEqual((2, "bob", string.Empty, (bool?)null), rows[1]);
            Assert.AreEqual((3, string.Empty, "Seattle", (bool?)true), rows[2]);
            Assert.AreEqual((4, string.Empty, "Portland", (bool?)false), rows[3]);
        }

        [TestMethod]
        public async Task V3_Insert_Overwrite_WithNewSchemaWithoutSchemaMode_FailsAndPreservesExistingTable()
        {
            string tablePath = NewWriteTestTablePath();

            var initialSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });
            await Client.CreateTableAsync(tablePath, initialSchema);

            Schema initialArrowSchema = BuildIdNameSchema();
            RecordBatch initialBatch = BuildIdNameBatch(
                new[] { 1, 2 },
                new[] { "alice", "bob" });
            await Client.InsertAsync(tablePath, initialArrowSchema,
                ToAsyncEnumerable(initialBatch), SaveMode.Overwrite);

            Schema replacementSchema = BuildIdCityActiveSchema();
            RecordBatch replacementBatch = BuildIdCityActiveBatch(
                new[] { 10, 20 },
                new[] { "Seattle", "Portland" },
                new[] { true, false });

            var ex = await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
            {
                await Client.InsertAsync(
                    tablePath,
                    replacementSchema,
                    ToAsyncEnumerable(replacementBatch),
                    SaveMode.Overwrite);
            });

            V3TestHelpers.AssertNativeFailure(ex);

            TableSchema readBackSchema = await Client.GetSchemaAsync(tablePath);
            CollectionAssert.AreEqual(
                new[] { "id", "name" },
                readBackSchema.Columns.Select(c => c.Name).ToArray());

            var rows = await ReadAllRowsSorted(Client, tablePath);
            Assert.AreEqual(2, rows.Count, $"Expected original 2 rows to remain, got {rows.Count}.");
            Assert.AreEqual((1, "alice"), (rows[0].id, rows[0].name));
            Assert.AreEqual((2, "bob"), (rows[1].id, rows[1].name));
        }

        [TestMethod]
        public async Task V3_CreateTable_WithTypeWideningConfiguration_FailsBecauseDeltaRsWritePathLacksSupport()
        {
            string tablePath = NewWriteTestTablePath();

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });

            var ex = await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
            {
                await Client.CreateTableAsync(
                    tablePath,
                    schema,
                    configuration: new Dictionary<string, string>
                    {
                        ["delta.enableTypeWidening"] = "true",
                    });
            });

            V3TestHelpers.AssertNativeFailure(ex);
            Assert.IsTrue(
                ex.Message.Contains("delta.enableTypeWidening", StringComparison.OrdinalIgnoreCase)
                || ex.Message.Contains("typewidening", StringComparison.OrdinalIgnoreCase)
                || ex.Message.Contains("Error parsing property", StringComparison.OrdinalIgnoreCase),
                $"Expected failure to mention unsupported type widening configuration. Actual: {ex.Message}");

            Assert.IsFalse(Directory.Exists(Path.Combine(tablePath, "_delta_log")),
                "Table should not be created when type widening configuration is rejected.");
        }

        [TestMethod]
        public async Task V3_Insert_Append_WithWidenedSchema_DoesNotUpgradeExistingSchema()
        {
            string tablePath = NewWriteTestTablePath();

            var initialSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });
            ExecuteResult createResult = await Client.CreateTableAsync(tablePath, initialSchema);
            Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

            Schema initialArrowSchema = BuildIdNameSchema();
            RecordBatch initialBatch = BuildIdNameBatch(
                new[] { 1, 2 },
                new[] { "alice", "bob" });
            await Client.InsertAsync(
                tablePath,
                initialArrowSchema,
                ToAsyncEnumerable(initialBatch),
                SaveMode.Append);

            Schema widenedArrowSchema = new Schema.Builder()
                .Field(new Field("id", Int64Type.Default, nullable: true))
                .Field(new Field("name", StringType.Default, nullable: true))
                .Build();
            RecordBatch widenedBatch = new RecordBatch.Builder()
                .Append("id", nullable: true, new Int64Array.Builder().AppendRange(new long[] { 3L, 4L }).Build())
                .Append("name", nullable: true, new StringArray.Builder().AppendRange(new[] { "carol", "david" }).Build())
                .Build();

            await Client.InsertAsync(
                tablePath,
                widenedArrowSchema,
                ToAsyncEnumerable(widenedBatch),
                SaveMode.Append,
                schemaMode: (WriteSchemaMode)0);

            TableSchema readBackSchema = await Client.GetSchemaAsync(tablePath);
            CollectionAssert.AreEqual(
                new[] { "id", "name" },
                readBackSchema.Columns.Select(c => c.Name).ToArray());
            CollectionAssert.AreEqual(
                new[] { "int32", "string" },
                readBackSchema.Columns.Select(c => c.DataType).ToArray(),
                $"Expected delta-rs write path to leave schema unchanged. Actual: {string.Join(", ", readBackSchema.Columns.Select(c => c.DataType))}");

            var rows = new List<(int id, string name)>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(tablePath))
            {
                var idArray = (Int32Array)batch.Column(0);
                IArrowArray nameArray = batch.Column(1);

                for (int i = 0; i < batch.Length; i++)
                {
                    rows.Add((
                        idArray.GetValue(i)!.Value,
                        V3TestHelpers.ReadStringValue(nameArray, i)));
                }
            }

            rows = rows.OrderBy(r => r.id).ToList();
            Assert.AreEqual(4, rows.Count, $"Expected 4 rows after widened append, got {rows.Count}.");
            Assert.AreEqual((1, "alice"), rows[0]);
            Assert.AreEqual((2, "bob"), rows[1]);
            Assert.AreEqual((3, "carol"), rows[2]);
            Assert.AreEqual((4, "david"), rows[3]);
        }

        [TestMethod]
        public async Task V3_ReadTable_V2CheckpointFixture_ReadsBackSuccessfully()
        {
            if (FixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(FixtureDataDir, "delta_test_v2_checkpoint");

            string deltaLogPath = Path.Combine(tablePath, "_delta_log");
            string lastCheckpointPath = Path.Combine(deltaLogPath, "_last_checkpoint");
            Assert.IsTrue(File.Exists(lastCheckpointPath),
                $"Expected checked-in fixture to contain _last_checkpoint: {lastCheckpointPath}");

            string[] checkpointArtifacts = Directory.GetFiles(deltaLogPath, "*.checkpoint*", SearchOption.TopDirectoryOnly);
            Assert.IsTrue(checkpointArtifacts.Length > 0,
                $"Expected checked-in fixture to contain checkpoint artifacts in {deltaLogPath}.");

            string sidecarDir = Path.Combine(deltaLogPath, "_sidecars");
            Assert.IsTrue(Directory.Exists(sidecarDir),
                $"Expected checked-in fixture to contain V2 checkpoint sidecars: {sidecarDir}");

            var rows = await ReadAllRowsSorted(Client, tablePath);
            Assert.AreEqual(11, rows.Count, $"Expected 11 rows in the V2 checkpoint fixture, got {rows.Count}.");

            for (int id = 1; id <= 11; id++)
            {
                Assert.AreEqual((id, $"name_{id}"), (rows[id - 1].id, rows[id - 1].name));
            }
        }

        [TestMethod]
        public async Task V3_Insert_Append_ExistingTypeWideningTable_FailsBecauseTypeWideningFeatureIsUnsupported()
        {
            if (FixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string sourceTablePath = Path.Combine(FixtureDataDir, "delta_test_type_widening");
            string tablePath = NewWriteTestTablePath();
            CopyDirectory(sourceTablePath, tablePath);

            Schema widenedArrowSchema = new Schema.Builder()
                .Field(new Field("id", Int64Type.Default, nullable: true))
                .Field(new Field("name", StringType.Default, nullable: true))
                .Build();
            RecordBatch widenedBatch = new RecordBatch.Builder()
                .Append("id", nullable: true, new Int64Array.Builder().AppendRange(new long[] { 3L, 4L }).Build())
                .Append("name", nullable: true, new StringArray.Builder().AppendRange(new[] { "carol", "david" }).Build())
                .Build();

            var ex = await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
            {
                await Client.InsertAsync(
                    tablePath,
                    widenedArrowSchema,
                    ToAsyncEnumerable(widenedBatch),
                    SaveMode.Append);
            });

            V3TestHelpers.AssertNativeFailure(ex);
            Assert.IsTrue(
                ex.Message.Contains("Unsupported table features required", StringComparison.OrdinalIgnoreCase)
                || ex.Message.Contains("TypeWidening", StringComparison.OrdinalIgnoreCase),
                $"Expected failure to mention unsupported TypeWidening feature. Actual: {ex.Message}");

            TableSchema readBackSchema = await Client.GetSchemaAsync(tablePath);
            CollectionAssert.AreEqual(
                new[] { "id", "name" },
                readBackSchema.Columns.Select(c => c.Name).ToArray());
            CollectionAssert.AreEqual(
                new[] { "int64", "string" },
                readBackSchema.Columns.Select(c => c.DataType).ToArray(),
                $"Expected type-widened fixture schema to remain int64/string. Actual: {string.Join(", ", readBackSchema.Columns.Select(c => c.DataType))}");

            var rows = new List<(long id, string name)>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(tablePath))
            {
                var idArray = (Int64Array)batch.Column(0);
                IArrowArray nameArray = batch.Column(1);

                for (int i = 0; i < batch.Length; i++)
                {
                    rows.Add((
                        idArray.GetValue(i)!.Value,
                        V3TestHelpers.ReadStringValue(nameArray, i)));
                }
            }

            rows = rows.OrderBy(r => r.id).ToList();
            Assert.AreEqual(2, rows.Count, $"Expected append to be rejected and original 2 rows to remain, got {rows.Count}.");
            Assert.AreEqual((1L, "Alice"), rows[0]);
            Assert.AreEqual((2L, "Bob"), rows[1]);
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

        [TestMethod]
        public async Task V3_MergeData_PartitionedTable_UpdatesAndInsertsRows()
        {
            string tablePath = NewWriteTestTablePath();

            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("region", "string"),
                new ColumnDefinition("name", "string"),
            });

            ExecuteResult createResult = await Client.CreateTableAsync(
                tablePath,
                tableSchema,
                partitionBy: new[] { "region" });
            Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

            Schema arrowSchema = BuildIdRegionNameSchema();
            RecordBatch initialBatch = BuildIdRegionNameBatch(
                new[] { 1, 2, 3 },
                new[] { "US", "EU", "US" },
                new[] { "Alice", "Bob", "Charlie" });
            await Client.InsertAsync(
                tablePath,
                arrowSchema,
                ToAsyncEnumerable(initialBatch),
                SaveMode.Overwrite);

            RecordBatch sourceBatch = BuildIdRegionNameBatch(
                new[] { 2, 4 },
                new[] { "EU", "APAC" },
                new[] { "Bobby", "Diana" });

            var mergeOptions = new MergeOptions("target.id = source.id")
            {
                WhenMatchedUpdateAll = true,
                WhenNotMatchedInsertAll = true,
            };

            ExecuteResult mergeResult = await Client.MergeDataAsync(
                tablePath,
                arrowSchema,
                ToAsyncEnumerable(sourceBatch),
                mergeOptions);

            Assert.IsTrue(mergeResult.Success, $"Merge failed: {mergeResult.Message}");

            var rows = new List<(int id, string region, string name)>();
            await foreach (RecordBatch batch in Client.ReadTableAsync(tablePath))
            {
                var idArray = (Int32Array)batch.Column(0);
                IArrowArray regionArray = batch.Column(1);
                IArrowArray nameArray = batch.Column(2);

                for (int i = 0; i < batch.Length; i++)
                {
                    rows.Add((
                        idArray.GetValue(i)!.Value,
                        V3TestHelpers.ReadStringValue(regionArray, i),
                        V3TestHelpers.ReadStringValue(nameArray, i)));
                }
            }

            rows = rows.OrderBy(r => r.id).ToList();
            Assert.AreEqual(4, rows.Count, $"Expected 4 rows after merge, got {rows.Count}.");
            Assert.AreEqual((1, "US", "Alice"), rows[0]);
            Assert.AreEqual((2, "EU", "Bobby"), rows[1]);
            Assert.AreEqual((3, "US", "Charlie"), rows[2]);
            Assert.AreEqual((4, "APAC", "Diana"), rows[3]);
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

        [TestMethod]
        public async Task V3_ReadChangeData_ReturnsExpectedChanges()
        {
            string tablePath = NewWriteTestTablePath();

            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });

            ExecuteResult createResult = await Client.CreateTableAsync(
                tablePath,
                tableSchema,
                configuration: new Dictionary<string, string>
                {
                    ["delta.enableChangeDataFeed"] = "true",
                });
            Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

            Schema arrowSchema = BuildIdNameSchema();
            RecordBatch initialBatch = BuildIdNameBatch(
                new[] { 1, 2 },
                new[] { "a", "b" });
            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(initialBatch), SaveMode.Append);

            ExecuteResult updateResult = await Client.UpdateAsync(
                "UPDATE tbl SET name = 'b2' WHERE id = 2",
                tablePath,
                "tbl");
            Assert.IsTrue(updateResult.Success, $"UpdateAsync failed: {updateResult.Message}");

            ExecuteResult deleteResult = await Client.DeleteAsync(
                "DELETE FROM tbl WHERE id = 1",
                tablePath,
                "tbl");
            Assert.IsTrue(deleteResult.Success, $"DeleteAsync failed: {deleteResult.Message}");

            RecordBatch appendBatch = BuildIdNameBatch(
                new[] { 3 },
                new[] { "c" });
            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(appendBatch), SaveMode.Append);

            List<Dictionary<string, object?>> cdfRows = await V3TestHelpers.ReadAllChangeDataRowsAsync(
                Client,
                tablePath,
                startingVersion: 1);

            Assert.IsTrue(cdfRows.Count >= 5, $"Expected multiple CDF rows, got {cdfRows.Count}.");
            Assert.IsTrue(cdfRows.All(r => r.ContainsKey("_change_type")), "Expected _change_type column in all CDF rows.");
            Assert.IsTrue(cdfRows.All(r => r.ContainsKey("_commit_version")), "Expected _commit_version column in all CDF rows.");
            Assert.IsTrue(cdfRows.All(r => r.ContainsKey("_commit_timestamp")), "Expected _commit_timestamp column in all CDF rows.");

            CollectionAssert.Contains(cdfRows.Select(r => r["_change_type"]?.ToString()).ToList(), "insert");
            CollectionAssert.Contains(cdfRows.Select(r => r["_change_type"]?.ToString()).ToList(), "delete");
            CollectionAssert.Contains(cdfRows.Select(r => r["_change_type"]?.ToString()).ToList(), "update_preimage");
            CollectionAssert.Contains(cdfRows.Select(r => r["_change_type"]?.ToString()).ToList(), "update_postimage");

            Assert.IsTrue(cdfRows.Any(r => Equals(r["id"], 1) && Equals(r["name"], "a") && Equals(r["_change_type"], "insert")),
                "Expected insert CDF row for id=1.");
            Assert.IsTrue(cdfRows.Any(r => Equals(r["id"], 2) && Equals(r["name"], "b") && Equals(r["_change_type"], "insert")),
                "Expected insert CDF row for id=2.");
            Assert.IsTrue(cdfRows.Any(r => Equals(r["id"], 1) && Equals(r["_change_type"], "delete")),
                "Expected delete CDF row for id=1.");
            Assert.IsTrue(cdfRows.Any(r => Equals(r["id"], 2) && Equals(r["name"], "b") && Equals(r["_change_type"], "update_preimage")),
                "Expected update_preimage CDF row for id=2.");
            Assert.IsTrue(cdfRows.Any(r => Equals(r["id"], 2) && Equals(r["name"], "b2") && Equals(r["_change_type"], "update_postimage")),
                "Expected update_postimage CDF row for id=2.");
            Assert.IsTrue(cdfRows.Any(r => Equals(r["id"], 3) && Equals(r["name"], "c") && Equals(r["_change_type"], "insert")),
                "Expected insert CDF row for id=3.");
        }

        [TestMethod]
        public async Task V3_ExecuteChangeDataQuery_ReturnsProjectedFilteredRows()
        {
            string tablePath = NewWriteTestTablePath();

            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });

            ExecuteResult createResult = await Client.CreateTableAsync(
                tablePath,
                tableSchema,
                configuration: new Dictionary<string, string>
                {
                    ["delta.enableChangeDataFeed"] = "true",
                });
            Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

            Schema arrowSchema = BuildIdNameSchema();
            RecordBatch initialBatch = BuildIdNameBatch(
                new[] { 1, 2 },
                new[] { "a", "b" });
            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(initialBatch), SaveMode.Append);

            ExecuteResult updateResult = await Client.UpdateAsync(
                "UPDATE tbl SET name = 'b2' WHERE id = 2",
                tablePath,
                "tbl");
            Assert.IsTrue(updateResult.Success, $"UpdateAsync failed: {updateResult.Message}");

            ExecuteResult deleteResult = await Client.DeleteAsync(
                "DELETE FROM tbl WHERE id = 1",
                tablePath,
                "tbl");
            Assert.IsTrue(deleteResult.Success, $"DeleteAsync failed: {deleteResult.Message}");

            List<Dictionary<string, object?>> rows = await V3TestHelpers.ExecuteChangeDataQueryRowsAsync(
                Client,
                "SELECT id, name, _change_type, _commit_version FROM _cdf WHERE _change_type <> 'update_preimage' ORDER BY _commit_version, id, _change_type",
                tablePath,
                startingVersion: 1);

            Assert.IsTrue(rows.Count >= 4, $"Expected at least 4 filtered CDF rows, got {rows.Count}.");
            Assert.IsTrue(rows.All(r => !r.ContainsKey("_commit_timestamp")), "Expected projected query to omit unselected columns.");
            Assert.IsTrue(rows.All(r => r.ContainsKey("id") && r.ContainsKey("name") && r.ContainsKey("_change_type") && r.ContainsKey("_commit_version")),
                "Expected projected columns in every row.");
            Assert.IsFalse(rows.Any(r => Equals(r["_change_type"], "update_preimage")), "Expected query filter to exclude update_preimage rows.");

            Assert.IsTrue(rows.Any(r => Equals(r["id"], 1) && Equals(r["name"], "a") && Equals(r["_change_type"], "insert")),
                "Expected initial insert row for id=1.");
            Assert.IsTrue(rows.Any(r => Equals(r["id"], 2) && Equals(r["name"], "b2") && Equals(r["_change_type"], "update_postimage")),
                "Expected update_postimage row for id=2.");
            Assert.IsTrue(rows.Any(r => Equals(r["id"], 1) && Equals(r["_change_type"], "delete")),
                "Expected delete row for id=1.");
        }

        [TestMethod]
        public async Task V3_ExecuteChangeDataQuery_RangeFiltersCustomColumn()
        {
            string tablePath = NewWriteTestTablePath();

            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("amount", "int32"),
                new ColumnDefinition("name", "string"),
            });

            ExecuteResult createResult = await Client.CreateTableAsync(
                tablePath,
                tableSchema,
                configuration: new Dictionary<string, string>
                {
                    ["delta.enableChangeDataFeed"] = "true",
                });
            Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

            Schema arrowSchema = BuildIdAmountNameSchema();
            RecordBatch initialBatch = BuildIdAmountNameBatch(
                new[] { 1, 2, 3 },
                new[] { 10, 40, 90 },
                new[] { "low", "mid", "high" });
            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(initialBatch), SaveMode.Append);

            ExecuteResult updateResult = await Client.UpdateAsync(
                "UPDATE tbl SET amount = 55 WHERE id = 2",
                tablePath,
                "tbl");
            Assert.IsTrue(updateResult.Success, $"UpdateAsync failed: {updateResult.Message}");

            RecordBatch appendBatch = BuildIdAmountNameBatch(
                new[] { 4 },
                new[] { 65 },
                new[] { "upper-mid" });
            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(appendBatch), SaveMode.Append);

            List<Dictionary<string, object?>> rows = await V3TestHelpers.ExecuteChangeDataQueryRowsAsync(
                Client,
                "SELECT id, amount, name, _change_type FROM _cdf WHERE amount BETWEEN 20 AND 80 AND _change_type <> 'update_preimage' ORDER BY amount, id",
                tablePath,
                startingVersion: 1);

            Assert.AreEqual(3, rows.Count, $"Expected 3 CDF rows in the amount range, got {rows.Count}.");
            CollectionAssert.AreEqual(new[] { 40, 55, 65 }, rows.Select(r => Convert.ToInt32(r["amount"])).ToArray());
            Assert.IsTrue(rows.Any(r => Equals(r["id"], 2) && Equals(r["amount"], 40) && Equals(r["_change_type"], "insert")),
                "Expected initial insert row for id=2 in range.");
            Assert.IsTrue(rows.Any(r => Equals(r["id"], 2) && Equals(r["amount"], 55) && Equals(r["_change_type"], "update_postimage")),
                "Expected update_postimage row for id=2 in range.");
            Assert.IsTrue(rows.Any(r => Equals(r["id"], 4) && Equals(r["amount"], 65) && Equals(r["_change_type"], "insert")),
                "Expected appended insert row for id=4 in range.");
        }

        [TestMethod]
        public async Task V3_ExecuteChangeDataQuery_InvalidSql_Throws()
        {
            string tablePath = NewWriteTestTablePath();

            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });

            ExecuteResult createResult = await Client.CreateTableAsync(
                tablePath,
                tableSchema,
                configuration: new Dictionary<string, string>
                {
                    ["delta.enableChangeDataFeed"] = "true",
                });
            Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

            Schema arrowSchema = BuildIdNameSchema();
            RecordBatch initialBatch = BuildIdNameBatch(
                new[] { 1 },
                new[] { "a" });
            await Client.InsertAsync(tablePath, arrowSchema,
                ToAsyncEnumerable(initialBatch), SaveMode.Append);

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
            {
                await Client.ExecuteChangeDataQueryAsync(
                        "SELECT definitely_missing FROM _cdf",
                        tablePath,
                        startingVersion: 1)
                    .ToListAsync();
            });
        }
    }
}
