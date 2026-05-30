using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DeltaLakeSharp.Client;
using DeltaLakeSharp.Client.Models;
using DeltaLakeSharp.Testing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DeltaLakeSharp.Tests
{
    /// <summary>
    /// Integration tests for the Delta Table Service.
    /// These tests require Docker to be running and will build/start the
    /// Delta Table Service container, then exercise the full round-trip
    /// through the C# client → Arrow Flight → PySpark → Delta Lake pipeline.
    ///
    /// Run with: dotnet test --filter TestCategory=Integration
    /// </summary>
    [TestClass]
    [TestCategory("Integration")]
    public class DeltaLakeSharpV1IntegrationTests
    {
        private static DeltaTableContainer? _container;
        private static DeltaTableServiceClient? _client;

        /// <summary>
        /// Path to the DeltaLakeSharp server directory containing the Dockerfile,
        /// relative to the test project output directory. Adjust if needed.
        /// </summary>
        private static readonly string DockerfilePath = GetDockerfilePath();

        /// <summary>
        /// Builds and starts the Docker container once for all integration tests.
        /// </summary>
        [ClassInitialize]
        public static async Task ClassInitialize(TestContext context)
        {
            FlightIntegrationTestGuards.EnsureArrowFlightSupported();

            _container = new DeltaTableContainer();
            await _container.BuildAndStartAsync(
                dockerfilePath: DockerfilePath,
                imageName: "delta-table-service:integration-test");

            _client = new DeltaTableServiceClient(_container.GetFlightUri());

            // Wait for the server to become healthy.
            bool healthy = false;
            for (int attempt = 0; attempt < 30; attempt++)
            {
                healthy = await _client.HealthCheckAsync();
                if (healthy) break;
                await Task.Delay(2000);
            }

            Assert.IsTrue(healthy, "Delta Table Service did not become healthy within timeout.");
        }

        /// <summary>
        /// Stops and disposes the Docker container after all integration tests.
        /// </summary>
        [ClassCleanup]
        public static async Task ClassCleanup()
        {
            _client?.Dispose();
            if (_container != null)
            {
                await _container.DisposeAsync();
            }
        }

        // ================================================================== //
        //  Health check
        // ================================================================== //

        [TestMethod]
        public async Task HealthCheck_ReturnsTrue()
        {
            bool healthy = await _client!.HealthCheckAsync();
            Assert.IsTrue(healthy, "Expected the server to report healthy.");
        }

        // ================================================================== //
        //  Create table (empty with schema) + Get schema
        // ================================================================== //

        [TestMethod]
        public async Task CreateEmptyTable_GetSchema_ReturnsCorrectColumns()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int"),
                new ColumnDefinition("name", "string"),
                new ColumnDefinition("value", "double"),
            });

            ExecuteResult createResult = await _client!.CreateTableAsync(tablePath, schema);
            Assert.IsTrue(createResult.Success, $"CreateTable failed: {createResult.Message}");

            TableSchema readSchema = await _client.GetSchemaAsync(tablePath);
            Assert.AreEqual(3, readSchema.Columns.Count);
            Assert.AreEqual("id", readSchema.Columns[0].Name);
            Assert.AreEqual("name", readSchema.Columns[1].Name);
            Assert.AreEqual("value", readSchema.Columns[2].Name);

            // Assert data types
            string idType = readSchema.Columns[0].DataType.ToLowerInvariant();
            Assert.IsTrue(idType.Contains("int"),
                $"Expected id column to be integer type, got '{idType}'.");

            string nameType = readSchema.Columns[1].DataType.ToLowerInvariant();
            Assert.IsTrue(nameType.Contains("utf8") || nameType.Contains("string"),
                $"Expected name column to be string/utf8 type, got '{nameType}'.");

            string valueType = readSchema.Columns[2].DataType.ToLowerInvariant();
            Assert.IsTrue(valueType.Contains("double") || valueType.Contains("float"),
                $"Expected value column to be double/float type, got '{valueType}'.");
        }

        // ================================================================== //
        //  Create table with object[][] data + Read table
        // ================================================================== //

        [TestMethod]
        public async Task CreateTableWithRows_ReadBack_DataMatches()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });

            var rows = new[]
            {
                new object[] { 1, "Alice" },
                new object[] { 2, "Bob" },
                new object[] { 3, "Charlie" },
            };

            var batch = ArrowConverter.FromRows(rows, schema);
            await _client!.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch));

            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();

            Assert.AreEqual(3, dt.Rows.Count);
            Assert.AreEqual(2, dt.Columns.Count);

            // Verify data (order may vary in Spark, so key by name)
            var rowsByName = new Dictionary<string, DataRow>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string name = dt.Rows[i]["name"]?.ToString() ?? "";
                rowsByName[name] = dt.Rows[i];
            }

            Assert.IsTrue(rowsByName.ContainsKey("Alice"), "Missing Alice");
            Assert.IsTrue(rowsByName.ContainsKey("Bob"), "Missing Bob");
            Assert.IsTrue(rowsByName.ContainsKey("Charlie"), "Missing Charlie");

            // Assert id↔name pairings (int32 widens to int64 through PySpark)
            Assert.AreEqual(1L, Convert.ToInt64(rowsByName["Alice"]["id"]), "Alice should have id=1");
            Assert.AreEqual(2L, Convert.ToInt64(rowsByName["Bob"]["id"]), "Bob should have id=2");
            Assert.AreEqual(3L, Convert.ToInt64(rowsByName["Charlie"]["id"]), "Charlie should have id=3");
        }

        // ================================================================== //
        //  Create table from DataTable + Read back
        // ================================================================== //

        [TestMethod]
        public async Task CreateTableFromDataTable_ReadBack_RoundTrips()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var source = new DataTable();
            source.Columns.Add("product", typeof(string));
            source.Columns.Add("quantity", typeof(int));
            source.Rows.Add("Widget", 100);
            source.Rows.Add("Gadget", 200);

            var batch = ArrowConverter.FromDataTable(source);
            await _client!.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch));

            DataTable result = await _client.ReadTableAsync(tablePath).ToDataTableAsync();

            Assert.AreEqual(2, result.Rows.Count);

            var rowsByProduct = new Dictionary<string, DataRow>();
            for (int i = 0; i < result.Rows.Count; i++)
            {
                string product = result.Rows[i]["product"]?.ToString() ?? "";
                rowsByProduct[product] = result.Rows[i];
            }

            Assert.IsTrue(rowsByProduct.ContainsKey("Widget"), "Missing Widget");
            Assert.IsTrue(rowsByProduct.ContainsKey("Gadget"), "Missing Gadget");

            // Assert product↔quantity pairings (int widens to int64 through PySpark)
            Assert.AreEqual(100L, Convert.ToInt64(rowsByProduct["Widget"]["quantity"]), "Widget should have quantity=100");
            Assert.AreEqual(200L, Convert.ToInt64(rowsByProduct["Gadget"]["quantity"]), "Gadget should have quantity=200");
        }

        // ================================================================== //
        //  Create table from CSV + Read back
        // ================================================================== //

        [TestMethod]
        public async Task CreateTableFromCsv_ReadBack_DataMatches()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            string csv = "city,state\nSeattle,WA\nPortland,OR\nSan Francisco,CA";

            var batch = ArrowConverter.FromCsv(csv);
            await _client!.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch));

            DataTable result = await _client.ReadTableAsync(tablePath).ToDataTableAsync();

            Assert.AreEqual(3, result.Rows.Count);
            Assert.AreEqual(2, result.Columns.Count);

            var rowsByCity = new Dictionary<string, DataRow>();
            for (int i = 0; i < result.Rows.Count; i++)
            {
                string city = result.Rows[i]["city"]?.ToString() ?? "";
                rowsByCity[city] = result.Rows[i];
            }

            Assert.IsTrue(rowsByCity.ContainsKey("Seattle"), "Missing Seattle");
            Assert.IsTrue(rowsByCity.ContainsKey("Portland"), "Missing Portland");
            Assert.IsTrue(rowsByCity.ContainsKey("San Francisco"), "Missing San Francisco");

            // Assert city↔state pairings
            Assert.AreEqual("WA", rowsByCity["Seattle"]["state"]?.ToString(), "Seattle should be in WA");
            Assert.AreEqual("OR", rowsByCity["Portland"]["state"]?.ToString(), "Portland should be in OR");
            Assert.AreEqual("CA", rowsByCity["San Francisco"]["state"]?.ToString(), "San Francisco should be in CA");
        }

        // ================================================================== //
        //  Execute SQL
        // ================================================================== //

        [TestMethod]
        public async Task ExecuteSql_ShowDatabases_Succeeds()
        {
            List<Apache.Arrow.RecordBatch> batches =
                await _client!.ExecuteQueryAsync("SHOW DATABASES").ToListAsync();

            Assert.IsTrue(batches.Count > 0, "SHOW DATABASES should return at least one RecordBatch.");

            // Find the 'namespace' column and check for 'default'
            bool hasDefault = false;
            foreach (var batch in batches)
            {
                int nsIdx = batch.Schema.GetFieldIndex("namespace");
                Assert.IsTrue(nsIdx >= 0, "Result schema should contain 'namespace' column.");
                var nsArray = batch.Column(nsIdx) as Apache.Arrow.StringArray;
                Assert.IsNotNull(nsArray, "Expected 'namespace' column to be a StringArray.");
                for (int i = 0; i < nsArray!.Length; i++)
                {
                    if (string.Equals(nsArray.GetString(i), "default", StringComparison.OrdinalIgnoreCase))
                    {
                        hasDefault = true;
                        break;
                    }
                }
                if (hasDefault) break;
            }
            Assert.IsTrue(hasDefault, "SHOW DATABASES should include the 'default' database.");
        }

        [TestMethod]
        public async Task ExecuteSql_SelectFromCreatedTable_ReturnsRows()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int"),
                new ColumnDefinition("value", "string"),
            });

            var rows = new[]
            {
                new object[] { 1, "one" },
                new object[] { 2, "two" },
            };

            var batch = ArrowConverter.FromRows(rows, schema);
            await _client!.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch));

            // Register the Delta table as a temp view so we can query it via SQL.
            string viewName = $"test_view_{Guid.NewGuid():N}";
            // DDL — just consume the stream; if the server returned !success, the backend already threw.
            await _client.ExecuteQueryAsync(
                $"CREATE OR REPLACE TEMPORARY VIEW {viewName} USING delta OPTIONS (path '{tablePath}')").ToListAsync();

            List<Apache.Arrow.RecordBatch> batches = await _client.ExecuteQueryAsync(
                $"SELECT COUNT(*) AS cnt FROM {viewName}").ToListAsync();

            Assert.IsTrue(batches.Count > 0, "SELECT COUNT(*) should return at least one RecordBatch.");

            Apache.Arrow.RecordBatch firstBatch = batches[0];
            int cntIdx = firstBatch.Schema.GetFieldIndex("cnt");
            Assert.IsTrue(cntIdx >= 0, "Result schema should contain 'cnt' column.");

            var cntArray = firstBatch.Column(cntIdx) as Apache.Arrow.Int64Array;
            Assert.IsNotNull(cntArray, $"Expected 'cnt' column to be Int64Array, got {firstBatch.Column(cntIdx).GetType().Name}.");
            Assert.AreEqual(2L, cntArray!.GetValue(0), "COUNT(*) should be 2.");
        }

        // ================================================================== //
        //  Read table — all rows returned
        // ================================================================== //

        [TestMethod]
        public async Task ReadTable_ReturnsAllRows()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("idx", "int32"),
            });

            // Write 10 rows
            var rows = new object[10][];
            for (int i = 0; i < 10; i++)
            {
                rows[i] = new object[] { i };
            }

            var batch = ArrowConverter.FromRows(rows, schema);
            await _client!.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch));

            // Read all rows (no numRows parameter)
            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
            Assert.AreEqual(10, dt.Rows.Count, "Expected all 10 rows.");

            // Assert each returned idx value is a valid integer in [0, 9]
            var idxValues = new HashSet<long>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                idxValues.Add(Convert.ToInt64(dt.Rows[i]["idx"]));
            }

            for (int i = 0; i < 10; i++)
            {
                Assert.IsTrue(idxValues.Contains((long)i),
                    $"Missing idx value {i}.");
            }
        }

        // ================================================================== //
        //  Overwrite mode — second write replaces data
        // ================================================================== //

        [TestMethod]
        public async Task CreateTable_OverwriteMode_ReplacesData()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("value", "string"),
            });

            // First write
            var batch1 = ArrowConverter.FromRows(
                new[] { new object[] { "original" } }, schema);
            await _client!.InsertAsync(tablePath, batch1.Schema, ArrowConverter.ToAsyncEnumerable(batch1));

            // Overwrite
            var batch2 = ArrowConverter.FromRows(
                new[] { new object[] { "replaced" } }, schema);
            await _client.InsertAsync(tablePath, batch2.Schema, ArrowConverter.ToAsyncEnumerable(batch2));

            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();

            Assert.AreEqual(1, dt.Rows.Count, "Overwrite should have replaced the original row.");
            Assert.AreEqual("replaced", dt.Rows[0]["value"]?.ToString());
        }

        // ================================================================== //
        //  Append mode — second write adds data
        // ================================================================== //

        [TestMethod]
        public async Task CreateTable_AppendMode_AddsData()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("value", "string"),
            });

            // First write
            var batch1 = ArrowConverter.FromRows(
                new[] { new object[] { "first" } }, schema);
            await _client!.InsertAsync(tablePath, batch1.Schema, ArrowConverter.ToAsyncEnumerable(batch1));

            // Append
            var batch2 = ArrowConverter.FromRows(
                new[] { new object[] { "second" } }, schema);
            await _client.InsertAsync(tablePath, batch2.Schema, ArrowConverter.ToAsyncEnumerable(batch2),
                mode: SaveMode.Append);

            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();

            Assert.AreEqual(2, dt.Rows.Count, "Append should result in 2 total rows.");

            // Assert both values are present
            var values = new HashSet<string>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                values.Add(dt.Rows[i]["value"]?.ToString() ?? "");
            }
            Assert.IsTrue(values.Contains("first"), "Missing 'first' value after append.");
            Assert.IsTrue(values.Contains("second"), "Missing 'second' value after append.");
        }

        // ================================================================== //
        //  GetSchema on a table with data
        // ================================================================== //

        [TestMethod]
        public async Task GetSchema_OnPopulatedTable_ReturnsCorrectSchema()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var source = new DataTable();
            source.Columns.Add("name", typeof(string));
            source.Columns.Add("age", typeof(int));
            source.Columns.Add("score", typeof(double));
            source.Rows.Add("Test", 30, 95.5);

            var batch = ArrowConverter.FromDataTable(source);
            await _client!.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch));

            TableSchema schema = await _client.GetSchemaAsync(tablePath);

            Assert.AreEqual(3, schema.Columns.Count);

            // Column names should match
            Assert.AreEqual("name", schema.Columns[0].Name);
            Assert.AreEqual("age", schema.Columns[1].Name);
            Assert.AreEqual("score", schema.Columns[2].Name);

            // Assert data types (PySpark widens through pandas→Arrow round-trip)
            string nameType = schema.Columns[0].DataType.ToLowerInvariant();
            Assert.IsTrue(nameType.Contains("utf8") || nameType.Contains("string"),
                $"Expected name column to be string/utf8 type, got '{nameType}'.");

            string ageType = schema.Columns[1].DataType.ToLowerInvariant();
            Assert.IsTrue(ageType.Contains("int"),
                $"Expected age column to be integer type, got '{ageType}'.");

            string scoreType = schema.Columns[2].DataType.ToLowerInvariant();
            Assert.IsTrue(scoreType.Contains("double") || scoreType.Contains("float"),
                $"Expected score column to be double/float type, got '{scoreType}'.");
        }

        // ================================================================== //
        //  ReadTableAsync — raw RecordBatch access
        // ================================================================== //

        [TestMethod]
        public async Task ReadTableAsArrow_ReturnsRecordBatches()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            string csv = "x,y\n1,2\n3,4";
            var csvBatch = ArrowConverter.FromCsv(csv);
            await _client!.InsertAsync(tablePath, csvBatch.Schema, ArrowConverter.ToAsyncEnumerable(csvBatch));

            List<Apache.Arrow.RecordBatch> batches =
                await _client.ReadTableAsync(tablePath).ToListAsync();

            Assert.IsTrue(batches.Count > 0, "Expected at least one RecordBatch.");

            int totalRows = 0;
            foreach (var batch in batches)
            {
                totalRows += batch.Length;
            }
            Assert.AreEqual(2, totalRows, "Expected 2 total rows across all batches.");

            // Assert column names
            Apache.Arrow.Schema arrowSchema = batches[0].Schema;
            var columnNames = arrowSchema.FieldsList.Select(f => f.Name).ToList();
            Assert.IsTrue(columnNames.Contains("x"), "Schema should contain column 'x'.");
            Assert.IsTrue(columnNames.Contains("y"), "Schema should contain column 'y'.");

            // Assert actual values (CSV creates string columns)
            var xValues = new HashSet<string>();
            var yValues = new HashSet<string>();
            foreach (var batch in batches)
            {
                int xIdx = arrowSchema.GetFieldIndex("x");
                int yIdx = arrowSchema.GetFieldIndex("y");
                var xArray = batch.Column(xIdx) as Apache.Arrow.StringArray;
                var yArray = batch.Column(yIdx) as Apache.Arrow.StringArray;
                Assert.IsNotNull(xArray, "Expected column 'x' to be a StringArray (CSV data).");
                Assert.IsNotNull(yArray, "Expected column 'y' to be a StringArray (CSV data).");
                for (int i = 0; i < batch.Length; i++)
                {
                    xValues.Add(xArray!.GetString(i));
                    yValues.Add(yArray!.GetString(i));
                }
            }
            Assert.IsTrue(xValues.Contains("1"), "Missing x value '1'.");
            Assert.IsTrue(xValues.Contains("3"), "Missing x value '3'.");
            Assert.IsTrue(yValues.Contains("2"), "Missing y value '2'.");
            Assert.IsTrue(yValues.Contains("4"), "Missing y value '4'.");
        }

        // ================================================================== //
        //  Column Mapping — create, write, read, rename
        // ================================================================== //

        [TestMethod]
        public async Task CreateTableWithColumnMapping_GetSchema_ReturnsCorrectColumns()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
                new ColumnDefinition("score", "double"),
            });

            var config = new Dictionary<string, string>
            {
                ["delta.columnMapping.mode"] = "name",
            };

            ExecuteResult createResult = await _client!.CreateTableAsync(
                tablePath, schema, config);
            Assert.IsTrue(createResult.Success, $"CreateTableWithConfig failed: {createResult.Message}");

            TableSchema readSchema = await _client.GetSchemaAsync(tablePath);
            Assert.AreEqual(3, readSchema.Columns.Count, "Expected 3 columns.");
            Assert.AreEqual("id", readSchema.Columns[0].Name);
            Assert.AreEqual("name", readSchema.Columns[1].Name);
            Assert.AreEqual("score", readSchema.Columns[2].Name);

            // Verify data types
            string idType = readSchema.Columns[0].DataType.ToLowerInvariant();
            Assert.IsTrue(idType.Contains("int"),
                $"Expected id column to be integer type, got '{idType}'.");

            string nameType = readSchema.Columns[1].DataType.ToLowerInvariant();
            Assert.IsTrue(nameType.Contains("utf8") || nameType.Contains("string"),
                $"Expected name column to be string/utf8 type, got '{nameType}'.");

            string scoreType = readSchema.Columns[2].DataType.ToLowerInvariant();
            Assert.IsTrue(scoreType.Contains("double") || scoreType.Contains("float"),
                $"Expected score column to be double/float type, got '{scoreType}'.");
        }

        [TestMethod]
        public async Task CreateTableWithColumnMapping_WriteAndRead_DataRoundTrips()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("city", "string"),
            });

            var config = new Dictionary<string, string>
            {
                ["delta.columnMapping.mode"] = "name",
            };

            ExecuteResult createResult = await _client!.CreateTableAsync(
                tablePath, schema, config);
            Assert.IsTrue(createResult.Success, $"CreateTableWithConfig failed: {createResult.Message}");

            // Write data via append to the column-mapped table
            var batch = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 1, "Seattle" },
                    new object[] { 2, "Portland" },
                    new object[] { 3, "Denver" },
                }, schema);
            await _client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                mode: SaveMode.Append);

            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
            Assert.AreEqual(3, dt.Rows.Count, "Expected 3 rows.");

            var rowsByCity = new Dictionary<string, DataRow>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string city = dt.Rows[i]["city"]?.ToString() ?? "";
                rowsByCity[city] = dt.Rows[i];
            }

            Assert.IsTrue(rowsByCity.ContainsKey("Seattle"), "Missing Seattle");
            Assert.IsTrue(rowsByCity.ContainsKey("Portland"), "Missing Portland");
            Assert.IsTrue(rowsByCity.ContainsKey("Denver"), "Missing Denver");

            Assert.AreEqual(1L, Convert.ToInt64(rowsByCity["Seattle"]["id"]), "Seattle should have id=1");
            Assert.AreEqual(2L, Convert.ToInt64(rowsByCity["Portland"]["id"]), "Portland should have id=2");
            Assert.AreEqual(3L, Convert.ToInt64(rowsByCity["Denver"]["id"]), "Denver should have id=3");
        }

        [TestMethod]
        public async Task CreateTableWithColumnMapping_AppendMultipleBatches_AllDataPresent()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("label", "string"),
            });

            var config = new Dictionary<string, string>
            {
                ["delta.columnMapping.mode"] = "name",
            };

            ExecuteResult createResult = await _client!.CreateTableAsync(
                tablePath, schema, config);
            Assert.IsTrue(createResult.Success, $"CreateTableWithConfig failed: {createResult.Message}");

            // First append
            var batch1 = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 1, "alpha" },
                    new object[] { 2, "bravo" },
                }, schema);
            await _client.InsertAsync(tablePath, batch1.Schema, ArrowConverter.ToAsyncEnumerable(batch1),
                mode: SaveMode.Append);

            // Second append
            var batch2 = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 3, "charlie" },
                    new object[] { 4, "delta" },
                }, schema);
            await _client.InsertAsync(tablePath, batch2.Schema, ArrowConverter.ToAsyncEnumerable(batch2),
                mode: SaveMode.Append);

            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
            Assert.AreEqual(4, dt.Rows.Count, "Expected 4 rows after two appends.");

            var labels = new HashSet<string>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                labels.Add(dt.Rows[i]["label"]?.ToString() ?? "");
            }

            Assert.IsTrue(labels.Contains("alpha"), "Missing 'alpha'");
            Assert.IsTrue(labels.Contains("bravo"), "Missing 'bravo'");
            Assert.IsTrue(labels.Contains("charlie"), "Missing 'charlie'");
            Assert.IsTrue(labels.Contains("delta"), "Missing 'delta'");
        }

        [TestMethod]
        public async Task CreateTableWithColumnMapping_RenameColumnViaSql_OldDataStillReadable()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("old_name", "string"),
            });

            var config = new Dictionary<string, string>
            {
                ["delta.columnMapping.mode"] = "name",
            };

            ExecuteResult createResult = await _client!.CreateTableAsync(
                tablePath, schema, config);
            Assert.IsTrue(createResult.Success, $"CreateTableWithConfig failed: {createResult.Message}");

            // Write initial data with the original column name
            var batch = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 1, "before_rename_1" },
                    new object[] { 2, "before_rename_2" },
                }, schema);
            await _client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                mode: SaveMode.Append);

            // Rename column via SQL — use delta.`path` syntax for DDL on Delta tables
            await _client.ExecuteQueryAsync(
                $"ALTER TABLE delta.`{tablePath}` RENAME COLUMN old_name TO new_name")
                .ToListAsync();

            // Read back — the column should now be named "new_name" and old data should be accessible
            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
            Assert.AreEqual(2, dt.Rows.Count, "Expected 2 rows after column rename.");

            // The column should now be "new_name"
            Assert.IsTrue(dt.Columns.Contains("new_name"),
                $"Expected 'new_name' column after rename. Columns: {string.Join(", ", dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName))}");
            Assert.IsFalse(dt.Columns.Contains("old_name"),
                "Column 'old_name' should no longer exist after rename.");

            // Old data should still be readable under the new column name
            var values = new HashSet<string>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                values.Add(dt.Rows[i]["new_name"]?.ToString() ?? "");
            }

            Assert.IsTrue(values.Contains("before_rename_1"), "Missing 'before_rename_1' after rename.");
            Assert.IsTrue(values.Contains("before_rename_2"), "Missing 'before_rename_2' after rename.");
        }

        // ================================================================== //
        //  Deletion Vectors — delete rows, verify correct data returned
        // ================================================================== //

        [TestMethod]
        public async Task CreateTableWithDeletionVectors_DeleteRows_OnlyRemainingRowsReturned()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("value", "string"),
            });

            var config = new Dictionary<string, string>
            {
                ["delta.enableDeletionVectors"] = "true",
            };

            ExecuteResult createResult = await _client!.CreateTableAsync(
                tablePath, schema, config);
            Assert.IsTrue(createResult.Success, $"CreateTableWithConfig failed: {createResult.Message}");

            // Write 5 rows
            var batch = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 1, "one" },
                    new object[] { 2, "two" },
                    new object[] { 3, "three" },
                    new object[] { 4, "four" },
                    new object[] { 5, "five" },
                }, schema);
            await _client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                mode: SaveMode.Append);

            // Delete row with id=3
            string tableName = $"dv_table_{Guid.NewGuid():N}";
            ExecuteResult deleteResult = await _client.DeleteAsync(
                $"DELETE FROM {tableName} WHERE id = 3", tablePath, tableName);
            Assert.IsTrue(deleteResult.Success, $"Delete failed: {deleteResult.Message}");

            // Read back — should have 4 rows, id=3 should be gone
            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
            Assert.AreEqual(4, dt.Rows.Count, "Expected 4 rows after deleting id=3.");

            var ids = new HashSet<long>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                ids.Add(Convert.ToInt64(dt.Rows[i]["id"]));
            }

            Assert.IsTrue(ids.Contains(1L), "Missing id=1");
            Assert.IsTrue(ids.Contains(2L), "Missing id=2");
            Assert.IsFalse(ids.Contains(3L), "id=3 should have been deleted");
            Assert.IsTrue(ids.Contains(4L), "Missing id=4");
            Assert.IsTrue(ids.Contains(5L), "Missing id=5");
        }

        [TestMethod]
        public async Task CreateTableWithDeletionVectors_DeleteMultipleRows_CorrectRowCount()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("category", "string"),
            });

            var config = new Dictionary<string, string>
            {
                ["delta.enableDeletionVectors"] = "true",
            };

            ExecuteResult createResult = await _client!.CreateTableAsync(
                tablePath, schema, config);
            Assert.IsTrue(createResult.Success, $"CreateTableWithConfig failed: {createResult.Message}");

            // Write 10 rows
            var rows = new object[10][];
            for (int i = 0; i < 10; i++)
            {
                rows[i] = new object[] { i + 1, i < 5 ? "keep" : "remove" };
            }

            var batch = ArrowConverter.FromRows(rows, schema);
            await _client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                mode: SaveMode.Append);

            // Delete rows where id > 5
            string tableName = $"dv_table_{Guid.NewGuid():N}";
            ExecuteResult deleteResult = await _client.DeleteAsync(
                $"DELETE FROM {tableName} WHERE id > 5", tablePath, tableName);
            Assert.IsTrue(deleteResult.Success, $"Delete failed: {deleteResult.Message}");

            // Read back — should have exactly 5 rows (ids 1-5)
            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
            Assert.AreEqual(5, dt.Rows.Count, "Expected 5 rows after deleting id > 5.");

            var ids = new HashSet<long>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                ids.Add(Convert.ToInt64(dt.Rows[i]["id"]));
            }

            for (long id = 1; id <= 5; id++)
            {
                Assert.IsTrue(ids.Contains(id), $"Missing id={id} (should have been kept).");
            }

            for (long id = 6; id <= 10; id++)
            {
                Assert.IsFalse(ids.Contains(id), $"id={id} should have been deleted.");
            }
        }

        [TestMethod]
        public async Task CreateTableWithDeletionVectors_DeleteThenAppend_BothOperationsWork()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("tag", "string"),
            });

            var config = new Dictionary<string, string>
            {
                ["delta.enableDeletionVectors"] = "true",
            };

            ExecuteResult createResult = await _client!.CreateTableAsync(
                tablePath, schema, config);
            Assert.IsTrue(createResult.Success, $"CreateTableWithConfig failed: {createResult.Message}");

            // Write initial data
            var batch1 = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 1, "original" },
                    new object[] { 2, "original" },
                    new object[] { 3, "original" },
                }, schema);
            await _client.InsertAsync(tablePath, batch1.Schema, ArrowConverter.ToAsyncEnumerable(batch1),
                mode: SaveMode.Append);

            // Delete id=2
            string tableName = $"dv_table_{Guid.NewGuid():N}";
            ExecuteResult deleteResult = await _client.DeleteAsync(
                $"DELETE FROM {tableName} WHERE id = 2", tablePath, tableName);
            Assert.IsTrue(deleteResult.Success, $"Delete failed: {deleteResult.Message}");

            // Append new rows after deletion
            var batch2 = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 10, "appended" },
                    new object[] { 11, "appended" },
                }, schema);
            await _client.InsertAsync(tablePath, batch2.Schema, ArrowConverter.ToAsyncEnumerable(batch2),
                mode: SaveMode.Append);

            // Read back — should have 4 rows: ids 1, 3, 10, 11
            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
            Assert.AreEqual(4, dt.Rows.Count, "Expected 4 rows: 3 original - 1 deleted + 2 appended.");

            var rowsById = new Dictionary<long, string>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                long id = Convert.ToInt64(dt.Rows[i]["id"]);
                string tag = dt.Rows[i]["tag"]?.ToString() ?? "";
                rowsById[id] = tag;
            }

            // Verify kept rows
            Assert.IsTrue(rowsById.ContainsKey(1L), "Missing id=1");
            Assert.AreEqual("original", rowsById[1L], "id=1 should have tag='original'");

            // Verify deleted row is gone
            Assert.IsFalse(rowsById.ContainsKey(2L), "id=2 should have been deleted");

            // Verify kept row
            Assert.IsTrue(rowsById.ContainsKey(3L), "Missing id=3");
            Assert.AreEqual("original", rowsById[3L], "id=3 should have tag='original'");

            // Verify appended rows
            Assert.IsTrue(rowsById.ContainsKey(10L), "Missing id=10 (appended)");
            Assert.AreEqual("appended", rowsById[10L], "id=10 should have tag='appended'");
            Assert.IsTrue(rowsById.ContainsKey(11L), "Missing id=11 (appended)");
            Assert.AreEqual("appended", rowsById[11L], "id=11 should have tag='appended'");
        }

        // ================================================================== //
        //  DML: UPDATE
        // ================================================================== //

        [TestMethod]
        public async Task UpdateAsync_BasicUpdate_ModifiesRows()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("status", "string"),
            });

            var config = new Dictionary<string, string>
            {
                ["delta.enableDeletionVectors"] = "true",
            };

            ExecuteResult createResult = await _client!.CreateTableAsync(
                tablePath, schema, config);
            Assert.IsTrue(createResult.Success, $"CreateTableWithConfig failed: {createResult.Message}");

            var batch = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 1, "active" },
                    new object[] { 2, "active" },
                    new object[] { 3, "inactive" },
                }, schema);
            await _client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                mode: SaveMode.Append);

            // Update: set status='updated' where id <= 2
            string tableName = $"upd_table_{Guid.NewGuid():N}";
            ExecuteResult updateResult = await _client.UpdateAsync(
                $"UPDATE {tableName} SET status = 'updated' WHERE id <= 2",
                tablePath, tableName);
            Assert.IsTrue(updateResult.Success, $"Update failed: {updateResult.Message}");

            // Read back and verify
            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
            Assert.AreEqual(3, dt.Rows.Count, "Expected 3 rows after update.");

            var rowsById = new Dictionary<long, string>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                long id = Convert.ToInt64(dt.Rows[i]["id"]);
                string status = dt.Rows[i]["status"]?.ToString() ?? "";
                rowsById[id] = status;
            }

            Assert.AreEqual("updated", rowsById[1L], "id=1 should have status='updated'");
            Assert.AreEqual("updated", rowsById[2L], "id=2 should have status='updated'");
            Assert.AreEqual("inactive", rowsById[3L], "id=3 should still be 'inactive'");
        }

        [TestMethod]
        public async Task UpdateAsync_UpdateAllRows_AllModified()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("value", "string"),
            });

            var config = new Dictionary<string, string>
            {
                ["delta.enableDeletionVectors"] = "true",
            };

            ExecuteResult createResult = await _client!.CreateTableAsync(
                tablePath, schema, config);
            Assert.IsTrue(createResult.Success, $"CreateTableWithConfig failed: {createResult.Message}");

            var batch = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 1, "old" },
                    new object[] { 2, "old" },
                }, schema);
            await _client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                mode: SaveMode.Append);

            // Update all rows (no WHERE clause)
            string tableName = $"upd_table_{Guid.NewGuid():N}";
            ExecuteResult updateResult = await _client.UpdateAsync(
                $"UPDATE {tableName} SET value = 'new'",
                tablePath, tableName);
            Assert.IsTrue(updateResult.Success, $"Update failed: {updateResult.Message}");

            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
            Assert.AreEqual(2, dt.Rows.Count, "Expected 2 rows after update.");

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                Assert.AreEqual("new", dt.Rows[i]["value"]?.ToString(),
                    $"Row {i} should have value='new' after update.");
            }
        }

        // ================================================================== //
        //  DML: MERGE
        // ================================================================== //

        [TestMethod]
        public async Task MergeAsync_WhenMatchedUpdate_WhenNotMatchedInsert()
        {
            // --- Target table ---
            string targetPath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("value", "string"),
            });

            var config = new Dictionary<string, string>
            {
                ["delta.enableDeletionVectors"] = "true",
            };

            ExecuteResult createResult = await _client!.CreateTableAsync(
                targetPath, schema, config);
            Assert.IsTrue(createResult.Success, $"CreateTableWithConfig (target) failed: {createResult.Message}");

            var targetBatch = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 1, "original_1" },
                    new object[] { 2, "original_2" },
                }, schema);
            await _client.InsertAsync(targetPath, targetBatch.Schema, ArrowConverter.ToAsyncEnumerable(targetBatch),
                mode: SaveMode.Append);

            // --- Source table ---
            string sourcePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var sourceBatch = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 2, "merged_2" },   // matches target id=2 → UPDATE
                    new object[] { 3, "merged_3" },   // no match → INSERT
                }, schema);
            await _client.InsertAsync(sourcePath, sourceBatch.Schema, ArrowConverter.ToAsyncEnumerable(sourceBatch));

            // Register both tables as temp views and run MERGE
            string targetName = $"target_{Guid.NewGuid():N}";
            string sourceName = $"source_{Guid.NewGuid():N}";

            // Register source table as a temp view first
            await _client.ExecuteQueryAsync(
                $"CREATE OR REPLACE TEMPORARY VIEW {sourceName} USING delta OPTIONS (path '{sourcePath}')")
                .ToListAsync();

            string mergeSql = $@"
MERGE INTO {targetName} AS t
USING {sourceName} AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.value = s.value
WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)";

            ExecuteResult mergeResult = await _client.MergeAsync(
                mergeSql, targetPath, targetName);
            Assert.IsTrue(mergeResult.Success, $"Merge failed: {mergeResult.Message}");

            // Read back target and verify
            DataTable dt = await _client.ReadTableAsync(targetPath).ToDataTableAsync();
            Assert.AreEqual(3, dt.Rows.Count, "Expected 3 rows after merge (2 original + 1 inserted).");

            var rowsById = new Dictionary<long, string>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                long id = Convert.ToInt64(dt.Rows[i]["id"]);
                string value = dt.Rows[i]["value"]?.ToString() ?? "";
                rowsById[id] = value;
            }

            Assert.AreEqual("original_1", rowsById[1L], "id=1 should remain 'original_1' (no match in source)");
            Assert.AreEqual("merged_2", rowsById[2L], "id=2 should be updated to 'merged_2'");
            Assert.AreEqual("merged_3", rowsById[3L], "id=3 should be inserted as 'merged_3'");
        }

        // ================================================================== //
        //  MergeData (streaming) — upsert all
        // ================================================================== //

        [TestMethod]
        public async Task V1_MergeData_UpsertAll_UpdatesAndInserts()
        {
            string targetPath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var targetSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("value", "string"),
            });

            var targetBatch = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 1, "original_1" },
                    new object[] { 2, "original_2" },
                    new object[] { 3, "original_3" },
                }, targetSchema);
            await _client!.InsertAsync(targetPath, targetBatch.Schema, ArrowConverter.ToAsyncEnumerable(targetBatch));

            // --- Build source Arrow data for merge ---
            var arrowSchema = new Apache.Arrow.Schema.Builder()
                .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int32Type.Default).Nullable(false))
                .Field(f => f.Name("value").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true))
                .Build();

            var idBuilder = new Apache.Arrow.Int32Array.Builder();
            var valueBuilder = new Apache.Arrow.StringArray.Builder();
            idBuilder.Append(2).Append(4);          // id=2 matches target, id=4 is new
            valueBuilder.Append("merged_2").Append("merged_4");

            var sourceBatch = new Apache.Arrow.RecordBatch(arrowSchema,
                new Apache.Arrow.IArrowArray[] { idBuilder.Build(), valueBuilder.Build() }, 2);

            // --- Execute MergeData ---
            var mergeOptions = new MergeOptions("target.id = source.id")
            {
                WhenMatchedUpdateAll = true,
                WhenNotMatchedInsertAll = true,
            };

            ExecuteResult result = await _client.MergeDataAsync(
                targetPath, arrowSchema, ArrowConverter.ToAsyncEnumerable(sourceBatch), mergeOptions);

            Assert.IsTrue(result.Success, $"MergeData failed: {result.Message}");

            // --- Read back and verify ---
            DataTable dt = await _client.ReadTableAsync(targetPath).ToDataTableAsync();
            Assert.AreEqual(4, dt.Rows.Count, "Expected 4 rows: 3 original + 1 inserted.");

            var rowsById = new Dictionary<long, string>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                long id = Convert.ToInt64(dt.Rows[i]["id"]);
                string value = dt.Rows[i]["value"]?.ToString() ?? "";
                rowsById[id] = value;
            }

            Assert.AreEqual("original_1", rowsById[1L], "id=1 should remain unchanged (not in source).");
            Assert.AreEqual("merged_2", rowsById[2L], "id=2 should be updated to 'merged_2'.");
            Assert.AreEqual("original_3", rowsById[3L], "id=3 should remain unchanged (not in source).");
            Assert.AreEqual("merged_4", rowsById[4L], "id=4 should be inserted as 'merged_4'.");
        }

        // ================================================================== //
        //  MergeData (streaming) — matched delete with predicate
        // ================================================================== //

        [TestMethod]
        public async Task V1_MergeData_WhenMatchedDelete_RemovesMatchedRows()
        {
            string targetPath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var targetSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("status", "string"),
            });

            var targetBatch = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 1, "active" },
                    new object[] { 2, "active" },
                    new object[] { 3, "active" },
                }, targetSchema);
            await _client!.InsertAsync(targetPath, targetBatch.Schema, ArrowConverter.ToAsyncEnumerable(targetBatch));

            // Source: id=2 flagged for deletion, id=1 not flagged
            var arrowSchema = new Apache.Arrow.Schema.Builder()
                .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int32Type.Default).Nullable(false))
                .Field(f => f.Name("deleted").DataType(Apache.Arrow.Types.BooleanType.Default).Nullable(false))
                .Build();

            var idBuilder = new Apache.Arrow.Int32Array.Builder();
            var deletedBuilder = new Apache.Arrow.BooleanArray.Builder();
            idBuilder.Append(1).Append(2);
            deletedBuilder.Append(false).Append(true);

            var sourceBatch = new Apache.Arrow.RecordBatch(arrowSchema,
                new Apache.Arrow.IArrowArray[] { idBuilder.Build(), deletedBuilder.Build() }, 2);

            // Delete matched rows where source.deleted = true
            var mergeOptions = new MergeOptions("target.id = source.id")
            {
                WhenMatchedDeletePredicate = "source.deleted = true",
            };

            ExecuteResult result = await _client.MergeDataAsync(
                targetPath, arrowSchema, ArrowConverter.ToAsyncEnumerable(sourceBatch), mergeOptions);

            Assert.IsTrue(result.Success, $"MergeData failed: {result.Message}");

            DataTable dt = await _client.ReadTableAsync(targetPath).ToDataTableAsync();
            Assert.AreEqual(2, dt.Rows.Count, "Expected 2 rows: id=2 should be deleted.");

            var ids = new HashSet<long>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                ids.Add(Convert.ToInt64(dt.Rows[i]["id"]));
            }

            Assert.IsTrue(ids.Contains(1L), "id=1 should remain (matched but deleted=false).");
            Assert.IsFalse(ids.Contains(2L), "id=2 should be deleted (matched and deleted=true).");
            Assert.IsTrue(ids.Contains(3L), "id=3 should remain (no match in source).");
        }

        // ================================================================== //
        //  MergeData (streaming) — explicit update set (not update all)
        // ================================================================== //

        [TestMethod]
        public async Task V1_MergeData_ExplicitUpdateSet_UpdatesSpecificColumns()
        {
            string targetPath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var targetSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
                new ColumnDefinition("score", "int32"),
            });

            var targetBatch = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 1, "Alice", 50 },
                    new object[] { 2, "Bob", 60 },
                }, targetSchema);
            await _client!.InsertAsync(targetPath, targetBatch.Schema, ArrowConverter.ToAsyncEnumerable(targetBatch));

            // Source has new score for id=1
            var arrowSchema = new Apache.Arrow.Schema.Builder()
                .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int32Type.Default).Nullable(false))
                .Field(f => f.Name("name").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true))
                .Field(f => f.Name("score").DataType(Apache.Arrow.Types.Int32Type.Default).Nullable(true))
                .Build();

            var idBuilder = new Apache.Arrow.Int32Array.Builder();
            var nameBuilder = new Apache.Arrow.StringArray.Builder();
            var scoreBuilder = new Apache.Arrow.Int32Array.Builder();
            idBuilder.Append(1);
            nameBuilder.Append("Alice_Updated");
            scoreBuilder.Append(99);

            var sourceBatch = new Apache.Arrow.RecordBatch(arrowSchema,
                new Apache.Arrow.IArrowArray[] { idBuilder.Build(), nameBuilder.Build(), scoreBuilder.Build() }, 1);

            // Only update score, not name
            var mergeOptions = new MergeOptions("target.id = source.id")
            {
                WhenMatchedUpdateSet = new Dictionary<string, string>
                {
                    { "score", "source.score" },
                },
            };

            ExecuteResult result = await _client.MergeDataAsync(
                targetPath, arrowSchema, ArrowConverter.ToAsyncEnumerable(sourceBatch), mergeOptions);

            Assert.IsTrue(result.Success, $"MergeData failed: {result.Message}");

            DataTable dt = await _client.ReadTableAsync(targetPath).ToDataTableAsync();
            Assert.AreEqual(2, dt.Rows.Count, "Expected 2 rows (no inserts).");

            var rowsById = new Dictionary<long, DataRow>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                long id = Convert.ToInt64(dt.Rows[i]["id"]);
                rowsById[id] = dt.Rows[i];
            }

            // id=1: name should stay "Alice" (not updated), score should be 99
            Assert.AreEqual("Alice", rowsById[1L]["name"]?.ToString(),
                "id=1 name should remain 'Alice' (only score was in update set).");
            Assert.AreEqual(99L, Convert.ToInt64(rowsById[1L]["score"]),
                "id=1 score should be updated to 99.");

            // id=2: completely unchanged
            Assert.AreEqual("Bob", rowsById[2L]["name"]?.ToString(), "id=2 should be unchanged.");
            Assert.AreEqual(60L, Convert.ToInt64(rowsById[2L]["score"]),
                "id=2 score should remain 60.");
        }

        // ================================================================== //
        //  MergeData (streaming) — not matched by source delete
        // ================================================================== //

        [TestMethod]
        public async Task V1_MergeData_NotMatchedBySourceDelete_RemovesOrphanedTargetRows()
        {
            string targetPath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var targetSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("value", "string"),
            });

            var targetBatch = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 1, "keep" },
                    new object[] { 2, "keep" },
                    new object[] { 3, "orphan" },
                }, targetSchema);
            await _client!.InsertAsync(targetPath, targetBatch.Schema, ArrowConverter.ToAsyncEnumerable(targetBatch));

            // Source only has id=1 and id=2 — id=3 is "not matched by source"
            var arrowSchema = new Apache.Arrow.Schema.Builder()
                .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int32Type.Default).Nullable(false))
                .Field(f => f.Name("value").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true))
                .Build();

            var idBuilder = new Apache.Arrow.Int32Array.Builder();
            var valueBuilder = new Apache.Arrow.StringArray.Builder();
            idBuilder.Append(1).Append(2);
            valueBuilder.Append("updated_1").Append("updated_2");

            var sourceBatch = new Apache.Arrow.RecordBatch(arrowSchema,
                new Apache.Arrow.IArrowArray[] { idBuilder.Build(), valueBuilder.Build() }, 2);

            var mergeOptions = new MergeOptions("target.id = source.id")
            {
                WhenMatchedUpdateAll = true,
                WhenNotMatchedBySourceDeletePredicate = "true",
            };

            ExecuteResult result = await _client.MergeDataAsync(
                targetPath, arrowSchema, ArrowConverter.ToAsyncEnumerable(sourceBatch), mergeOptions);

            Assert.IsTrue(result.Success, $"MergeData failed: {result.Message}");

            DataTable dt = await _client.ReadTableAsync(targetPath).ToDataTableAsync();
            Assert.AreEqual(2, dt.Rows.Count,
                "Expected 2 rows: id=3 should be deleted (not matched by source).");

            var rowsById = new Dictionary<long, string>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                long id = Convert.ToInt64(dt.Rows[i]["id"]);
                string value = dt.Rows[i]["value"]?.ToString() ?? "";
                rowsById[id] = value;
            }

            Assert.AreEqual("updated_1", rowsById[1L], "id=1 should be updated.");
            Assert.AreEqual("updated_2", rowsById[2L], "id=2 should be updated.");
            Assert.IsFalse(rowsById.ContainsKey(3L), "id=3 should be deleted (orphaned).");
        }

        // ================================================================== //
        //  MergeData (streaming) — insert only (no matched clause)
        // ================================================================== //

        [TestMethod]
        public async Task V1_MergeData_InsertOnly_InsertsUnmatchedRows()
        {
            string targetPath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var targetSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("value", "string"),
            });

            var targetBatch = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 1, "existing" },
                }, targetSchema);
            await _client!.InsertAsync(targetPath, targetBatch.Schema, ArrowConverter.ToAsyncEnumerable(targetBatch));

            // Source: id=1 (matches — but no when_matched clause, so no update)
            //         id=2 (no match — should be inserted)
            var arrowSchema = new Apache.Arrow.Schema.Builder()
                .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int32Type.Default).Nullable(false))
                .Field(f => f.Name("value").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true))
                .Build();

            var idBuilder = new Apache.Arrow.Int32Array.Builder();
            var valueBuilder = new Apache.Arrow.StringArray.Builder();
            idBuilder.Append(1).Append(2);
            valueBuilder.Append("ignored").Append("inserted");

            var sourceBatch = new Apache.Arrow.RecordBatch(arrowSchema,
                new Apache.Arrow.IArrowArray[] { idBuilder.Build(), valueBuilder.Build() }, 2);

            // Only insert, no update clause
            var mergeOptions = new MergeOptions("target.id = source.id")
            {
                WhenNotMatchedInsertAll = true,
            };

            ExecuteResult result = await _client.MergeDataAsync(
                targetPath, arrowSchema, ArrowConverter.ToAsyncEnumerable(sourceBatch), mergeOptions);

            Assert.IsTrue(result.Success, $"MergeData failed: {result.Message}");

            DataTable dt = await _client.ReadTableAsync(targetPath).ToDataTableAsync();
            Assert.AreEqual(2, dt.Rows.Count, "Expected 2 rows: 1 existing + 1 inserted.");

            var rowsById = new Dictionary<long, string>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                long id = Convert.ToInt64(dt.Rows[i]["id"]);
                string value = dt.Rows[i]["value"]?.ToString() ?? "";
                rowsById[id] = value;
            }

            Assert.AreEqual("existing", rowsById[1L],
                "id=1 should remain 'existing' (no when_matched clause).");
            Assert.AreEqual("inserted", rowsById[2L],
                "id=2 should be inserted as 'inserted'.");
        }

        // ================================================================== //
        //  MergeData (streaming) — multiple batches streamed
        // ================================================================== //

        [TestMethod]
        public async Task V1_MergeData_MultipleBatches_AllDataMerged()
        {
            string targetPath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var targetSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("value", "string"),
            });

            var targetBatch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 1, "old_1" },
                    new object[] { 2, "old_2" },
                }, targetSchema);
            await _client!.InsertAsync(targetPath, targetBatch.Schema, ArrowConverter.ToAsyncEnumerable(targetBatch));

            var arrowSchema = new Apache.Arrow.Schema.Builder()
                .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int32Type.Default).Nullable(false))
                .Field(f => f.Name("value").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true))
                .Build();

            // Batch 1: update id=1
            var batch1 = BuildBatch(arrowSchema, new[] { 1 }, new[] { "new_1" });
            // Batch 2: insert id=3
            var batch2 = BuildBatch(arrowSchema, new[] { 3 }, new[] { "new_3" });

            var mergeOptions = new MergeOptions("target.id = source.id")
            {
                WhenMatchedUpdateAll = true,
                WhenNotMatchedInsertAll = true,
            };

            ExecuteResult result = await _client.MergeDataAsync(
                targetPath, arrowSchema, ArrowConverter.ToAsyncEnumerable(batch1, batch2), mergeOptions);

            Assert.IsTrue(result.Success, $"MergeData failed: {result.Message}");

            DataTable dt = await _client.ReadTableAsync(targetPath).ToDataTableAsync();
            Assert.AreEqual(3, dt.Rows.Count, "Expected 3 rows: 2 original + 1 inserted.");

            var rowsById = new Dictionary<long, string>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                long id = Convert.ToInt64(dt.Rows[i]["id"]);
                string value = dt.Rows[i]["value"]?.ToString() ?? "";
                rowsById[id] = value;
            }

            Assert.AreEqual("new_1", rowsById[1L], "id=1 should be updated to 'new_1'.");
            Assert.AreEqual("old_2", rowsById[2L], "id=2 should remain 'old_2' (not in source).");
            Assert.AreEqual("new_3", rowsById[3L], "id=3 should be inserted as 'new_3'.");
        }

        // ================================================================== //
        //  DML: DELETE edge cases
        // ================================================================== //

        [TestMethod]
        public async Task DeleteAsync_NoWhereClause_DeletesAllRows()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("value", "string"),
            });

            var config = new Dictionary<string, string>
            {
                ["delta.enableDeletionVectors"] = "true",
            };

            ExecuteResult createResult = await _client!.CreateTableAsync(
                tablePath, schema, config);
            Assert.IsTrue(createResult.Success, $"CreateTableWithConfig failed: {createResult.Message}");

            var batch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 1, "a" },
                    new object[] { 2, "b" },
                    new object[] { 3, "c" },
                }, schema);
            await _client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                mode: SaveMode.Append);

            // Delete all rows (no WHERE clause)
            string tableName = $"del_table_{Guid.NewGuid():N}";
            ExecuteResult deleteResult = await _client.DeleteAsync(
                $"DELETE FROM {tableName}", tablePath, tableName);
            Assert.IsTrue(deleteResult.Success, $"Delete failed: {deleteResult.Message}");

            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
            Assert.AreEqual(0, dt.Rows.Count, "Expected 0 rows after DELETE with no WHERE clause.");
        }

        // ================================================================== //
        //  SQL prefix validation (client-side, no Docker needed)
        // ================================================================== //

        [TestMethod]
        public void DeleteAsync_InvalidSqlPrefix_ThrowsArgumentException()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                _client!.DeleteAsync("SELECT * FROM t", "/tmp/unused", "t").GetAwaiter().GetResult());
        }

        [TestMethod]
        public void UpdateAsync_InvalidSqlPrefix_ThrowsArgumentException()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                _client!.UpdateAsync("SELECT * FROM t", "/tmp/unused", "t").GetAwaiter().GetResult());
        }

        [TestMethod]
        public void MergeAsync_InvalidSqlPrefix_ThrowsArgumentException()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                _client!.MergeAsync("SELECT * FROM t", "/tmp/unused", "t").GetAwaiter().GetResult());
        }

        // ================================================================== //
        //  ExecuteQueryAsDataTableAsync — convenience method
        // ================================================================== //

        [TestMethod]
        public async Task ExecuteQueryAsDataTable_SelectFromTable_ReturnsDataTable()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });

            var batch = ArrowConverter.FromRows(new[]
                {
                    new object[] { 1, "Alice" },
                    new object[] { 2, "Bob" },
                }, schema);
            await _client!.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch));

            // Register a temp view and query via the DataTable convenience method
            string viewName = $"dt_view_{Guid.NewGuid():N}";
            await _client.ExecuteQueryAsync(
                $"CREATE OR REPLACE TEMPORARY VIEW {viewName} USING delta OPTIONS (path '{tablePath}')")
                .ToListAsync();

            DataTable result = await _client.ExecuteQueryAsync(
                $"SELECT * FROM {viewName} ORDER BY id")
                .ToDataTableAsync();

            Assert.IsNotNull(result, "Query should return a non-null DataTable.");
            Assert.AreEqual(2, result.Rows.Count, "Expected 2 rows.");
            Assert.IsTrue(result.Columns.Contains("id"), "DataTable should contain 'id' column.");
            Assert.IsTrue(result.Columns.Contains("name"), "DataTable should contain 'name' column.");

            // Verify data is accessible as a DataTable
            var names = new HashSet<string>();
            for (int i = 0; i < result.Rows.Count; i++)
            {
                names.Add(result.Rows[i]["name"]?.ToString() ?? "");
            }
            Assert.IsTrue(names.Contains("Alice"), "Missing Alice");
            Assert.IsTrue(names.Contains("Bob"), "Missing Bob");
        }

        // ================================================================== //
        //  InsertAsync — explicit SaveMode.Overwrite
        // ================================================================== //

        [TestMethod]
        public async Task InsertAsync_ExplicitOverwriteMode_ReplacesData()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("value", "string"),
            });

            // First write
            var batch1 = ArrowConverter.FromRows(new[] { new object[] { "first" } }, schema);
            await _client!.InsertAsync(tablePath, batch1.Schema, ArrowConverter.ToAsyncEnumerable(batch1));

            // Explicit overwrite (not relying on default)
            var batch2 = ArrowConverter.FromRows(new[] { new object[] { "second" } }, schema);
            await _client.InsertAsync(tablePath, batch2.Schema, ArrowConverter.ToAsyncEnumerable(batch2),
                mode: SaveMode.Overwrite);

            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
            Assert.AreEqual(1, dt.Rows.Count, "Explicit Overwrite should replace data.");
            Assert.AreEqual("second", dt.Rows[0]["value"]?.ToString());
        }

        // ================================================================== //
        //  Timestamp round-trip — tz-aware (DateTimeOffset → timestamp)
        // ================================================================== //

        [TestMethod]
        public async Task InsertTimestamp_DateTimeOffset_RoundTrips()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("ts", "timestamp"),
            });

            var ts1 = new DateTimeOffset(2024, 6, 15, 10, 30, 0, TimeSpan.Zero);
            var ts2 = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

            var batch = ArrowConverter.FromRows(new object[][]
            {
                new object[] { 1, ts1 },
                new object[] { 2, ts2 },
            }, schema);

            await _client!.InsertAsync(tablePath, batch.Schema,
                ArrowConverter.ToAsyncEnumerable(batch));

            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();

            Assert.AreEqual(2, dt.Rows.Count, "Expected 2 rows.");

            var rowsById = new Dictionary<int, DataRow>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                int id = Convert.ToInt32(dt.Rows[i]["id"]);
                rowsById[id] = dt.Rows[i];
            }

            // tz-aware timestamps should come back as DateTimeOffset
            Assert.IsInstanceOfType(rowsById[1]["ts"], typeof(DateTimeOffset),
                "tz-aware timestamp should deserialize as DateTimeOffset.");
            Assert.AreEqual(ts1, (DateTimeOffset)rowsById[1]["ts"],
                "Timestamp for id=1 should match.");
            Assert.AreEqual(ts2, (DateTimeOffset)rowsById[2]["ts"],
                "Timestamp for id=2 should match.");
        }

        // ================================================================== //
        //  Timestamp round-trip — tz-naive (DateTime → timestamp_ntz)
        // ================================================================== //

        [TestMethod]
        public async Task InsertTimestampNtz_DateTime_RoundTrips()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("ts", "timestamp_ntz"),
            });

            var dt1 = new DateTime(2024, 6, 15, 10, 30, 0, DateTimeKind.Unspecified);
            var dt2 = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

            var batch = ArrowConverter.FromRows(new object[][]
            {
                new object[] { 1, dt1 },
                new object[] { 2, dt2 },
            }, schema);

            await _client!.InsertAsync(tablePath, batch.Schema,
                ArrowConverter.ToAsyncEnumerable(batch));

            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();

            Assert.AreEqual(2, dt.Rows.Count, "Expected 2 rows.");

            var rowsById = new Dictionary<int, DataRow>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                int id = Convert.ToInt32(dt.Rows[i]["id"]);
                rowsById[id] = dt.Rows[i];
            }

            // tz-naive timestamps should come back as DateTime
            Assert.IsInstanceOfType(rowsById[1]["ts"], typeof(DateTime),
                "tz-naive timestamp should deserialize as DateTime.");
            Assert.AreEqual(dt1, (DateTime)rowsById[1]["ts"],
                "Timestamp for id=1 should match.");
            Assert.AreEqual(dt2, (DateTime)rowsById[2]["ts"],
                "Timestamp for id=2 should match.");
            Assert.AreEqual(DateTimeKind.Unspecified, ((DateTime)rowsById[1]["ts"]).Kind,
                "tz-naive DateTime should have Kind=Unspecified.");
            Assert.AreEqual(DateTimeKind.Unspecified, ((DateTime)rowsById[2]["ts"]).Kind,
                "tz-naive DateTime should have Kind=Unspecified.");
        }

        // ================================================================== //
        //  Timestamp with null values
        // ================================================================== //

        [TestMethod]
        public async Task InsertTimestamp_NullValues_PreservesNulls()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("ts", "timestamp"),
            });

            var ts1 = new DateTimeOffset(2024, 3, 14, 12, 0, 0, TimeSpan.Zero);

            var batch = ArrowConverter.FromRows(new object[][]
            {
                new object[] { 1, ts1 },
                new object[] { 2, null! },
            }, schema);

            await _client!.InsertAsync(tablePath, batch.Schema,
                ArrowConverter.ToAsyncEnumerable(batch));

            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();

            Assert.AreEqual(2, dt.Rows.Count, "Expected 2 rows.");

            var rowsById = new Dictionary<int, DataRow>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                int id = Convert.ToInt32(dt.Rows[i]["id"]);
                rowsById[id] = dt.Rows[i];
            }

            Assert.IsInstanceOfType(rowsById[1]["ts"], typeof(DateTimeOffset),
                "Non-null timestamp should be DateTimeOffset.");
            Assert.AreEqual(ts1, (DateTimeOffset)rowsById[1]["ts"]);
            Assert.IsTrue(rowsById[2]["ts"] == DBNull.Value || rowsById[2]["ts"] is null,
                "Null timestamp should be DBNull or null.");
        }

        // ================================================================== //
        //  Schema reports correct timestamp types
        // ================================================================== //

        [TestMethod]
        public async Task CreateTable_TimestampColumns_SchemaReportsCorrectTypes()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("ts_aware", "timestamp"),
                new ColumnDefinition("ts_naive", "timestamp_ntz"),
            });

            var batch = ArrowConverter.FromRows(new object[][]
            {
                new object[]
                {
                    1,
                    new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero),
                    new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Unspecified),
                },
            }, schema);

            await _client!.InsertAsync(tablePath, batch.Schema,
                ArrowConverter.ToAsyncEnumerable(batch));

            TableSchema readSchema = await _client.GetSchemaAsync(tablePath);

            Assert.AreEqual(3, readSchema.Columns.Count, "Expected 3 columns.");

            var colsByName = new Dictionary<string, ColumnDefinition>();
            foreach (var col in readSchema.Columns)
                colsByName[col.Name] = col;

            Assert.AreEqual("int32", colsByName["id"].DataType,
                "id column should be int32.");

            // V1 (PySpark): timestamp columns come back with their Delta type
            // ts_aware → "timestamp", ts_naive → "timestamp_ntz"
            Assert.AreEqual("timestamp", colsByName["ts_aware"].DataType,
                "ts_aware should be 'timestamp'.");
            Assert.AreEqual("timestamp_ntz", colsByName["ts_naive"].DataType,
                "ts_naive should be 'timestamp_ntz'.");
        }

        // ================================================================== //
        //  Helper
        // ================================================================== //

        /// <summary>
        /// Builds a simple 2-column (int32 id, string value) <see cref="Apache.Arrow.RecordBatch"/>.
        /// </summary>
        private static Apache.Arrow.RecordBatch BuildBatch(
            Apache.Arrow.Schema schema, int[] ids, string[] values)
        {
            var idBuilder = new Apache.Arrow.Int32Array.Builder();
            var valueBuilder = new Apache.Arrow.StringArray.Builder();
            foreach (int id in ids) idBuilder.Append(id);
            foreach (string v in values) valueBuilder.Append(v);
            return new Apache.Arrow.RecordBatch(schema,
                new Apache.Arrow.IArrowArray[] { idBuilder.Build(), valueBuilder.Build() }, ids.Length);
        }

        /// <summary>
        /// Resolves the path to the DeltaLakeSharp.Server directory containing the Dockerfile.
        /// The Server files are copied to the test output directory at build time via the .csproj.
        /// </summary>
        private static string GetDockerfilePath()
        {
            string assemblyDir = Path.GetDirectoryName(
                typeof(DeltaLakeSharpV1IntegrationTests).Assembly.Location)!;
            return Path.Combine(assemblyDir, "DeltaLakeSharp.Server");
        }

        // ================================================================== //
        //  Partition support tests
        // ================================================================== //

        [TestMethod]
        public async Task CreatePartitionedTable_InsertAndReadBack_DataMatches()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("region", "string"),
                new ColumnDefinition("value", "double"),
            });

            ExecuteResult createResult = await _client!.CreateTableAsync(
                tablePath, schema, partitionBy: new[] { "region" });
            Assert.IsTrue(createResult.Success, $"CreateTable failed: {createResult.Message}");

            var rows = new[]
            {
                new object[] { 1, "US", 10.5 },
                new object[] { 2, "EU", 20.3 },
                new object[] { 3, "US", 30.1 },
                new object[] { 4, "APAC", 40.7 },
            };

            var batch = ArrowConverter.FromRows(rows, schema);
            await _client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                partitionBy: new[] { "region" });

            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();

            Assert.AreEqual(4, dt.Rows.Count, "Expected 4 rows in partitioned table.");
            Assert.IsTrue(dt.Columns.Contains("id"), "Missing 'id' column.");
            Assert.IsTrue(dt.Columns.Contains("region"), "Missing 'region' column.");
            Assert.IsTrue(dt.Columns.Contains("value"), "Missing 'value' column.");

            var rowsByRegion = new Dictionary<string, List<DataRow>>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string region = dt.Rows[i]["region"]?.ToString() ?? "";
                if (!rowsByRegion.ContainsKey(region))
                    rowsByRegion[region] = new List<DataRow>();
                rowsByRegion[region].Add(dt.Rows[i]);
            }

            Assert.AreEqual(2, rowsByRegion["US"].Count, "Expected 2 US rows.");
            Assert.AreEqual(1, rowsByRegion["EU"].Count, "Expected 1 EU row.");
            Assert.AreEqual(1, rowsByRegion["APAC"].Count, "Expected 1 APAC row.");
        }

        [TestMethod]
        public async Task PartitionedTable_SqlFilterOnPartitionColumn_ReturnsFilteredRows()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("region", "string"),
                new ColumnDefinition("amount", "double"),
            });

            var rows = new[]
            {
                new object[] { 1, "US", 100.0 },
                new object[] { 2, "EU", 200.0 },
                new object[] { 3, "US", 300.0 },
                new object[] { 4, "APAC", 400.0 },
                new object[] { 5, "EU", 500.0 },
            };

            var batch = ArrowConverter.FromRows(rows, schema);
            await _client!.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                partitionBy: new[] { "region" });

            // Query filtering on the partition column.
            string viewName = $"part_view_{Guid.NewGuid():N}";
            await _client.ExecuteQueryAsync(
                $"CREATE OR REPLACE TEMPORARY VIEW {viewName} USING delta OPTIONS (path '{tablePath}')").ToListAsync();

            List<Apache.Arrow.RecordBatch> batches = await _client.ExecuteQueryAsync(
                $"SELECT id, amount FROM {viewName} WHERE region = 'US' ORDER BY id").ToListAsync();

            Assert.IsTrue(batches.Count > 0, "Expected at least one RecordBatch.");

            DataTable dt = ArrowConverter.ToDataTable(batches);
            Assert.AreEqual(2, dt.Rows.Count, "Expected 2 rows for region='US'.");

            var ids = new List<long>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                ids.Add(Convert.ToInt64(dt.Rows[i]["id"]));
            }
            CollectionAssert.AreEquivalent(new[] { 1L, 3L }, ids, "Expected ids 1 and 3 for US region.");
        }

        [TestMethod]
        public async Task PartitionedTable_AppendData_AllRowsPresent()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("region", "string"),
                new ColumnDefinition("name", "string"),
            });

            // First write (overwrite) — creates the partitioned table.
            var rows1 = new[]
            {
                new object[] { 1, "US", "Alice" },
                new object[] { 2, "EU", "Bob" },
            };
            var batch1 = ArrowConverter.FromRows(rows1, schema);
            await _client!.InsertAsync(tablePath, batch1.Schema, ArrowConverter.ToAsyncEnumerable(batch1),
                mode: SaveMode.Overwrite, partitionBy: new[] { "region" });

            // Second write (append) — adds rows to existing partitioned table.
            var rows2 = new[]
            {
                new object[] { 3, "US", "Charlie" },
                new object[] { 4, "APAC", "Diana" },
            };
            var batch2 = ArrowConverter.FromRows(rows2, schema);
            await _client.InsertAsync(tablePath, batch2.Schema, ArrowConverter.ToAsyncEnumerable(batch2),
                mode: SaveMode.Append);

            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
            Assert.AreEqual(4, dt.Rows.Count, "Expected 4 rows after append.");

            var names = new HashSet<string>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                names.Add(dt.Rows[i]["name"]?.ToString() ?? "");
            }

            Assert.IsTrue(names.Contains("Alice"), "Missing Alice.");
            Assert.IsTrue(names.Contains("Bob"), "Missing Bob.");
            Assert.IsTrue(names.Contains("Charlie"), "Missing Charlie.");
            Assert.IsTrue(names.Contains("Diana"), "Missing Diana.");
        }

        [TestMethod]
        public async Task PartitionedTable_GetSchema_ReturnsAllColumnsIncludingPartition()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("region", "string"),
                new ColumnDefinition("value", "double"),
            });

            ExecuteResult createResult = await _client!.CreateTableAsync(
                tablePath, schema, partitionBy: new[] { "region" });
            Assert.IsTrue(createResult.Success, $"CreateTable failed: {createResult.Message}");

            TableSchema readSchema = await _client.GetSchemaAsync(tablePath);
            Assert.AreEqual(3, readSchema.Columns.Count, "Expected 3 columns in schema.");

            var columnNames = readSchema.Columns.Select(c => c.Name).ToList();
            CollectionAssert.Contains(columnNames, "id", "Missing 'id' column in schema.");
            CollectionAssert.Contains(columnNames, "region", "Missing 'region' column in schema.");
            CollectionAssert.Contains(columnNames, "value", "Missing 'value' column in schema.");
        }

        [TestMethod]
        public async Task PartitionedTable_SqlAggregateByPartitionColumn_CorrectResults()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("region", "string"),
                new ColumnDefinition("amount", "double"),
            });

            var rows = new[]
            {
                new object[] { 1, "US", 100.0 },
                new object[] { 2, "US", 200.0 },
                new object[] { 3, "EU", 50.0 },
                new object[] { 4, "EU", 150.0 },
                new object[] { 5, "APAC", 300.0 },
            };

            var batch = ArrowConverter.FromRows(rows, schema);
            await _client!.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                partitionBy: new[] { "region" });

            string viewName = $"agg_view_{Guid.NewGuid():N}";
            await _client.ExecuteQueryAsync(
                $"CREATE OR REPLACE TEMPORARY VIEW {viewName} USING delta OPTIONS (path '{tablePath}')").ToListAsync();

            List<Apache.Arrow.RecordBatch> batches = await _client.ExecuteQueryAsync(
                $"SELECT region, SUM(amount) AS total FROM {viewName} GROUP BY region ORDER BY region").ToListAsync();

            DataTable dt = ArrowConverter.ToDataTable(batches);
            Assert.AreEqual(3, dt.Rows.Count, "Expected 3 groups (APAC, EU, US).");

            var totals = new Dictionary<string, double>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string region = dt.Rows[i]["region"]?.ToString() ?? "";
                totals[region] = Convert.ToDouble(dt.Rows[i]["total"]);
            }

            Assert.AreEqual(300.0, totals["US"], 0.01, "US total should be 300.");
            Assert.AreEqual(200.0, totals["EU"], 0.01, "EU total should be 200.");
            Assert.AreEqual(300.0, totals["APAC"], 0.01, "APAC total should be 300.");
        }

        [TestMethod]
        public async Task PartitionedTable_MultiplePartitionColumns_DataMatches()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("region", "string"),
                new ColumnDefinition("category", "string"),
                new ColumnDefinition("amount", "double"),
            });

            var rows = new[]
            {
                new object[] { 1, "US", "A", 10.0 },
                new object[] { 2, "US", "B", 20.0 },
                new object[] { 3, "EU", "A", 30.0 },
                new object[] { 4, "EU", "B", 40.0 },
            };

            var batch = ArrowConverter.FromRows(rows, schema);
            await _client!.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                partitionBy: new[] { "region", "category" });

            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
            Assert.AreEqual(4, dt.Rows.Count, "Expected 4 rows with multi-column partitioning.");

            // Query filtering on both partition columns.
            string viewName = $"multi_part_{Guid.NewGuid():N}";
            await _client.ExecuteQueryAsync(
                $"CREATE OR REPLACE TEMPORARY VIEW {viewName} USING delta OPTIONS (path '{tablePath}')").ToListAsync();

            List<Apache.Arrow.RecordBatch> batches = await _client.ExecuteQueryAsync(
                $"SELECT id, amount FROM {viewName} WHERE region = 'US' AND category = 'B'").ToListAsync();

            DataTable filtered = ArrowConverter.ToDataTable(batches);
            Assert.AreEqual(1, filtered.Rows.Count, "Expected 1 row for region='US', category='B'.");
            Assert.AreEqual(2L, Convert.ToInt64(filtered.Rows[0]["id"]), "Expected id=2.");
        }

        // ================================================================== //
        //  Protocol upgrade
        // ================================================================== //

        [TestMethod]
        public async Task UpgradeTableProtocol_BumpsReaderAndWriterVersion()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            // Create a plain table (default protocol: reader=1, writer=2 in Spark).
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });
            ExecuteResult createResult = await _client!.CreateTableAsync(tablePath, schema);
            Assert.IsTrue(createResult.Success, $"CreateTable failed: {createResult.Message}");

            // Upgrade protocol to reader=2, writer=5.
            ExecuteResult upgradeResult = await _client.UpgradeTableProtocolAsync(
                tablePath, readerVersion: 2, writerVersion: 5);
            Assert.IsTrue(upgradeResult.Success, $"UpgradeProtocol failed: {upgradeResult.Message}");

            // Verify the protocol versions from the result.
            Assert.IsNotNull(upgradeResult.Result, "Result should contain protocol info.");
            Assert.IsTrue(upgradeResult.Result.Count > 0, "Result should have at least one row.");
            var proto = upgradeResult.Result[0];
            Assert.AreEqual(2L, Convert.ToInt64(proto["minReaderVersion"]),
                "Expected minReaderVersion=2 after upgrade.");
            Assert.IsTrue(Convert.ToInt64(proto["minWriterVersion"]) >= 5,
                $"Expected minWriterVersion>=5 after upgrade, got {proto["minWriterVersion"]}.");
        }

        [TestMethod]
        public async Task UpgradeTableProtocol_PreservesExistingTableFeatures()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            // Create a plain table (no appendOnly yet) so we can insert data first.
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("value", "double"),
            });
            ExecuteResult createResult = await _client!.CreateTableAsync(tablePath, schema);
            Assert.IsTrue(createResult.Success, $"CreateTable failed: {createResult.Message}");

            // Insert some data so the table is non-empty.
            var batch = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 1, 10.0 },
                    new object[] { 2, 20.0 },
                }, schema);
            await _client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch));

            // Upgrade protocol and enable the appendOnly writer feature.
            // This tests that feature enablement works AND data survives.
            ExecuteResult upgradeResult = await _client.UpgradeTableProtocolAsync(
                tablePath, readerVersion: 1, writerVersion: 5,
                writerFeatures: new List<string> { "appendOnly" });
            Assert.IsTrue(upgradeResult.Success, $"UpgradeProtocol failed: {upgradeResult.Message}");

            // Verify protocol versions from the upgrade result.
            Assert.IsNotNull(upgradeResult.Result, "Result should contain protocol info.");
            Assert.IsTrue(upgradeResult.Result.Count > 0, "Result should have at least one row.");
            var proto = upgradeResult.Result[0];
            long writerVersion = Convert.ToInt64(proto["minWriterVersion"]);
            Assert.IsTrue(writerVersion >= 5,
                $"Expected minWriterVersion>=5 after upgrade, got {writerVersion}.");

            // Verify tableFeatures contains appendOnly.
            Assert.IsTrue(proto.ContainsKey("tableFeatures"),
                "Expected tableFeatures in protocol info after feature upgrade.");
            string tableFeatures = proto["tableFeatures"].ToString()!;
            Assert.IsTrue(tableFeatures.Contains("appendOnly"),
                $"Expected tableFeatures to contain 'appendOnly', got: {tableFeatures}");

            // Verify configuration contains the companion property.
            Assert.IsTrue(proto.ContainsKey("metadata.configuration"),
                "Expected metadata.configuration in protocol info after feature upgrade.");
            string configuration = proto["metadata.configuration"].ToString()!;
            Assert.IsTrue(configuration.Contains("delta.appendOnly"),
                $"Expected metadata.configuration to contain 'delta.appendOnly', got: {configuration}");

            // Verify the data is still readable after protocol upgrade.
            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
            Assert.AreEqual(2, dt.Rows.Count, "Expected 2 rows after protocol upgrade.");
        }

        [TestMethod]
        public async Task UpgradeTableProtocol_EnablesColumnMapping_DataStillReadable()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            // Create a table and insert data before enabling columnMapping.
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });
            ExecuteResult createResult = await _client!.CreateTableAsync(tablePath, schema);
            Assert.IsTrue(createResult.Success, $"CreateTable failed: {createResult.Message}");

            var batch = ArrowConverter.FromRows(
                new[]
                {
                    new object[] { 1, "Alice" },
                    new object[] { 2, "Bob" },
                }, schema);
            await _client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch));

            // Upgrade protocol and enable the columnMapping reader+writer feature.
            // columnMapping requires minReaderVersion >= 2 and minWriterVersion >= 5.
            // This exercises the companion-property path (delta.columnMapping.mode = 'name')
            // rather than the delta.feature.columnMapping = 'supported' path.
            ExecuteResult upgradeResult = await _client.UpgradeTableProtocolAsync(
                tablePath, readerVersion: 2, writerVersion: 5,
                readerFeatures: new List<string> { "columnMapping" },
                writerFeatures: new List<string> { "columnMapping" });
            Assert.IsTrue(upgradeResult.Success, $"UpgradeProtocol failed: {upgradeResult.Message}");

            // Verify tableFeatures contains columnMapping.
            Assert.IsNotNull(upgradeResult.Result, "Result should contain protocol info.");
            Assert.IsTrue(upgradeResult.Result.Count > 0, "Result should have at least one row.");
            var cmProto = upgradeResult.Result[0];
            Assert.IsTrue(cmProto.ContainsKey("tableFeatures"),
                "Expected tableFeatures in protocol info after columnMapping upgrade.");
            string cmTableFeatures = cmProto["tableFeatures"].ToString()!;
            Assert.IsTrue(cmTableFeatures.Contains("columnMapping"),
                $"Expected tableFeatures to contain 'columnMapping', got: {cmTableFeatures}");

            // Verify configuration contains the companion property.
            Assert.IsTrue(cmProto.ContainsKey("metadata.configuration"),
                "Expected metadata.configuration in protocol info after columnMapping upgrade.");
            string cmConfiguration = cmProto["metadata.configuration"].ToString()!;
            Assert.IsTrue(cmConfiguration.Contains("delta.columnMapping.mode"),
                $"Expected metadata.configuration to contain 'delta.columnMapping.mode', got: {cmConfiguration}");

            // Verify the data is still readable after columnMapping is enabled.
            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
            Assert.AreEqual(2, dt.Rows.Count, "Expected 2 rows after columnMapping upgrade.");
            Assert.AreEqual("Alice", dt.Rows[0]["name"]?.ToString());
        }

        [TestMethod]
        public async Task UpgradeTableProtocol_EnablesChangeDataFeed()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            // Create a plain table.
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("value", "double"),
            });
            ExecuteResult createResult = await _client!.CreateTableAsync(tablePath, schema);
            Assert.IsTrue(createResult.Success, $"CreateTable failed: {createResult.Message}");

            // Upgrade protocol and enable changeDataFeed.
            // This exercises the companion-property path (delta.enableChangeDataFeed = 'true').
            ExecuteResult upgradeResult = await _client.UpgradeTableProtocolAsync(
                tablePath, readerVersion: 1, writerVersion: 5,
                writerFeatures: new List<string> { "changeDataFeed" });
            Assert.IsTrue(upgradeResult.Success, $"UpgradeProtocol failed: {upgradeResult.Message}");

            // Verify the protocol was upgraded and feature is in tableFeatures.
            Assert.IsNotNull(upgradeResult.Result, "Result should contain protocol info.");
            Assert.IsTrue(upgradeResult.Result.Count > 0, "Result should have at least one row.");
            var cdfProto = upgradeResult.Result[0];
            Assert.IsTrue(cdfProto.ContainsKey("tableFeatures"),
                "Expected tableFeatures in protocol info after CDF upgrade.");
            string cdfTableFeatures = cdfProto["tableFeatures"].ToString()!;
            Assert.IsTrue(cdfTableFeatures.Contains("changeDataFeed"),
                $"Expected tableFeatures to contain 'changeDataFeed', got: {cdfTableFeatures}");

            // Verify configuration contains the companion property.
            Assert.IsTrue(cdfProto.ContainsKey("metadata.configuration"),
                "Expected metadata.configuration in protocol info after CDF upgrade.");
            string cdfConfiguration = cdfProto["metadata.configuration"].ToString()!;
            Assert.IsTrue(cdfConfiguration.Contains("delta.enableChangeDataFeed"),
                $"Expected metadata.configuration to contain 'delta.enableChangeDataFeed', got: {cdfConfiguration}");
        }

        // ================================================================== //
        //  Create table with CDF enabled, insert data, read back
        // ================================================================== //

        [TestMethod]
        [TestCategory("Integration")]
        public async Task CreateTableWithChangeDataFeed_InsertAndReadBack_Succeeds()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });

            var config = new Dictionary<string, string>
            {
                ["delta.enableChangeDataFeed"] = "true",
            };

            // Create table with CDF enabled at creation time.
            ExecuteResult createResult = await _client!.CreateTableAsync(tablePath, schema, config);
            Assert.IsTrue(createResult.Success, $"CreateTable failed: {createResult.Message}");

            // Insert data into the CDF-enabled table.
            var rows = new[]
            {
                new object[] { 1, "Alice" },
                new object[] { 2, "Bob" },
                new object[] { 3, "Charlie" },
            };
            var batch = ArrowConverter.FromRows(rows, schema);
            await _client.InsertAsync(tablePath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                mode: SaveMode.Append);

            // Read back and verify — normal reads should not include CDF metadata columns.
            DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();

            Assert.AreEqual(3, dt.Rows.Count, "Expected 3 rows.");
            Assert.AreEqual(2, dt.Columns.Count,
                "Expected 2 columns (id, name) — CDF metadata columns should not appear in normal reads.");

            // Verify data (order may vary in Spark, so key by name).
            var rowsByName = new Dictionary<string, DataRow>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string name = dt.Rows[i]["name"]?.ToString() ?? "";
                rowsByName[name] = dt.Rows[i];
            }

            Assert.IsTrue(rowsByName.ContainsKey("Alice"), "Missing Alice");
            Assert.IsTrue(rowsByName.ContainsKey("Bob"), "Missing Bob");
            Assert.IsTrue(rowsByName.ContainsKey("Charlie"), "Missing Charlie");
            Assert.AreEqual(1L, Convert.ToInt64(rowsByName["Alice"]["id"]), "Alice should have id=1");
            Assert.AreEqual(2L, Convert.ToInt64(rowsByName["Bob"]["id"]), "Bob should have id=2");
            Assert.AreEqual(3L, Convert.ToInt64(rowsByName["Charlie"]["id"]), "Charlie should have id=3");
        }

        [TestMethod]
        [TestCategory("Integration")]
        public async Task CreateTableWithTypeWidening_AlterColumnAndReadBack_UpcastsValues()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";
            string tableName = $"type_widening_{Guid.NewGuid():N}";

            try
            {
                await _client!.ExecuteQueryAsync(
                    $"CREATE TABLE {tableName} (id INT, name STRING) USING DELTA LOCATION '{tablePath}' TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
                    .ToListAsync();

                await _client.ExecuteQueryAsync(
                    $"INSERT INTO {tableName} VALUES (1, 'Alice'), (2, 'Bob')")
                    .ToListAsync();

                try
                {
                    await _client.ExecuteQueryAsync(
                        $"ALTER TABLE {tableName} ALTER COLUMN id TYPE BIGINT")
                        .ToListAsync();
                }
                catch (Exception ex) when (ex.Message.Contains(
                    "DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP",
                    StringComparison.OrdinalIgnoreCase))
                {
                    Assert.Inconclusive(
                        "The current V1 test image uses Delta Lake 3.2.1, which rejects INT-to-BIGINT ALTER COLUMN type widening even when delta.enableTypeWidening is enabled.");
                }

                TableSchema widenedSchema = await _client.GetSchemaAsync(tablePath);
                Assert.AreEqual(2, widenedSchema.Columns.Count, "Expected 2 columns after type widening.");
                string widenedIdType = widenedSchema.Columns[0].DataType.ToLowerInvariant();
                Assert.IsTrue(
                    widenedIdType.Contains("long") || widenedIdType.Contains("int64") || widenedIdType.Contains("bigint"),
                    $"Expected widened id column to be bigint/int64/long, got '{widenedSchema.Columns[0].DataType}'.");

                DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
                Assert.AreEqual(2, dt.Rows.Count, "Expected 2 rows after type widening.");
                Assert.AreEqual(typeof(long), dt.Columns["id"].DataType,
                    $"Expected DataTable id column to be Int64 after widening, got {dt.Columns["id"].DataType}.");

                var rowsByName = new Dictionary<string, DataRow>();
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    string name = dt.Rows[i]["name"]?.ToString() ?? "";
                    rowsByName[name] = dt.Rows[i];
                }

                Assert.IsTrue(rowsByName.ContainsKey("Alice"), "Missing Alice");
                Assert.IsTrue(rowsByName.ContainsKey("Bob"), "Missing Bob");
                Assert.AreEqual(1L, (long)rowsByName["Alice"]["id"], "Alice should have widened id=1.");
                Assert.AreEqual(2L, (long)rowsByName["Bob"]["id"], "Bob should have widened id=2.");
            }
            finally
            {
                try
                {
                    await _client!.ExecuteQueryAsync($"DROP TABLE IF EXISTS {tableName}").ToListAsync();
                }
                catch
                {
                }
            }
        }

        [TestMethod]
        [TestCategory("Integration")]
        public async Task CreateTableWithV2Checkpoint_GenerateCheckpointAndReadBack_Succeeds()
        {
            string tablePath = $"/tmp/delta_test_{Guid.NewGuid():N}";
            string tableName = $"checkpoint_v2_{Guid.NewGuid():N}";

            try
            {
                await _client!.ExecuteQueryAsync(
                    $"CREATE TABLE {tableName} (id INT, name STRING) USING DELTA LOCATION '{tablePath}' TBLPROPERTIES ('delta.checkpointPolicy' = 'v2', 'delta.checkpoint.writeStatsAsStruct' = 'true')")
                    .ToListAsync();

                // Delta checkpoints are typically emitted periodically rather than immediately.
                // Drive the table past the normal checkpoint interval so the V2 checkpoint-enabled
                // table has an opportunity to materialize a checkpoint before we validate reads.
                for (int commit = 0; commit < 11; commit++)
                {
                    int id = commit + 1;
                    string name = $"name_{id}";
                    await _client.ExecuteQueryAsync(
                        $"INSERT INTO {tableName} VALUES ({id}, '{name}')")
                        .ToListAsync();
                }

                DataTable dt = await _client.ReadTableAsync(tablePath).ToDataTableAsync();
                Assert.AreEqual(11, dt.Rows.Count, "Expected 11 rows after repeated commits on the V2 checkpoint table.");

                var rowsByName = new Dictionary<string, DataRow>();
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    string name = dt.Rows[i]["name"]?.ToString() ?? "";
                    rowsByName[name] = dt.Rows[i];
                }

                for (int id = 1; id <= 11; id++)
                {
                    string name = $"name_{id}";
                    Assert.IsTrue(rowsByName.ContainsKey(name), $"Missing {name}");
                    Assert.AreEqual((long)id, Convert.ToInt64(rowsByName[name]["id"]), $"{name} should have id={id}");
                }
            }
            finally
            {
                try
                {
                    await _client!.ExecuteQueryAsync($"DROP TABLE IF EXISTS {tableName}").ToListAsync();
                }
                catch
                {
                }
            }
        }

    }
}
