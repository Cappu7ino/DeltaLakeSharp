// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Apache.Arrow;
using Azure;
using Azure.Storage.Files.DataLake;
using Microsoft.DI.DeltaTableService.Client;
using Microsoft.DI.DeltaTableService.Client.Models;
using Microsoft.DI.DeltaTableService.Testing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.DI.DeltaTableService.Tests
{
    /// <summary>
    /// Shared base class for OneLake end-to-end integration tests.
    /// Handles Docker container lifecycle and health checks.  Subclasses
    /// specify the backend version (V1/PySpark or V2/DataFusion).
    ///
    /// <para>
    /// Each test method owns its OneLake configuration (workspace, lakehouse,
    /// table) and acquires its own SAS token via
    /// <see cref="GetOneLakeTableConfigAsync"/>.  This allows different test
    /// methods to target different Delta tables without sharing state.
    /// </para>
    ///
    /// <para>
    /// Authentication uses <c>DefaultAzureCredential</c> — ensure you are
    /// logged in via <c>az login</c> or have a managed identity available.
    /// </para>
    /// </summary>
    public abstract class OneLakeIntegrationTestBase
    {
        /// <summary>
        /// Indicates whether <see cref="InitializeAsync"/> completed successfully.
        /// Individual test methods check this so they can report <c>Inconclusive</c>
        /// rather than a misleading <see cref="NullReferenceException"/> when
        /// initialization fails (e.g. missing credentials).
        /// </summary>
        protected static bool Initialized { get; private set; }

        /// <summary>
        /// Human-readable reason captured when <see cref="InitializeAsync"/> fails.
        /// </summary>
        protected static string InitFailureReason { get; private set; }

        /// <summary>
        /// The Docker container running the Delta Table Service backend.
        /// </summary>
        protected static DeltaTableContainer Container { get; set; }

        /// <summary>
        /// The high-level C# client connected to the running container.
        /// </summary>
        protected static DeltaTableServiceClient Client { get; set; }

        /// <summary>
        /// Tracks OneLake table directories created by write-path tests so
        /// they can be cleaned up in <see cref="CleanupAsync"/>.
        /// Each entry stores the workspace/lakehouse/table identifiers and
        /// the <see cref="StorageConfig"/> (with SAS token) that was used to
        /// create the table — the same SAS is reused for deletion.
        /// </summary>
        private static readonly ConcurrentBag<(string WorkspaceId, string LakehouseId, string TableName, StorageConfig Config)>
            CreatedTables = new();

        // ------------------------------------------------------------------ //
        //  Initialization helper (called by subclass [ClassInitialize])
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Starts the Docker container, creates a client, and waits for the
        /// health check to pass.  OneLake-specific configuration (workspace,
        /// lakehouse, table) is intentionally <b>not</b> handled here — each
        /// test method acquires its own via <see cref="GetOneLakeTableConfigAsync"/>.
        /// </summary>
        /// <param name="mode">The backend mode (V1_Spark or V2_DataFusion).</param>
        protected static async Task InitializeAsync(ServiceMode mode)
        {
            FlightIntegrationTestGuards.EnsureArrowFlightSupported();

            Initialized = false;
            InitFailureReason = null;

            try
            {
                // 1. Start the Docker container from the pre-built image.
                //    The images must be built externally with:
                //      docker build --no-cache -f v1/Dockerfile -t delta-v1-onelake:test <server-dir>
                //      docker build --no-cache -f v2/Dockerfile -t delta-v2-onelake:test <server-dir>
                string imageName = mode == ServiceMode.V2_DataFusion
                    ? "delta-v2-onelake:test"
                    : "delta-v1-onelake:test";

                Container = new DeltaTableContainer();
                await Container.PullAndStartAsync(imageName, mode);

                Client = new DeltaTableServiceClient(Container.GetFlightUri(), mode);

                // 2. Wait for the server to become healthy.
                bool healthy = false;
                int maxAttempts = mode == ServiceMode.V1_Spark ? 60 : 30;
                for (int attempt = 0; attempt < maxAttempts; attempt++)
                {
                    healthy = await Client.HealthCheckAsync();
                    if (healthy) break;
                    await Task.Delay(2000);
                }

                Assert.IsTrue(healthy,
                    $"Delta Table Service ({mode}) did not become healthy within timeout.");

                Initialized = true;
            }
            catch (Exception ex) when (ex is not AssertInconclusiveException)
            {
                InitFailureReason = $"ClassInitialize failed: {ex.Message}";
                throw;
            }
        }

        // ------------------------------------------------------------------ //
        //  Cleanup helper (called by subclass [ClassCleanup])
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Disposes the client and stops the Docker container.
        /// Captures and writes container logs to the test output before disposal
        /// to aid debugging (especially for V2 OneLake issues).
        /// Also cleans up any OneLake test tables created during the run.
        /// </summary>
        protected static async Task CleanupAsync()
        {
            // Delete test tables from OneLake before tearing down anything.
            // This runs while credentials are still valid.
            await DeleteTestTablesAsync();

            Client?.Dispose();
            Client = null;

            if (Container != null)
            {
                try
                {
                    var (stdout, stderr) = await Container.GetLogsAsync();
                    if (!string.IsNullOrWhiteSpace(stdout))
                    {
                        Console.WriteLine("=== Container STDOUT ===");
                        Console.WriteLine(stdout);
                    }
                    if (!string.IsNullOrWhiteSpace(stderr))
                    {
                        Console.WriteLine("=== Container STDERR ===");
                        Console.WriteLine(stderr);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning: Failed to retrieve container logs: {ex.Message}");
                }

                await Container.DisposeAsync();
                Container = null;
            }
        }

        /// <summary>
        /// Deletes OneLake table directories that were created by write-path
        /// tests during this run.  Uses the <c>Azure.Storage.Files.DataLake</c>
        /// SDK to remove each table's subdirectory under
        /// <c>{lakehouseId}/Tables/{tableName}</c>.
        ///
        /// <para>
        /// Safety: the path always includes the table name (a GUID-based
        /// string generated by <see cref="GenerateOneLakeTableName"/>), so
        /// the deletion can never target the parent <c>Tables/</c> directory
        /// or any pre-existing table.
        /// </para>
        ///
        /// <para>
        /// Best-effort: exceptions are logged but not propagated, so cleanup
        /// failures never cause test failures.
        /// </para>
        /// </summary>
        private static async Task DeleteTestTablesAsync()
        {
            while (CreatedTables.TryTake(out var entry))
            {
                try
                {
                    // Build the path to the specific table directory:
                    //   {lakehouseId}/Tables/{tableName}
                    // This MUST target the table subdirectory, NOT the
                    // parent Tables/ directory.
                    string tableDirectoryPath =
                        $"{entry.LakehouseId}/Tables/{entry.TableName}";

                    var dfsUri = new Uri(
                        $"https://{entry.Config.StorageAccount}.dfs.fabric.microsoft.com");
                    var serviceClient = new DataLakeServiceClient(
                        dfsUri,
                        new AzureSasCredential(entry.Config.SasToken));
                    var fsClient = serviceClient.GetFileSystemClient(entry.WorkspaceId);
                    var dirClient = fsClient.GetDirectoryClient(tableDirectoryPath);

                    await dirClient.DeleteAsync();
                    Console.WriteLine($"Cleaned up test table: {entry.TableName}");
                }
                catch (Exception ex)
                {
                    // Best-effort: log but don't fail the test run.
                    Console.WriteLine(
                        $"Warning: Failed to clean up table '{entry.TableName}': {ex.Message}");
                }
            }
        }

        // ------------------------------------------------------------------ //
        //  Guard
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Ensures that <see cref="InitializeAsync"/> completed. Call this at
        /// the beginning of every test method so that tests report
        /// <c>Inconclusive</c> instead of <see cref="NullReferenceException"/>
        /// when initialization failed.
        /// </summary>
        protected static void EnsureInitialized()
        {
            if (!Initialized)
            {
                Assert.Inconclusive(
                    InitFailureReason ??
                    "ClassInitialize did not complete. Check Docker / auth.");
            }
        }

        // ------------------------------------------------------------------ //
        //  Per-test OneLake configuration helper
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Acquires a OneLake SAS token and builds the fully qualified ABFSS
        /// path for the given Delta table.  Each test method calls this with
        /// its own workspace / lakehouse / table values so that different tests
        /// can target different tables independently.
        /// </summary>
        /// <param name="workspaceId">The Fabric workspace ID (GUID string).</param>
        /// <param name="lakehouseId">The Lakehouse artifact ID (GUID string).</param>
        /// <param name="tableName">The name of the Delta table under <c>Tables/</c>.</param>
        /// <param name="environment">
        /// The OneLake environment to target. Defaults to
        /// <see cref="OneLakeEnvironment.Msit"/>.
        /// </param>
        /// <returns>
        /// A tuple containing:
        /// <list type="bullet">
        ///   <item><description>
        ///     <c>StorageConfig</c> — the account name and SAS token ready for
        ///     the <see cref="DeltaTableServiceClient"/> API.
        ///   </description></item>
        ///   <item><description>
        ///     <c>AbfssPath</c> — the fully qualified
        ///     <c>abfss://{workspaceId}@{host}/{lakehouseId}/Tables/{tableName}</c> path.
        ///   </description></item>
        ///   <item><description>
        ///     <c>TableName</c> — the table name (echoed back for convenience).
        ///   </description></item>
        /// </list>
        /// </returns>
        protected static async Task<(StorageConfig StorageConfig, string AbfssPath, string TableName)>
            GetOneLakeTableConfigAsync(
                string workspaceId,
                string lakehouseId,
                string tableName,
                OneLakeEnvironment environment = OneLakeEnvironment.Msit)
        {
            string artifactPath = $"{lakehouseId}/Tables/{tableName}";
            var storageConfig = await OneLakeSasHelper.GetStorageConfigAsync(
                workspaceId,
                artifactPath,
                environment);

            Assert.IsNotNull(storageConfig, "Failed to acquire OneLake SAS token.");
            Assert.IsFalse(
                string.IsNullOrWhiteSpace(storageConfig.SasToken),
                "SAS token should not be empty.");

            // Build the ABFSS path using the DNS account name from StorageConfig.
            string dfsHost = $"{storageConfig.StorageAccount}.dfs.fabric.microsoft.com";
            string abfssPath = $"abfss://{workspaceId}@{dfsHost}/{lakehouseId}/Tables/{tableName}";

            return (storageConfig, abfssPath, tableName);
        }

        // ------------------------------------------------------------------ //
        //  Shared test logic
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Reads the OneLake Delta table via <see cref="DeltaTableServiceClient.ReadTableAsync"/>
        /// and asserts that at least one row is returned.
        /// </summary>
        /// <param name="storageConfig">Per-test OneLake storage config with SAS token.</param>
        /// <param name="abfssPath">Fully qualified ABFSS path to the Delta table.</param>
        protected async Task ReadTable_ReturnsData_Core(
            StorageConfig storageConfig,
            string abfssPath)
        {
            EnsureInitialized();

            DataTable dt = await Client.ReadTableAsync(abfssPath, storageConfig)
                .ToDataTableAsync();

            Assert.IsNotNull(dt, "ReadTableAsync should return a non-null DataTable.");
            Assert.IsTrue(dt.Rows.Count > 0,
                "Expected at least one row from the OneLake Delta table.");
            Assert.IsTrue(dt.Columns.Count > 0,
                "Expected at least one column from the OneLake Delta table.");
        }

        /// <summary>
        /// Retrieves the schema of the OneLake Delta table and asserts that
        /// it has at least one column.
        /// </summary>
        /// <param name="storageConfig">Per-test OneLake storage config with SAS token.</param>
        /// <param name="abfssPath">Fully qualified ABFSS path to the Delta table.</param>
        protected async Task GetSchema_ReturnsColumns_Core(
            StorageConfig storageConfig,
            string abfssPath)
        {
            EnsureInitialized();

            TableSchema schema = await Client.GetSchemaAsync(abfssPath, storageConfig);

            Assert.IsNotNull(schema, "GetSchemaAsync should return a non-null TableSchema.");
            Assert.IsTrue(schema.Columns.Count > 0,
                "Expected at least one column in the OneLake Delta table schema.");

            // Every column should have a non-empty name and data type.
            foreach (var col in schema.Columns)
            {
                Assert.IsFalse(string.IsNullOrWhiteSpace(col.Name),
                    "Column name should not be empty.");
                Assert.IsFalse(string.IsNullOrWhiteSpace(col.DataType),
                    $"Column '{col.Name}' should have a non-empty data type.");
            }
        }

        /// <summary>
        /// Executes a <c>SELECT * FROM {table} LIMIT 10</c> query against the
        /// OneLake Delta table and asserts that rows are returned.
        /// </summary>
        /// <param name="storageConfig">Per-test OneLake storage config with SAS token.</param>
        /// <param name="abfssPath">Fully qualified ABFSS path to the Delta table.</param>
        /// <param name="tableName">Table name used to build the SQL alias.</param>
        protected async Task ExecuteQuery_SelectAll_ReturnsRows_Core(
            StorageConfig storageConfig,
            string abfssPath,
            string tableName)
        {
            EnsureInitialized();

            // Use a unique alias per invocation to avoid state collisions.
            string alias = $"onelake_tbl_{Guid.NewGuid():N}";

            DataTable result = await Client.ExecuteQueryAsync(
                $"SELECT * FROM {alias} LIMIT 10",
                abfssPath,
                alias,
                storageConfig).ToDataTableAsync();

            Assert.IsNotNull(result, "Query should return a non-null DataTable.");
            Assert.IsTrue(result.Rows.Count > 0,
                "Expected at least one row from the SELECT query.");
            Assert.IsTrue(result.Columns.Count > 0,
                "Expected at least one column in the query result.");
        }

        // ------------------------------------------------------------------ //
        //  Write-path helpers
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Generates a unique OneLake table name for write tests.
        /// Each test gets its own table to avoid collisions and protect the
        /// shared <c>alltypestable</c>.
        /// </summary>
        protected static string GenerateOneLakeTableName()
        {
            return $"test_write_{Guid.NewGuid():N}";
        }

        /// <summary>
        /// Convenience overload that creates a unique table name, acquires a
        /// SAS token scoped at the <b>Tables directory</b>, and returns the
        /// full ABFSS path.  The SAS is scoped to
        /// <c>{lakehouseId}/Tables</c> (the pre-existing Tables folder) so
        /// that Spark can create new table subdirectories beneath it.
        /// Scoping to the exact table path would fail because the directory
        /// does not exist yet, and scoping to the lakehouse root is broader
        /// than necessary.
        /// </summary>
        protected static async Task<(StorageConfig StorageConfig, string AbfssPath, string TableName)>
            GetWriteTableConfigAsync(
                string workspaceId = "882a9e47-88c5-42c8-9a0f-ada2892b05eb",
                string lakehouseId = "16d76589-d699-43c1-8a92-eb87eb126caf")
        {
            string tableName = GenerateOneLakeTableName();

            // Scope the SAS at the Tables directory so that Spark can create
            // arbitrary table subdirectories under Tables/.
            string artifactPath = $"{lakehouseId}/Tables";
            var storageConfig = await OneLakeSasHelper.GetStorageConfigAsync(
                workspaceId,
                artifactPath,
                OneLakeEnvironment.Msit);

            Assert.IsNotNull(storageConfig, "Failed to acquire OneLake SAS token.");
            Assert.IsFalse(
                string.IsNullOrWhiteSpace(storageConfig.SasToken),
                "SAS token should not be empty.");

            string dfsHost = $"{storageConfig.StorageAccount}.dfs.fabric.microsoft.com";
            string abfssPath = $"abfss://{workspaceId}@{dfsHost}/{lakehouseId}/Tables/{tableName}";

            // Register this table for cleanup in CleanupAsync().
            CreatedTables.Add((workspaceId, lakehouseId, tableName, storageConfig));

            return (storageConfig, abfssPath, tableName);
        }

        // ------------------------------------------------------------------ //
        //  Shared write-path test logic
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Creates an empty Delta table on OneLake with a simple schema, then
        /// reads back the schema and verifies the columns are correct.
        /// </summary>
        protected async Task CreateTable_GetSchema_ReturnsCorrectColumns_Core(
            StorageConfig storageConfig,
            string abfssPath)
        {
            EnsureInitialized();

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int"),
                new ColumnDefinition("name", "string"),
                new ColumnDefinition("value", "double"),
            });

            ExecuteResult createResult = await Client.CreateTableAsync(
                abfssPath, schema, storageConfig: storageConfig);
            Assert.IsTrue(createResult.Success,
                $"CreateTableAsync failed: {createResult.Message}");

            // Read back schema and verify
            TableSchema readSchema = await Client.GetSchemaAsync(abfssPath, storageConfig);
            Assert.AreEqual(3, readSchema.Columns.Count,
                "Expected 3 columns in the created table schema.");
            Assert.AreEqual("id", readSchema.Columns[0].Name);
            Assert.AreEqual("name", readSchema.Columns[1].Name);
            Assert.AreEqual("value", readSchema.Columns[2].Name);

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

        /// <summary>
        /// Inserts rows into a new OneLake Delta table (Overwrite mode) and
        /// reads them back to verify the data round-trips correctly.
        /// </summary>
        protected async Task InsertAndRead_DataRoundTrips_Core(
            StorageConfig storageConfig,
            string abfssPath)
        {
            EnsureInitialized();

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
            await Client.InsertAsync(
                abfssPath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                storageConfig: storageConfig);

            // Read back and verify
            DataTable dt = await Client.ReadTableAsync(abfssPath, storageConfig)
                .ToDataTableAsync();

            Assert.AreEqual(3, dt.Rows.Count, "Expected 3 rows after insert.");
            Assert.AreEqual(2, dt.Columns.Count, "Expected 2 columns.");

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

        /// <summary>
        /// Inserts initial rows (Overwrite), then appends additional rows
        /// (Append mode) and verifies all rows are present.
        /// </summary>
        protected async Task InsertAppend_AddsRows_Core(
            StorageConfig storageConfig,
            string abfssPath)
        {
            EnsureInitialized();

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("city", "string"),
            });

            // Initial insert (Overwrite — creates the table)
            var batch1 = ArrowConverter.FromRows(new[]
            {
                new object[] { 1, "Seattle" },
                new object[] { 2, "Portland" },
            }, schema);
            await Client.InsertAsync(
                abfssPath, batch1.Schema, ArrowConverter.ToAsyncEnumerable(batch1),
                storageConfig: storageConfig);

            // Append more rows
            var batch2 = ArrowConverter.FromRows(new[]
            {
                new object[] { 3, "Denver" },
            }, schema);
            await Client.InsertAsync(
                abfssPath, batch2.Schema, ArrowConverter.ToAsyncEnumerable(batch2),
                mode: SaveMode.Append,
                storageConfig: storageConfig);

            // Read back and verify all 3 rows present
            DataTable dt = await Client.ReadTableAsync(abfssPath, storageConfig)
                .ToDataTableAsync();

            Assert.AreEqual(3, dt.Rows.Count, "Expected 3 rows after initial insert + append.");

            var cities = new HashSet<string>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                cities.Add(dt.Rows[i]["city"]?.ToString() ?? "");
            }

            Assert.IsTrue(cities.Contains("Seattle"), "Missing Seattle");
            Assert.IsTrue(cities.Contains("Portland"), "Missing Portland");
            Assert.IsTrue(cities.Contains("Denver"), "Missing Denver (appended row)");
        }

        /// <summary>
        /// Creates a table, inserts rows, deletes a subset with a WHERE clause,
        /// and verifies only the expected rows remain.
        /// </summary>
        protected async Task DeleteRows_RemovesMatchingRows_Core(
            StorageConfig storageConfig,
            string abfssPath,
            string tableName)
        {
            EnsureInitialized();

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("value", "string"),
            });

            // Create table with deletion vectors enabled
            var config = new Dictionary<string, string>
            {
                ["delta.enableDeletionVectors"] = "true",
            };

            ExecuteResult createResult = await Client.CreateTableAsync(
                abfssPath, schema, config, storageConfig);
            Assert.IsTrue(createResult.Success,
                $"CreateTableWithConfig failed: {createResult.Message}");

            // Insert rows
            var batch = ArrowConverter.FromRows(new[]
            {
                new object[] { 1, "keep" },
                new object[] { 2, "remove" },
                new object[] { 3, "keep" },
            }, schema);
            await Client.InsertAsync(
                abfssPath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                mode: SaveMode.Append,
                storageConfig: storageConfig);

            // Delete where id = 2
            string delAlias = $"del_{Guid.NewGuid():N}";
            ExecuteResult deleteResult = await Client.DeleteAsync(
                $"DELETE FROM {delAlias} WHERE id = 2",
                abfssPath, delAlias, storageConfig);
            Assert.IsTrue(deleteResult.Success,
                $"DeleteAsync failed: {deleteResult.Message}");

            // Read back and verify
            DataTable dt = await Client.ReadTableAsync(abfssPath, storageConfig)
                .ToDataTableAsync();

            Assert.AreEqual(2, dt.Rows.Count, "Expected 2 rows after deleting id=2.");

            var ids = new HashSet<long>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                ids.Add(Convert.ToInt64(dt.Rows[i]["id"]));
            }

            Assert.IsTrue(ids.Contains(1L), "id=1 should remain");
            Assert.IsTrue(ids.Contains(3L), "id=3 should remain");
            Assert.IsFalse(ids.Contains(2L), "id=2 should have been deleted");
        }

        /// <summary>
        /// Creates a table, inserts rows, updates a subset with a WHERE clause,
        /// and verifies only the matching rows were modified.
        /// </summary>
        protected async Task UpdateRows_ModifiesMatchingRows_Core(
            StorageConfig storageConfig,
            string abfssPath,
            string tableName)
        {
            EnsureInitialized();

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("status", "string"),
            });

            var config = new Dictionary<string, string>
            {
                ["delta.enableDeletionVectors"] = "true",
            };

            ExecuteResult createResult = await Client.CreateTableAsync(
                abfssPath, schema, config, storageConfig);
            Assert.IsTrue(createResult.Success,
                $"CreateTableWithConfig failed: {createResult.Message}");

            var batch = ArrowConverter.FromRows(new[]
            {
                new object[] { 1, "active" },
                new object[] { 2, "active" },
                new object[] { 3, "inactive" },
            }, schema);
            await Client.InsertAsync(
                abfssPath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                mode: SaveMode.Append,
                storageConfig: storageConfig);

            // Update: set status='updated' where id <= 2
            string updAlias = $"upd_{Guid.NewGuid():N}";
            ExecuteResult updateResult = await Client.UpdateAsync(
                $"UPDATE {updAlias} SET status = 'updated' WHERE id <= 2",
                abfssPath, updAlias, storageConfig);
            Assert.IsTrue(updateResult.Success,
                $"UpdateAsync failed: {updateResult.Message}");

            // Read back and verify
            DataTable dt = await Client.ReadTableAsync(abfssPath, storageConfig)
                .ToDataTableAsync();

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

        /// <summary>
        /// Creates a target table, then uses <see cref="DeltaTableServiceClient.MergeDataAsync"/>
        /// to stream source data and perform an upsert (update matched + insert unmatched).
        /// Verifies the merge result.
        /// </summary>
        protected async Task MergeData_UpsertAll_UpdatesAndInserts_Core(
            StorageConfig storageConfig,
            string abfssPath)
        {
            EnsureInitialized();

            var targetSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("value", "string"),
            });

            // Create target table with initial data
            var targetBatch = ArrowConverter.FromRows(new[]
            {
                new object[] { 1, "original_1" },
                new object[] { 2, "original_2" },
                new object[] { 3, "original_3" },
            }, targetSchema);
            await Client.InsertAsync(
                abfssPath, targetBatch.Schema, ArrowConverter.ToAsyncEnumerable(targetBatch),
                storageConfig: storageConfig);

            // Build source Arrow data for merge
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

            // Execute MergeData
            var mergeOptions = new MergeOptions("target.id = source.id")
            {
                WhenMatchedUpdateAll = true,
                WhenNotMatchedInsertAll = true,
            };

            ExecuteResult result = await Client.MergeDataAsync(
                abfssPath, arrowSchema, ArrowConverter.ToAsyncEnumerable(sourceBatch),
                mergeOptions, storageConfig);
            Assert.IsTrue(result.Success, $"MergeDataAsync failed: {result.Message}");

            // Read back and verify
            DataTable dt = await Client.ReadTableAsync(abfssPath, storageConfig)
                .ToDataTableAsync();

            Assert.AreEqual(4, dt.Rows.Count,
                "Expected 4 rows: 3 original + 1 inserted.");

            var rowsById = new Dictionary<long, string>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                long id = Convert.ToInt64(dt.Rows[i]["id"]);
                string value = dt.Rows[i]["value"]?.ToString() ?? "";
                rowsById[id] = value;
            }

            Assert.AreEqual("original_1", rowsById[1L], "id=1 should remain unchanged.");
            Assert.AreEqual("merged_2", rowsById[2L], "id=2 should be updated to 'merged_2'.");
            Assert.AreEqual("original_3", rowsById[3L], "id=3 should remain unchanged.");
            Assert.AreEqual("merged_4", rowsById[4L], "id=4 should be inserted as 'merged_4'.");
        }

        /// <summary>
        /// Creates a Delta table with explicit configuration (deletion vectors
        /// enabled), inserts data, and verifies the table is functional.
        /// </summary>
        protected async Task CreateTableWithConfig_RespectsConfiguration_Core(
            StorageConfig storageConfig,
            string abfssPath)
        {
            EnsureInitialized();

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("label", "string"),
            });

            var config = new Dictionary<string, string>
            {
                ["delta.enableDeletionVectors"] = "true",
            };

            ExecuteResult createResult = await Client.CreateTableAsync(
                abfssPath, schema, config, storageConfig);
            Assert.IsTrue(createResult.Success,
                $"CreateTableAsync failed: {createResult.Message}");

            // Verify schema was created correctly
            TableSchema readSchema = await Client.GetSchemaAsync(abfssPath, storageConfig);
            Assert.AreEqual(2, readSchema.Columns.Count,
                "Expected 2 columns in the configured table schema.");
            Assert.AreEqual("id", readSchema.Columns[0].Name);
            Assert.AreEqual("label", readSchema.Columns[1].Name);

            // Insert some data to verify the table is functional
            var batch = ArrowConverter.FromRows(new[]
            {
                new object[] { 1, "alpha" },
                new object[] { 2, "beta" },
            }, schema);
            await Client.InsertAsync(
                abfssPath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                mode: SaveMode.Append,
                storageConfig: storageConfig);

            DataTable dt = await Client.ReadTableAsync(abfssPath, storageConfig)
                .ToDataTableAsync();

            Assert.AreEqual(2, dt.Rows.Count, "Expected 2 rows after insert into configured table.");
        }

        /// <summary>
        /// Creates a Delta table with change data feed enabled at creation time,
        /// inserts rows, and verifies the rows can be read back normally.
        /// </summary>
        protected async Task CreateTableWithChangeDataFeed_InsertAndReadBack_Core(
            StorageConfig storageConfig,
            string abfssPath)
        {
            EnsureInitialized();

            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });

            var config = new Dictionary<string, string>
            {
                ["delta.enableChangeDataFeed"] = "true",
            };

            ExecuteResult createResult = await Client.CreateTableAsync(
                abfssPath, schema, config, storageConfig);
            Assert.IsTrue(createResult.Success,
                $"CreateTableAsync failed: {createResult.Message}");

            var rows = new[]
            {
                new object[] { 1, "Alice" },
                new object[] { 2, "Bob" },
                new object[] { 3, "Charlie" },
            };
            var batch = ArrowConverter.FromRows(rows, schema);
            await Client.InsertAsync(
                abfssPath, batch.Schema, ArrowConverter.ToAsyncEnumerable(batch),
                mode: SaveMode.Append,
                storageConfig: storageConfig);

            DataTable dt = await Client.ReadTableAsync(abfssPath, storageConfig)
                .ToDataTableAsync();

            Assert.AreEqual(3, dt.Rows.Count, "Expected 3 rows after insert into CDF-enabled table.");
            Assert.AreEqual(2, dt.Columns.Count,
                "Expected 2 columns (id, name); CDF metadata columns should not appear in normal reads.");

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
    }
}
