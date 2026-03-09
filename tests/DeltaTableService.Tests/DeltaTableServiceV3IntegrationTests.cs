// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Microsoft.ADMS.Testing.DeltaTableService.Client;
using Microsoft.ADMS.Testing.DeltaTableService.Client.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.ADMS.Testing.DeltaTableService.Tests
{
    /// <summary>
    /// Integration tests for the Delta Table Service V3 (native Rust binary).
    /// Exercises the Flight server via <see cref="DeltaTableServiceClient"/>
    /// using <see cref="DeltaTableProcess"/> to manage the server lifecycle.
    ///
    /// Phase 1 tests cover health, list-actions, and basic client interactions.
    /// Phase 2 tests cover the read path: ReadTable, GetSchema, ExecuteQuery.
    ///
    /// Run with: dotnet test --filter "TestCategory=Integration&amp;TestCategory=V3Flight"
    /// </summary>
    [TestClass]
    [TestCategory("Integration")]
    [TestCategory("V3")]
    [TestCategory("V3Flight")]
    public class DeltaTableServiceV3IntegrationTests
    {
        private static DeltaTableProcess? _process;
        private static DeltaTableServiceClient? _client;
        private static string? _testTablePath;
        private static string? _partitionedTablePath;
        private static string? _timeTravelTablePath;
        private static string? _tempDir;
        private static string? _fixtureDataDir;

        /// <summary>
        /// Resolves the path to the Rust binary by walking up from the test output
        /// directory to the repo root.
        /// </summary>
        private static string? FindRustBinary()
        {
            string? dir = AppContext.BaseDirectory;
            while (dir != null)
            {
                string solutionFile = Path.Combine(dir, "DeltaTableService.sln");
                if (File.Exists(solutionFile))
                {
                    string binaryPath = Path.Combine(
                        dir, "src", "DeltaTableService.Server", "v3",
                        "target", "debug", "delta-table-service-v3.exe");
                    return File.Exists(binaryPath) ? binaryPath : null;
                }
                dir = Path.GetDirectoryName(dir);
            }

            return null;
        }

        /// <summary>
        /// Resolves the path to the checked-in test fixture data directory by
        /// walking up from the test output directory to the repo root.
        /// Returns null if the directory is not found.
        /// </summary>
        private static string? FindFixtureDataDir()
        {
            string? dir = AppContext.BaseDirectory;
            while (dir != null)
            {
                string solutionFile = Path.Combine(dir, "DeltaTableService.sln");
                if (File.Exists(solutionFile))
                {
                    string dataDir = Path.Combine(
                        dir, "tests", "DeltaTableService.Tests", "data");
                    return Directory.Exists(dataDir) ? dataDir : null;
                }
                dir = Path.GetDirectoryName(dir);
            }

            return null;
        }

        /// <summary>
        /// Creates a test Delta table using the Rust binary's
        /// <c>create-test-fixture</c> subcommand.
        /// </summary>
        /// <param name="binaryPath">Path to the Rust binary.</param>
        /// <param name="tablePath">Directory where the fixture will be created.</param>
        /// <param name="fixtureType">Fixture type: "basic", "partitioned", or "time-travel".</param>
        private static void CreateTestDeltaTable(string binaryPath, string tablePath, string fixtureType = "basic")
        {
            var psi = new ProcessStartInfo
            {
                FileName = binaryPath,
                Arguments = $"create-test-fixture \"{tablePath}\" --fixture-type {fixtureType}",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            using var proc = Process.Start(psi)
                ?? throw new InvalidOperationException("Failed to start create-test-fixture process.");

            string stdout = proc.StandardOutput.ReadToEnd();
            string stderr = proc.StandardError.ReadToEnd();
            proc.WaitForExit(30_000);

            if (proc.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"create-test-fixture failed (exit code {proc.ExitCode}).\n" +
                    $"stdout: {stdout}\nstderr: {stderr}");
            }

            if (!stdout.Contains("TEST_FIXTURE_CREATED"))
            {
                throw new InvalidOperationException(
                    $"create-test-fixture did not print expected sentinel.\n" +
                    $"stdout: {stdout}\nstderr: {stderr}");
            }
        }

        /// <summary>
        /// Creates a test Delta table fixture, then starts the V3 Rust server
        /// once for all integration tests.
        /// The Rust binary must be pre-built via <c>cargo build</c>.
        /// </summary>
        [ClassInitialize]
        public static async Task ClassInitialize(TestContext context)
        {
            string? binaryPath = FindRustBinary();
            if (binaryPath == null)
            {
                Assert.Inconclusive(
                    "Rust binary not found. Build it first: " +
                    "cd src/DeltaTableService.Server/v3 && cargo build");
                return;
            }

            // Create test Delta table fixtures in a temp directory.
            _tempDir = Path.Combine(Path.GetTempPath(), $"v3_test_{Guid.NewGuid():N}");

            // Resolve checked-in fixture data directory.
            _fixtureDataDir = FindFixtureDataDir();

            _testTablePath = Path.Combine(_tempDir, "test_table");
            CreateTestDeltaTable(binaryPath, _testTablePath);

            _partitionedTablePath = Path.Combine(_tempDir, "partitioned_table");
            CreateTestDeltaTable(binaryPath, _partitionedTablePath, "partitioned");

            _timeTravelTablePath = Path.Combine(_tempDir, "time_travel_table");
            CreateTestDeltaTable(binaryPath, _timeTravelTablePath, "time-travel");

            // Start the Flight server.
            _process = new DeltaTableProcess();
            await _process.StartAsync(binaryPath);

            _client = new DeltaTableServiceClient(
                _process.GetFlightUri(), ServiceMode.V3_Rust);

            // Verify health after startup.
            bool healthy = await _client.HealthCheckAsync();
            Assert.IsTrue(healthy, "Delta Table Service V3 did not become healthy.");
        }

        /// <summary>
        /// Stops the Rust server and cleans up the test fixture.
        /// </summary>
        [ClassCleanup]
        public static async Task ClassCleanup()
        {
            _client?.Dispose();
            if (_process != null)
            {
                await _process.DisposeAsync();
            }

            // Clean up test fixture directory.
            if (_tempDir != null && Directory.Exists(_tempDir))
            {
                try { Directory.Delete(_tempDir, recursive: true); }
                catch { /* best-effort cleanup */ }
            }
        }

        // ================================================================== //
        //  Phase 1: Health check
        // ================================================================== //

        [TestMethod]
        public async Task V3_HealthCheck_ReturnsTrue()
        {
            bool healthy = await _client!.HealthCheckAsync();
            Assert.IsTrue(healthy, "Expected the V3 server to report healthy.");
        }

        // ================================================================== //
        //  Phase 1: Service mode
        // ================================================================== //

        [TestMethod]
        public void V3_Client_ReportsCorrectServiceMode()
        {
            Assert.AreEqual(ServiceMode.V3_Rust, _client!.Mode,
                "Expected client to report V3_Rust service mode.");
        }

        // ================================================================== //
        //  Phase 1: ListActions
        // ================================================================== //

        [TestMethod]
        public async Task V3_ListActions_ReturnsExpectedActions()
        {
            // Use a raw Flight client to call ListActions, since
            // DeltaTableServiceClient doesn't expose it directly.
            var channel = Grpc.Net.Client.GrpcChannel.ForAddress(
                _process!.GetFlightUri());
            try
            {
                var flightClient = new Apache.Arrow.Flight.Client.FlightClient(channel);
                var call = flightClient.ListActions();

                var actions = new List<string>();
                while (await call.ResponseStream.MoveNext(default))
                {
                    actions.Add(call.ResponseStream.Current.Type);
                }

                // Phase 1 actions: health, shutdown, create_table, execute_dml, upgrade_protocol
                Assert.IsTrue(actions.Contains("health"),
                    $"Missing 'health' action. Got: [{string.Join(", ", actions)}]");
                Assert.IsTrue(actions.Contains("shutdown"),
                    $"Missing 'shutdown' action. Got: [{string.Join(", ", actions)}]");
                Assert.IsTrue(actions.Contains("create_table"),
                    $"Missing 'create_table' action. Got: [{string.Join(", ", actions)}]");
                Assert.IsTrue(actions.Contains("execute_dml"),
                    $"Missing 'execute_dml' action. Got: [{string.Join(", ", actions)}]");
                Assert.IsTrue(actions.Contains("upgrade_protocol"),
                    $"Missing 'upgrade_protocol' action. Got: [{string.Join(", ", actions)}]");
                Assert.AreEqual(5, actions.Count,
                    $"Expected exactly 5 actions, got {actions.Count}: [{string.Join(", ", actions)}]");
            }
            finally
            {
                channel.Dispose();
            }
        }

        // ================================================================== //
        //  Phase 1: Unknown action returns error
        // ================================================================== //

        [TestMethod]
        public async Task V3_DoAction_UnknownType_ReturnsInvalidArgument()
        {
            var channel = Grpc.Net.Client.GrpcChannel.ForAddress(
                _process!.GetFlightUri());
            try
            {
                var flightClient = new Apache.Arrow.Flight.Client.FlightClient(channel);
                var action = new Apache.Arrow.Flight.FlightAction(
                    "nonexistent_action",
                    Google.Protobuf.ByteString.Empty);

                var call = flightClient.DoAction(action);

                var ex = await Assert.ThrowsExceptionAsync<Grpc.Core.RpcException>(async () =>
                {
                    while (await call.ResponseStream.MoveNext(default)) { }
                });

                Assert.AreEqual(Grpc.Core.StatusCode.InvalidArgument, ex.StatusCode,
                    $"Expected InvalidArgument, got {ex.StatusCode}: {ex.Status.Detail}");
            }
            finally
            {
                channel.Dispose();
            }
        }

        // ================================================================== //
        //  Phase 2: GetSchema
        // ================================================================== //

        [TestMethod]
        public async Task V3_GetSchema_ReturnsCorrectSchema()
        {
            TableSchema schema = await _client!.GetSchemaAsync(_testTablePath!);

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
            await foreach (RecordBatch batch in _client!.ReadTableAsync(_testTablePath!))
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

            await foreach (RecordBatch batch in _client!.ReadTableAsync(_testTablePath!))
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
            await foreach (RecordBatch batch in _client!.ExecuteQueryAsync(
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

            await foreach (RecordBatch batch in _client!.ExecuteQueryAsync(
                sql: "SELECT id FROM tbl WHERE id > 1",
                tablePath: _testTablePath!,
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
            await foreach (RecordBatch batch in _client!.ExecuteQueryAsync(
                sql: "SELECT * FROM tbl LIMIT 2",
                tablePath: _testTablePath!,
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
            var ex = await Assert.ThrowsExceptionAsync<Grpc.Core.RpcException>(async () =>
            {
                await foreach (RecordBatch _ in _client!.ReadTableAsync(
                    "/nonexistent/path/to/table"))
                {
                    // Should not reach here.
                }
            });

            // The server returns InvalidArgument when the path cannot be
            // converted to a file URL, Internal/NotFound for other failures.
            Assert.IsTrue(
                ex.StatusCode == Grpc.Core.StatusCode.Internal ||
                ex.StatusCode == Grpc.Core.StatusCode.NotFound ||
                ex.StatusCode == Grpc.Core.StatusCode.InvalidArgument,
                $"Expected Internal, NotFound, or InvalidArgument, got {ex.StatusCode}: {ex.Status.Detail}");
        }

        [TestMethod]
        public async Task V3_GetSchema_InvalidPath_ReturnsError()
        {
            var ex = await Assert.ThrowsExceptionAsync<Grpc.Core.RpcException>(async () =>
            {
                await _client!.GetSchemaAsync("/nonexistent/path/to/table");
            });

            Assert.IsTrue(
                ex.StatusCode == Grpc.Core.StatusCode.Internal ||
                ex.StatusCode == Grpc.Core.StatusCode.NotFound ||
                ex.StatusCode == Grpc.Core.StatusCode.InvalidArgument,
                $"Expected Internal, NotFound, or InvalidArgument, got {ex.StatusCode}: {ex.Status.Detail}");
        }

        [TestMethod]
        public async Task V3_ExecuteQuery_InvalidSql_ReturnsError()
        {
            var ex = await Assert.ThrowsExceptionAsync<Grpc.Core.RpcException>(async () =>
            {
                await foreach (RecordBatch _ in _client!.ExecuteQueryAsync(
                    "SELECT * FROM nonexistent_table_xyz"))
                {
                    // Should not reach here.
                }
            });

            // The server should return an error for the unknown table.
            Assert.IsTrue(
                ex.StatusCode == Grpc.Core.StatusCode.Internal ||
                ex.StatusCode == Grpc.Core.StatusCode.InvalidArgument,
                $"Expected Internal or InvalidArgument, got {ex.StatusCode}: {ex.Status.Detail}");
        }

        // ================================================================== //
        //  Phase 2: Partitioned tables
        // ================================================================== //

        [TestMethod]
        public async Task V3_ReadTable_Partitioned_ReturnsAllRows()
        {
            var batches = new List<RecordBatch>();
            await foreach (RecordBatch batch in _client!.ReadTableAsync(_partitionedTablePath!))
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
            await foreach (RecordBatch batch in _client!.ReadTableAsync(_partitionedTablePath!))
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
            TableSchema schema = await _client!.GetSchemaAsync(_partitionedTablePath!);

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

            await foreach (RecordBatch batch in _client!.ExecuteQueryAsync(
                sql: "SELECT id FROM tbl WHERE region = 'us' ORDER BY id",
                tablePath: _partitionedTablePath!,
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
            await foreach (RecordBatch batch in _client!.ReadTableAsync(
                _timeTravelTablePath!, version: 0))
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
            await foreach (RecordBatch batch in _client!.ReadTableAsync(_timeTravelTablePath!))
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

            await foreach (RecordBatch batch in _client!.ReadTableAsync(
                _timeTravelTablePath!, version: 0))
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
            TableSchema schema = await _client!.GetSchemaAsync(
                _timeTravelTablePath!, version: 0);

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
            await foreach (RecordBatch batch in _client!.ReadTableAsync(
                _testTablePath!, numRows: 1))
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
            await foreach (RecordBatch batch in _client!.ReadTableAsync(
                _testTablePath!, numRows: 0))
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
            await foreach (RecordBatch batch in _client!.ReadTableAsync(
                _testTablePath!, numRows: 100))
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
            await foreach (RecordBatch batch in _client!.ReadTableAsync(
                _partitionedTablePath!, numRows: 2))
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
            if (_fixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(_fixtureDataDir, "delta_test_column_mapping_name");
            TableSchema schema = await _client!.GetSchemaAsync(tablePath);

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
            if (_fixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(_fixtureDataDir, "delta_test_column_mapping_name");
            var batches = new List<RecordBatch>();
            await foreach (RecordBatch batch in _client!.ReadTableAsync(tablePath))
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
            if (_fixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(_fixtureDataDir, "delta_test_column_mapping_name");
            var ids = new List<int>();
            var cities = new List<string?>();

            await foreach (RecordBatch batch in _client!.ReadTableAsync(tablePath))
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
            if (_fixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(_fixtureDataDir, "delta_test_column_mapping_name");
            var ids = new List<int>();

            await foreach (RecordBatch batch in _client!.ExecuteQueryAsync(
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
            if (_fixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(_fixtureDataDir, "delta_test_deletion_vector");
            TableSchema schema = await _client!.GetSchemaAsync(tablePath);

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
            if (_fixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(_fixtureDataDir, "delta_test_deletion_vector");
            var batches = new List<RecordBatch>();
            await foreach (RecordBatch batch in _client!.ReadTableAsync(tablePath))
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
            if (_fixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(_fixtureDataDir, "delta_test_deletion_vector");
            var ids = new List<int>();
            var values = new List<string?>();

            await foreach (RecordBatch batch in _client!.ReadTableAsync(tablePath))
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
            if (_fixtureDataDir == null)
            {
                Assert.Inconclusive("Fixture data directory not found.");
                return;
            }

            string tablePath = Path.Combine(_fixtureDataDir, "delta_test_deletion_vector");
            var ids = new List<int>();

            await foreach (RecordBatch batch in _client!.ExecuteQueryAsync(
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
    }
}
