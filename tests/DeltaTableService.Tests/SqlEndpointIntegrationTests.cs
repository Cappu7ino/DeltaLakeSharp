// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Data;
using System.Threading.Tasks;
using Microsoft.DI.DeltaTableService.Client;
using Microsoft.DI.DeltaTableService.Client.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.DI.DeltaTableService.Tests
{
    /// <summary>
    /// Integration tests for querying Delta tables in a Fabric Lakehouse
    /// through the SQL analytics endpoint (read-only T-SQL over TDS).
    /// </summary>
    /// <remarks>
    /// <para>
    /// These tests connect directly to the Fabric SQL analytics endpoint —
    /// no Docker container is needed. Authentication uses
    /// <see cref="Azure.Identity.DefaultAzureCredential"/>, so the caller must
    /// be signed in via <c>az login</c> or have a managed identity available.
    /// </para>
    /// <para>
    /// The SQL analytics endpoint is read-only; these tests only exercise
    /// SELECT queries, schema inspection, and connectivity checks.
    /// </para>
    /// <para>
    /// Run with: <c>dotnet test &lt;dll&gt; --filter "TestCategory=SqlEndpoint" --Platform x64</c>
    /// </para>
    /// </remarks>
    [TestClass]
    [TestCategory("SqlEndpoint")]
    public class SqlEndpointIntegrationTests
    {
        // ================================================================== //
        //  Configuration
        // ================================================================== //
        //
        //  Update these constants with your Fabric Lakehouse SQL analytics
        //  endpoint details. The server hostname can be found in the Fabric
        //  portal: Lakehouse → Settings → SQL endpoint → SQL connection string.
        //

        /// <summary>
        /// The SQL analytics endpoint hostname.
        /// Format: &lt;guid&gt;.datawarehouse.fabric.microsoft.com
        /// </summary>
        private const string SqlEndpointServer = "x6eps4xrq2xudenlfv6naeo3i4-i6pcvcgfrdeefgqpvwriskyf5m.msit-datawarehouse.fabric.microsoft.com";

        /// <summary>
        /// The database name (= Lakehouse name, not the GUID).
        /// </summary>
        private const string SqlEndpointDatabase = "XingLakehouse";

        /// <summary>
        /// The name of the pre-existing Delta table used for read tests.
        /// This should match the table used in the OneLake V1 read tests.
        /// </summary>
        private const string ReadTestTableName = "SimpleTable";

        // ================================================================== //
        //  Shared state
        // ================================================================== //

        private static SqlEndpointClient _client;
        private static bool _initialized;
        private static string _initFailureReason;

        // ================================================================== //
        //  Lifecycle
        // ================================================================== //

        [ClassInitialize]
        public static void ClassInitialize(TestContext context)
        {
            _initialized = false;
            _initFailureReason = null;

            try
            {
                if (SqlEndpointServer.StartsWith("TODO", StringComparison.Ordinal) ||
                    SqlEndpointDatabase.StartsWith("TODO", StringComparison.Ordinal))
                {
                    _initFailureReason =
                        "SQL analytics endpoint not configured. " +
                        "Update SqlEndpointServer and SqlEndpointDatabase constants " +
                        "in SqlEndpointIntegrationTests.cs.";
                    return;
                }

                var config = new SqlEndpointConfig(SqlEndpointServer, SqlEndpointDatabase);
                _client = new SqlEndpointClient(config);
                _initialized = true;
            }
            catch (Exception ex)
            {
                _initFailureReason = $"ClassInitialize failed: {ex.Message}";
            }
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            _client?.Dispose();
            _client = null;
        }

        /// <summary>
        /// Guard: reports Inconclusive if initialization did not complete.
        /// </summary>
        private static void EnsureInitialized()
        {
            if (!_initialized)
            {
                Assert.Inconclusive(
                    _initFailureReason ??
                    "ClassInitialize did not complete. Check SQL endpoint configuration / auth.");
            }
        }

        // ================================================================== //
        //  Health check
        // ================================================================== //

        [TestMethod]
        public async Task SqlEndpoint_HealthCheck_ReturnsTrue()
        {
            EnsureInitialized();

            bool healthy = await _client.HealthCheckAsync();
            Assert.IsTrue(healthy, "Expected the SQL analytics endpoint to be reachable.");
        }

        // ================================================================== //
        //  Schema
        // ================================================================== //

        [TestMethod]
        public async Task SqlEndpoint_GetSchema_ReturnsColumns()
        {
            EnsureInitialized();

            DataTable schema = await _client.GetSchemaAsync(ReadTestTableName);

            Assert.IsNotNull(schema, "Schema DataTable should not be null.");
            Assert.IsTrue(schema.Columns.Count > 0,
                "Expected at least one column in the schema.");
            Assert.AreEqual(0, schema.Rows.Count,
                "GetSchemaAsync should return zero rows.");

            Console.WriteLine($"Table '{ReadTestTableName}' has {schema.Columns.Count} columns:");
            foreach (DataColumn col in schema.Columns)
            {
                Console.WriteLine($"  {col.ColumnName} ({col.DataType.Name})");
            }
        }

        // ================================================================== //
        //  SELECT * (all rows)
        // ================================================================== //

        [TestMethod]
        public async Task SqlEndpoint_ExecuteQuery_SelectAll_ReturnsRows()
        {
            EnsureInitialized();

            string sql = $"SELECT * FROM [{ReadTestTableName}]";
            DataTable result = await _client.ExecuteQueryAsync(sql);

            Assert.IsNotNull(result, "Result DataTable should not be null.");
            Assert.IsTrue(result.Rows.Count > 0,
                "Expected at least one row from SELECT *.");
            Assert.IsTrue(result.Columns.Count > 0,
                "Expected at least one column in the result.");

            Console.WriteLine(
                $"SELECT * returned {result.Rows.Count} rows, " +
                $"{result.Columns.Count} columns.");
        }

        // ================================================================== //
        //  SELECT with TOP
        // ================================================================== //

        [TestMethod]
        public async Task SqlEndpoint_ExecuteQuery_SelectTop_ReturnsLimitedRows()
        {
            EnsureInitialized();

            const int topN = 5;
            string sql = $"SELECT TOP {topN} * FROM [{ReadTestTableName}]";
            DataTable result = await _client.ExecuteQueryAsync(sql);

            Assert.IsNotNull(result, "Result DataTable should not be null.");
            Assert.IsTrue(result.Rows.Count <= topN,
                $"Expected at most {topN} rows but got {result.Rows.Count}.");
            Assert.IsTrue(result.Rows.Count > 0,
                "Expected at least one row.");

            Console.WriteLine($"SELECT TOP {topN} returned {result.Rows.Count} rows.");
        }

        // ================================================================== //
        //  COUNT(*)
        // ================================================================== //

        [TestMethod]
        public async Task SqlEndpoint_ExecuteScalar_Count_ReturnsNumber()
        {
            EnsureInitialized();

            string sql = $"SELECT COUNT(*) FROM [{ReadTestTableName}]";
            object result = await _client.ExecuteScalarAsync(sql);

            Assert.IsNotNull(result, "Scalar result should not be null.");

            long count = Convert.ToInt64(result);
            Assert.IsTrue(count > 0,
                $"Expected a positive row count but got {count}.");

            Console.WriteLine($"COUNT(*) = {count}");
        }

        // ================================================================== //
        //  SELECT with WHERE filter
        // ================================================================== //

        [TestMethod]
        public async Task SqlEndpoint_ExecuteQuery_SelectWithFilter_ReturnsFilteredRows()
        {
            EnsureInitialized();

            // First, get the total count to establish a baseline.
            string countSql = $"SELECT COUNT(*) FROM [{ReadTestTableName}]";
            long totalCount = Convert.ToInt64(
                await _client.ExecuteScalarAsync(countSql));

            // Query with TOP 1 to get a sample value for filtering.
            string sampleSql = $"SELECT TOP 1 * FROM [{ReadTestTableName}]";
            DataTable sample = await _client.ExecuteQueryAsync(sampleSql);
            Assert.IsTrue(sample.Rows.Count > 0, "Need at least one row for filter test.");

            // Use the first column for a WHERE clause.
            string firstColName = sample.Columns[0].ColumnName;
            object firstColValue = sample.Rows[0][0];

            string filterSql;
            if (firstColValue is string strValue)
            {
                filterSql = $"SELECT * FROM [{ReadTestTableName}] WHERE [{firstColName}] = '{strValue.Replace("'", "''")}'";
            }
            else
            {
                filterSql = $"SELECT * FROM [{ReadTestTableName}] WHERE [{firstColName}] = {firstColValue}";
            }

            DataTable filtered = await _client.ExecuteQueryAsync(filterSql);

            Assert.IsNotNull(filtered, "Filtered result should not be null.");
            Assert.IsTrue(filtered.Rows.Count > 0,
                "Expected at least one row matching the filter.");
            Assert.IsTrue(filtered.Rows.Count <= totalCount,
                "Filtered count should not exceed total count.");

            Console.WriteLine(
                $"Filter on [{firstColName}] returned {filtered.Rows.Count} of {totalCount} total rows.");
        }

        // ================================================================== //
        //  INFORMATION_SCHEMA (T-SQL metadata)
        // ================================================================== //

        [TestMethod]
        public async Task SqlEndpoint_ExecuteQuery_InformationSchema_ReturnsMetadata()
        {
            EnsureInitialized();

            string sql =
                "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE " +
                "FROM INFORMATION_SCHEMA.COLUMNS " +
                $"WHERE TABLE_NAME = '{ReadTestTableName}' " +
                "ORDER BY ORDINAL_POSITION";
            DataTable result = await _client.ExecuteQueryAsync(sql);

            Assert.IsNotNull(result, "INFORMATION_SCHEMA result should not be null.");
            Assert.IsTrue(result.Rows.Count > 0,
                $"Expected column metadata for table '{ReadTestTableName}'.");

            Console.WriteLine($"INFORMATION_SCHEMA returned {result.Rows.Count} columns:");
            foreach (DataRow row in result.Rows)
            {
                Console.WriteLine($"  {row["COLUMN_NAME"]} ({row["DATA_TYPE"]})");
            }
        }
    }
}
