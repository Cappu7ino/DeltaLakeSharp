using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;
using Azure;
using Azure.Core;
using Azure.Identity;
using DeltaLakeSharp.Adbc.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DeltaLakeSharp.Adbc.Tests
{
    [TestClass]
    [TestCategory("Integration")]
    [TestCategory("OneLake")]
    [TestCategory("Fabric")]
    [TestCategory("ADBC")]
    public class DeltaAdbcOneLakeIntegrationTests
    {
        private const string StorageTokenScope = "https://storage.azure.com/.default";
        private const string TenantIdEnvironmentVariable = "DELTALAKESHARP_ONELAKE_TENANT_ID";
        private const string ClientIdEnvironmentVariable = "DELTALAKESHARP_ONELAKE_CLIENT_ID";
        private const string CertificateThumbprintEnvironmentVariable = "DELTALAKESHARP_ONELAKE_CERTIFICATE_THUMBPRINT";
        private const string CertificateNameEnvironmentVariable = "DELTALAKESHARP_ONELAKE_CERTIFICATE_NAME";
        private const string CertificateSubjectEnvironmentVariable = "DELTALAKESHARP_ONELAKE_CERTIFICATE_SUBJECT";
        private const string PrimitiveTypesTableUriEnvironmentVariable = "DELTALAKESHARP_ONELAKE_PRIMITIVE_TYPES_TABLE_URI";
        private const string ColumnMappingTableUriEnvironmentVariable = "DELTALAKESHARP_ONELAKE_COLUMN_MAPPING_TABLE_URI";
        private const string ChangeDataTableUriEnvironmentVariable = "DELTALAKESHARP_ONELAKE_CDF_TABLE_URI";
        private const string BenchmarkTableUriEnvironmentVariable = "DELTALAKESHARP_ONELAKE_BENCHMARK_TABLE_URI";
        private const string BenchmarkFilteredSql = "SELECT id, tenant_id, amount, region FROM delta_table WHERE tenant_id = 42 AND is_active = true";
        private const int BenchmarkFilteredExpectedRowCount = 1000;

        private static string PrimitiveTypesTableUri => GetRequiredEnvironmentVariable(PrimitiveTypesTableUriEnvironmentVariable);

        private static string ColumnMappingTableUri => GetRequiredEnvironmentVariable(ColumnMappingTableUriEnvironmentVariable);

        private static string ChangeDataTableUri => GetRequiredEnvironmentVariable(ChangeDataTableUriEnvironmentVariable);

        private static string BenchmarkTableUri => GetRequiredEnvironmentVariable(BenchmarkTableUriEnvironmentVariable);

        private static readonly string[] ExpectedChangeDataColumnNames =
        {
            "id",
            "name",
            "_change_type",
            "_commit_version",
            "_commit_timestamp",
        };

        private static readonly string[] ExpectedChangeDataProjectedColumnNames =
        {
            "id",
            "name",
            "_change_type",
        };

        private static readonly string[] ExpectedColumnNames =
        {
            "c_byte",
            "c_short",
            "c_int",
            "c_long",
            "c_float",
            "c_double",
            "c_decimal",
            "c_string",
            "c_boolean",
            "c_binary",
            "c_date",
            "c_timestamp_ntz",
            "c_timestamp",
        };

        private static readonly string[] ExpectedColumnMappingColumnNames =
        {
            "id",
            "{name}",
            "price",
            "created_at",
        };

        private static readonly string[] ExpectedPrimitiveProjectedColumnNames =
        {
            "c_int",
            "c_string",
            "c_binary",
            "c_timestamp",
        };

        private static readonly string[] ExpectedColumnMappingProjectedColumnNames =
        {
            "id",
            "{name}",
        };

        private static readonly DateTime ExpectedDate = new DateTime(2024, 3, 22, 0, 0, 0, DateTimeKind.Unspecified);
        private static readonly DateTime ExpectedTimestampNtz = new DateTime(2024, 3, 22, 12, 0, 0, DateTimeKind.Unspecified);
        private static readonly DateTimeOffset ExpectedTimestampLtz =
            new DateTimeOffset(2026, 3, 22, 6, 36, 2, TimeSpan.Zero).AddTicks(3095600);
        private static readonly byte[] ExpectedBinaryPayload = { 0x61, 0x62, 0x63 };
        private static readonly DateTimeOffset ExpectedColumnMappingTimestamp = new DateTimeOffset(2024, 3, 22, 12, 0, 0, TimeSpan.Zero);

        [TestMethod]
        public void OneLake_ClientCertificateCredential_GetTableSchema_ReturnsColumns()
        {
            using OpenedOneLakeConnection opened = OpenConnection(PrimitiveTypesTableUri);
            AdbcConnection connection = opened.Connection;

            Schema schema = connection.GetTableSchema(null, null, DeltaAdbcConnectOptions.LogicalTableName);

            AssertExpectedSchema(schema);
        }

        [TestMethod]
        public void OneLake_ClientCertificateCredential_ExecuteQuery_ReturnsRows()
        {
            using OpenedOneLakeConnection opened = OpenConnection(PrimitiveTypesTableUri);
            AdbcConnection connection = opened.Connection;

            var statement = connection.CreateStatement();
            try
            {
                QueryResult result = statement.ExecuteQuery();
                var stream = result.Stream ?? throw new AssertFailedException("Read stream should not be null.");
                try
                {
                    IReadOnlyList<RecordBatch> batches = ReadAllBatches(stream);
                    AssertExpectedSingleRowResult(batches);
                }
                finally
                {
                    stream.Dispose();
                }
            }
            finally
            {
                statement.Dispose();
            }
        }

        [TestMethod]
        public void OneLake_ClientCertificateCredential_SqlQuery_ProjectsSelectedColumns()
        {
            using OpenedOneLakeConnection opened = OpenConnection(PrimitiveTypesTableUri);
            AdbcConnection connection = opened.Connection;

            var statement = connection.CreateStatement();
            try
            {
                statement.SqlQuery = "SELECT c_int, c_string, c_binary, c_timestamp FROM delta_table LIMIT 10";
                QueryResult result = statement.ExecuteQuery();
                var stream = result.Stream ?? throw new AssertFailedException("SQL stream should not be null.");
                try
                {
                    IReadOnlyList<RecordBatch> batches = ReadAllBatches(stream);
                    AssertExpectedPrimitiveProjectedRow(batches);
                }
                finally
                {
                    stream.Dispose();
                }
            }
            finally
            {
                statement.Dispose();
            }
        }

        [TestMethod]
        public void OneLake_ClientCertificateCredential_GetInfo_ReturnsDriverMetadata()
        {
            using OpenedOneLakeConnection opened = OpenConnection(PrimitiveTypesTableUri, validateTableAccess: false);
            AdbcConnection connection = opened.Connection;

            using var stream = connection.GetInfo(System.Array.Empty<AdbcInfoCode>());
            RecordBatch? batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();

            Assert.IsNotNull(batch);
            Assert.AreEqual("Delta Lake", ReadInfoValue(batch, AdbcInfoCode.VendorName));
            Assert.AreEqual("DeltaLakeSharp.Adbc", ReadInfoValue(batch, AdbcInfoCode.DriverName));
        }

        [TestMethod]
        public void OneLake_ClientCertificateCredential_GetObjects_ReturnsLogicalTableMetadata()
        {
            using OpenedOneLakeConnection opened = OpenConnection(PrimitiveTypesTableUri);
            AdbcConnection connection = opened.Connection;

            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.All,
                null,
                null,
                null,
                null,
                null);

            RecordBatch? batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();
            Assert.IsNotNull(batch);
            Assert.AreEqual(1, batch.Length);

            var dbSchemas = (ListArray)batch.Column(1);
            AssertListLength(dbSchemas, 0, 1);

            var dbSchemaValues = (StructArray)dbSchemas.Values;
            Assert.AreEqual(string.Empty, ((StringArray)dbSchemaValues.Fields[0]).GetString(0));

            var tables = (ListArray)dbSchemaValues.Fields[1];
            AssertListLength(tables, 0, 1);

            var tableValues = (StructArray)tables.Values;
            Assert.AreEqual(DeltaAdbcConnectOptions.LogicalTableName, ((StringArray)tableValues.Fields[0]).GetString(0));
            Assert.AreEqual("TABLE", ((StringArray)tableValues.Fields[1]).GetString(0));

            var columns = (ListArray)tableValues.Fields[2];
            AssertListLength(columns, 0, ExpectedColumnNames.Length);

            var columnValues = (StructArray)columns.Values;
            var columnNames = (StringArray)columnValues.Fields[0];
            var ordinalPositions = (Int32Array)columnValues.Fields[1];
            var xdbcTypeNames = (StringArray)columnValues.Fields[4];
            var nullableFlags = (StringArray)columnValues.Fields[13];

            Schema schema = connection.GetTableSchema(null, null, DeltaAdbcConnectOptions.LogicalTableName);
            AssertExpectedSchema(schema);

            for (int i = 0; i < ExpectedColumnNames.Length; i++)
            {
                Assert.AreEqual(ExpectedColumnNames[i], columnNames.GetString(i), $"Unexpected GetObjects column name at ordinal {i + 1}.");
                Assert.AreEqual(i + 1, ordinalPositions.GetValue(i), $"Unexpected GetObjects ordinal for '{ExpectedColumnNames[i]}'.");
                Assert.AreEqual(schema.FieldsList[i].DataType.Name, xdbcTypeNames.GetString(i), $"Unexpected XDBC type name for '{ExpectedColumnNames[i]}'.");
                Assert.AreEqual("YES", nullableFlags.GetString(i), $"Expected '{ExpectedColumnNames[i]}' to be nullable.");
            }

            var constraints = (ListArray)tableValues.Fields[3];
            AssertListLength(constraints, 0, 0);
        }

        [TestMethod]
        public void OneLake_ClientCertificateCredential_CanAcquireBearerToken()
        {
            string bearerToken = AcquireBearerToken();

            Assert.IsFalse(string.IsNullOrWhiteSpace(bearerToken), "Bearer token should not be empty.");
        }

        [TestMethod]
        public void OneLake_ClientCertificateCredential_ColumnMappingName_GetTableSchema_ReturnsLogicalColumns()
        {
            using OpenedOneLakeConnection opened = OpenConnection(ColumnMappingTableUri);
            AdbcConnection connection = opened.Connection;

            Schema schema = connection.GetTableSchema(null, null, DeltaAdbcConnectOptions.LogicalTableName);

            AssertExpectedColumnMappingSchema(schema);
        }

        [TestMethod]
        public void OneLake_ClientCertificateCredential_ColumnMappingName_ExecuteQuery_ReturnsRenamedColumn()
        {
            using OpenedOneLakeConnection opened = OpenConnection(ColumnMappingTableUri);
            AdbcConnection connection = opened.Connection;

            var statement = connection.CreateStatement();
            try
            {
                QueryResult result = statement.ExecuteQuery();
                var stream = result.Stream ?? throw new AssertFailedException("Read stream should not be null.");
                try
                {
                    IReadOnlyList<RecordBatch> batches = ReadAllBatches(stream);
                    AssertExpectedColumnMappingRow(batches);
                }
                finally
                {
                    stream.Dispose();
                }
            }
            finally
            {
                statement.Dispose();
            }
        }

        [TestMethod]
        public void OneLake_ClientCertificateCredential_ColumnMappingName_SqlQuery_ProjectsSelectedColumns()
        {
            using OpenedOneLakeConnection opened = OpenConnection(ColumnMappingTableUri);
            AdbcConnection connection = opened.Connection;

            var statement = connection.CreateStatement();
            try
            {
                statement.SqlQuery = "SELECT id, `{name}` FROM delta_table";
                QueryResult result = statement.ExecuteQuery();
                var stream = result.Stream ?? throw new AssertFailedException("SQL stream should not be null.");
                try
                {
                    IReadOnlyList<RecordBatch> batches = ReadAllBatches(stream);
                    AssertExpectedColumnMappingProjectedRow(batches);
                }
                finally
                {
                    stream.Dispose();
                }
            }
            finally
            {
                statement.Dispose();
            }
        }

        [TestMethod]
        public void OneLake_ClientCertificateCredential_ColumnMappingName_GetObjects_ReturnsLogicalColumns()
        {
            using OpenedOneLakeConnection opened = OpenConnection(ColumnMappingTableUri);
            AdbcConnection connection = opened.Connection;

            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.All,
                null,
                null,
                null,
                null,
                null);

            RecordBatch? batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();
            Assert.IsNotNull(batch);
            Assert.AreEqual(1, batch.Length);

            var dbSchemas = (ListArray)batch.Column(1);
            AssertListLength(dbSchemas, 0, 1);

            var dbSchemaValues = (StructArray)dbSchemas.Values;
            Assert.AreEqual(string.Empty, ((StringArray)dbSchemaValues.Fields[0]).GetString(0));

            var tables = (ListArray)dbSchemaValues.Fields[1];
            AssertListLength(tables, 0, 1);

            var tableValues = (StructArray)tables.Values;
            Assert.AreEqual(DeltaAdbcConnectOptions.LogicalTableName, ((StringArray)tableValues.Fields[0]).GetString(0));
            Assert.AreEqual("TABLE", ((StringArray)tableValues.Fields[1]).GetString(0));

            var columns = (ListArray)tableValues.Fields[2];
            AssertListLength(columns, 0, ExpectedColumnMappingColumnNames.Length);

            var columnValues = (StructArray)columns.Values;
            var columnNames = (StringArray)columnValues.Fields[0];
            var ordinalPositions = (Int32Array)columnValues.Fields[1];
            var xdbcTypeNames = (StringArray)columnValues.Fields[4];
            var nullableFlags = (StringArray)columnValues.Fields[13];

            Schema schema = connection.GetTableSchema(null, null, DeltaAdbcConnectOptions.LogicalTableName);
            AssertExpectedColumnMappingSchema(schema);

            for (int i = 0; i < ExpectedColumnMappingColumnNames.Length; i++)
            {
                Assert.AreEqual(ExpectedColumnMappingColumnNames[i], columnNames.GetString(i), $"Unexpected GetObjects column name at ordinal {i + 1}.");
                Assert.AreEqual(i + 1, ordinalPositions.GetValue(i), $"Unexpected GetObjects ordinal for '{ExpectedColumnMappingColumnNames[i]}'.");
                Assert.AreEqual(schema.FieldsList[i].DataType.Name, xdbcTypeNames.GetString(i), $"Unexpected XDBC type name for '{ExpectedColumnMappingColumnNames[i]}'.");
                Assert.AreEqual("YES", nullableFlags.GetString(i), $"Expected '{ExpectedColumnMappingColumnNames[i]}' to be nullable.");
            }

            var constraints = (ListArray)tableValues.Fields[3];
            AssertListLength(constraints, 0, 0);
        }

        [TestMethod]
        public void OneLake_ClientCertificateCredential_ChangeData_DirectReadViaStatementOptions_ReturnsExpectedRows()
        {
            using OpenedOneLakeConnection opened = OpenConnection(ChangeDataTableUri);
            AdbcConnection connection = opened.Connection;

            var statement = connection.CreateStatement();
            try
            {
                statement.SetOption(DeltaAdbcStatementOptions.CdfStartingVersionOptionKey, "1");
                QueryResult result = statement.ExecuteQuery();
                var stream = result.Stream ?? throw new AssertFailedException("CDF stream should not be null.");
                try
                {
                    IReadOnlyList<RecordBatch> batches = ReadAllBatches(stream);
                    AssertExpectedChangeDataRows(batches);
                }
                finally
                {
                    stream.Dispose();
                }
            }
            finally
            {
                statement.Dispose();
            }
        }

        [TestMethod]
        public void OneLake_ClientCertificateCredential_ChangeData_ProjectedQueryViaStatementOptions_ReturnsExpectedRows()
        {
            using OpenedOneLakeConnection opened = OpenConnection(ChangeDataTableUri);
            AdbcConnection connection = opened.Connection;

            var statement = connection.CreateStatement();
            try
            {
                statement.SetOption(DeltaAdbcStatementOptions.CdfStartingVersionOptionKey, "1");
                statement.SqlQuery = "SELECT id, name, _change_type FROM _cdf WHERE _change_type <> 'update_preimage' ORDER BY _change_type, id";
                QueryResult result = statement.ExecuteQuery();
                var stream = result.Stream ?? throw new AssertFailedException("Projected CDF stream should not be null.");
                try
                {
                    IReadOnlyList<RecordBatch> batches = ReadAllBatches(stream);
                    AssertExpectedProjectedChangeDataRows(batches);
                }
                finally
                {
                    stream.Dispose();
                }
            }
            finally
            {
                statement.Dispose();
            }
        }

        [TestMethod]
        [TestCategory("Profile")]
        public async Task OneLake_ClientCertificateCredential_BenchmarkTable_FilteredRead_ForProfiling()
        {
            using OpenedOneLakeConnection opened = OpenConnection(BenchmarkTableUri);
            AdbcConnection connection = opened.Connection;

            var statement = connection.CreateStatement();
            try
            {
                statement.SqlQuery = BenchmarkFilteredSql;
                QueryResult result = statement.ExecuteQuery();
                var stream = result.Stream ?? throw new AssertFailedException("Benchmark filtered stream should not be null.");
                try
                {
                    IReadOnlyList<RecordBatch> batches = await ReadAllBatchesAsync(stream).ConfigureAwait(false);
                    Assert.IsTrue(batches.Count > 0, "Expected at least one Arrow record batch for the benchmark profiling query.");

                    int rowCount = batches.Sum(batch => checked((int)batch.Length));
                    Assert.AreEqual(BenchmarkFilteredExpectedRowCount, rowCount, "Unexpected benchmark profiling row count.");

                    RecordBatch batch = batches.FirstOrDefault(candidate => candidate.Length > 0)
                        ?? throw new AssertFailedException("Expected at least one non-empty Arrow record batch for the benchmark profiling query.");

                    Assert.AreEqual(4, batch.ColumnCount, "Unexpected benchmark profiling column count.");
                    CollectionAssert.AreEqual(
                        new[] { "id", "tenant_id", "amount", "region" },
                        batch.Schema.FieldsList.Select(field => field.Name).ToArray());
                }
                finally
                {
                    stream.Dispose();
                }
            }
            finally
            {
                statement.Dispose();
            }
        }

        private static OpenedOneLakeConnection OpenConnection(string tableUri, bool validateTableAccess = true)
        {
            try
            {
                string bearerToken = AcquireBearerToken();

                var driver = new DeltaAdbcDriver();
                var database = driver.Open(new Dictionary<string, string>
                {
                    [DeltaAdbcConnectOptions.TableUriKey] = tableUri,
                    [$"{DeltaAdbcConnectOptions.StorageOptionPrefix}account_name"] = "onelake",
                    [$"{DeltaAdbcConnectOptions.StorageOptionPrefix}{DeltaAdbcConnectOptions.BearerTokenStorageOptionKey}"] = bearerToken,
                    [$"{DeltaAdbcConnectOptions.StorageOptionPrefix}use_fabric_endpoint"] = "true",
                });
                var connection = database.Connect(null);

                if (validateTableAccess)
                {
                    connection.GetTableSchema(null, null, DeltaAdbcConnectOptions.LogicalTableName);
                }

                return new OpenedOneLakeConnection(driver, database, connection);
            }
            catch (AssertInconclusiveException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Assert.Inconclusive($"Production OneLake ADBC integration test could not open the Fabric table with bearer-token auth: {ex.Message}");
                throw;
            }
        }

        private static void AssertExpectedColumnMappingSchema(Schema schema)
        {
            Assert.AreEqual(ExpectedColumnMappingColumnNames.Length, schema.FieldsList.Count, "Unexpected column-mapping schema column count.");
            CollectionAssert.AreEqual(ExpectedColumnMappingColumnNames, schema.FieldsList.Select(field => field.Name).ToArray());

            Assert.IsInstanceOfType(schema.FieldsList[0].DataType, typeof(Int32Type));
            Assert.IsTrue(IsStringLikeType(schema.FieldsList[1].DataType), "Expected '{name}' to use an Arrow string-compatible type.");
            Assert.IsInstanceOfType(schema.FieldsList[2].DataType, typeof(DoubleType));

            var createdAtType = schema.FieldsList[3].DataType as TimestampType;
            Assert.IsNotNull(createdAtType, "Expected created_at to be TimestampType.");

            foreach (Field field in schema.FieldsList)
            {
                Assert.IsTrue(field.IsNullable, $"Expected '{field.Name}' to be nullable.");
            }
        }

        private static void AssertExpectedColumnMappingRow(IReadOnlyList<RecordBatch> batches)
        {
            Assert.IsTrue(batches.Count > 0, "Expected at least one Arrow record batch.");

            int rowCount = batches.Sum(batch => checked((int)batch.Length));
            Assert.AreEqual(1, rowCount, "Expected exactly one inserted column-mapping row.");

            RecordBatch batch = batches.FirstOrDefault(candidate => candidate.Length > 0)
                ?? throw new AssertFailedException("Expected at least one non-empty Arrow record batch.");

            Assert.AreEqual(ExpectedColumnMappingColumnNames.Length, batch.ColumnCount, "Unexpected column-mapping result column count.");
            AssertExpectedColumnMappingSchema(batch.Schema);

            int? idValue = ((Int32Array)batch.Column(0)).GetValue(0);
            Assert.IsTrue(idValue.HasValue, "Expected id to have a value.");
            Assert.AreEqual(1, idValue.GetValueOrDefault());

            Assert.AreEqual("Sample Product", GetStringValue(batch.Column(1), 0));

            double? priceValue = ((DoubleArray)batch.Column(2)).GetValue(0);
            Assert.IsTrue(priceValue.HasValue, "Expected price to have a value.");
            Assert.IsTrue(Math.Abs(priceValue.GetValueOrDefault() - 12.5d) < 0.0000001d, $"Unexpected price value: {priceValue.GetValueOrDefault()}");

            var createdAtArray = (TimestampArray)batch.Column(3);
            DateTimeOffset? createdAtValue = createdAtArray.GetTimestamp(0);
            Assert.IsTrue(createdAtValue.HasValue, "Expected created_at to have a value.");
            Assert.AreEqual(ExpectedColumnMappingTimestamp, createdAtValue.GetValueOrDefault().ToUniversalTime());
        }

        private static void AssertExpectedPrimitiveProjectedRow(IReadOnlyList<RecordBatch> batches)
        {
            Assert.IsTrue(batches.Count > 0, "Expected at least one Arrow record batch.");

            int rowCount = batches.Sum(batch => checked((int)batch.Length));
            Assert.AreEqual(1, rowCount, "Expected exactly one projected primitive row.");

            RecordBatch batch = batches.FirstOrDefault(candidate => candidate.Length > 0)
                ?? throw new AssertFailedException("Expected at least one non-empty Arrow record batch.");

            Assert.AreEqual(ExpectedPrimitiveProjectedColumnNames.Length, batch.ColumnCount, "Unexpected primitive projection column count.");
            CollectionAssert.AreEqual(ExpectedPrimitiveProjectedColumnNames, batch.Schema.FieldsList.Select(field => field.Name).ToArray());

            int? intValue = ((Int32Array)batch.Column(0)).GetValue(0);
            Assert.IsTrue(intValue.HasValue, "Expected c_int to have a value.");
            Assert.AreEqual(100, intValue.GetValueOrDefault());

            Assert.AreEqual("sample_string", GetStringValue(batch.Column(1), 0));
            CollectionAssert.AreEqual(ExpectedBinaryPayload, GetBinaryValue(batch.Column(2), 0));

            var timestampArray = (TimestampArray)batch.Column(3);
            DateTimeOffset? timestampValue = timestampArray.GetTimestamp(0);
            Assert.IsTrue(timestampValue.HasValue, "Expected c_timestamp to have a value.");
            Assert.AreEqual(ExpectedTimestampLtz, timestampValue.GetValueOrDefault().ToUniversalTime());
        }

        private static void AssertExpectedChangeDataRows(IReadOnlyList<RecordBatch> batches)
        {
            Assert.IsTrue(batches.Count > 0, "Expected at least one Arrow record batch.");

            int rowCount = batches.Sum(batch => checked((int)batch.Length));
            Assert.IsTrue(rowCount >= 3, $"Expected at least 3 change rows, got {rowCount}.");

            RecordBatch batch = batches.FirstOrDefault(candidate => candidate.Length > 0)
                ?? throw new AssertFailedException("Expected at least one non-empty Arrow record batch.");

            Assert.AreEqual(ExpectedChangeDataColumnNames.Length, batch.ColumnCount, "Unexpected change-data column count.");
            CollectionAssert.AreEqual(ExpectedChangeDataColumnNames, batch.Schema.FieldsList.Select(field => field.Name).ToArray());

            List<Dictionary<string, object?>> rows = FlattenRows(batches);
            Assert.IsTrue(rows.All(row => row.ContainsKey("_change_type")), "Expected _change_type in all CDF rows.");
            Assert.IsTrue(rows.All(row => row.ContainsKey("_commit_version")), "Expected _commit_version in all CDF rows.");
            Assert.IsTrue(rows.All(row => row.ContainsKey("_commit_timestamp")), "Expected _commit_timestamp in all CDF rows.");

            CollectionAssert.Contains(rows.Select(row => row["_change_type"]?.ToString()).ToList(), "insert");
            CollectionAssert.Contains(rows.Select(row => row["_change_type"]?.ToString()).ToList(), "update_postimage");

            Assert.IsTrue(rows.Any(row => Equals(row["id"], 2) && Equals(row["name"], "b2") && Equals(row["_change_type"], "update_postimage")),
                "Expected update_postimage CDF row for id=2.");
            Assert.IsTrue(rows.Any(row => Equals(row["id"], 3) && Equals(row["name"], "c") && Equals(row["_change_type"], "insert")),
                "Expected insert CDF row for id=3.");
        }

        private static void AssertExpectedProjectedChangeDataRows(IReadOnlyList<RecordBatch> batches)
        {
            Assert.IsTrue(batches.Count > 0, "Expected at least one Arrow record batch.");

            int rowCount = batches.Sum(batch => checked((int)batch.Length));
            Assert.IsTrue(rowCount >= 2, $"Expected at least 2 projected change rows, got {rowCount}.");

            RecordBatch batch = batches.FirstOrDefault(candidate => candidate.Length > 0)
                ?? throw new AssertFailedException("Expected at least one non-empty Arrow record batch.");

            Assert.AreEqual(ExpectedChangeDataProjectedColumnNames.Length, batch.ColumnCount, "Unexpected projected change-data column count.");
            CollectionAssert.AreEqual(ExpectedChangeDataProjectedColumnNames, batch.Schema.FieldsList.Select(field => field.Name).ToArray());

            List<Dictionary<string, object?>> rows = FlattenRows(batches);
            Assert.IsTrue(rows.All(row => row.Keys.Count == ExpectedChangeDataProjectedColumnNames.Length), "Expected projected CDF query to return only selected columns.");
            Assert.IsFalse(rows.Any(row => Equals(row["_change_type"], "update_preimage")), "Expected projected CDF query to filter out update_preimage rows.");
            Assert.IsTrue(rows.Any(row => Equals(row["id"], 1) && Equals(row["name"], "a") && Equals(row["_change_type"], "insert")),
                "Expected insert CDF row for id=1.");
            Assert.IsTrue(rows.Any(row => Equals(row["id"], 2) && Equals(row["name"], "b2") && Equals(row["_change_type"], "update_postimage")),
                "Expected update_postimage CDF row for id=2.");
        }

        private static List<Dictionary<string, object?>> FlattenRows(IReadOnlyList<RecordBatch> batches)
        {
            var rows = new List<Dictionary<string, object?>>();

            foreach (RecordBatch batch in batches)
            {
                for (int rowIndex = 0; rowIndex < batch.Length; rowIndex++)
                {
                    var row = new Dictionary<string, object?>(batch.ColumnCount, StringComparer.Ordinal);
                    for (int columnIndex = 0; columnIndex < batch.ColumnCount; columnIndex++)
                    {
                        row[batch.Schema.GetFieldByIndex(columnIndex).Name] = GetValue(batch.Column(columnIndex), rowIndex);
                    }

                    rows.Add(row);
                }
            }

            return rows;
        }

        private static object? GetValue(IArrowArray array, int index)
        {
            return array switch
            {
                Int32Array int32Array => int32Array.GetValue(index),
                Int64Array int64Array => int64Array.GetValue(index),
                BooleanArray booleanArray => booleanArray.GetValue(index),
                DoubleArray doubleArray => doubleArray.GetValue(index),
                FloatArray floatArray => floatArray.GetValue(index),
                StringArray stringArray => stringArray.GetString(index),
                StringViewArray stringViewArray => stringViewArray.GetString(index),
                LargeStringArray largeStringArray => largeStringArray.GetString(index),
                TimestampArray timestampArray => timestampArray.GetTimestamp(index),
                Date32Array date32Array => date32Array.GetDateTimeOffset(index),
                Decimal128Array decimalArray => decimalArray.GetValue(index),
                _ => throw new AssertFailedException($"Unsupported OneLake Arrow array type: {array.GetType().FullName}"),
            };
        }

        private static void AssertExpectedColumnMappingProjectedRow(IReadOnlyList<RecordBatch> batches)
        {
            Assert.IsTrue(batches.Count > 0, "Expected at least one Arrow record batch.");

            int rowCount = batches.Sum(batch => checked((int)batch.Length));
            Assert.AreEqual(1, rowCount, "Expected exactly one projected column-mapping row.");

            RecordBatch batch = batches.FirstOrDefault(candidate => candidate.Length > 0)
                ?? throw new AssertFailedException("Expected at least one non-empty Arrow record batch.");

            Assert.AreEqual(ExpectedColumnMappingProjectedColumnNames.Length, batch.ColumnCount, "Unexpected column-mapping projection column count.");
            CollectionAssert.AreEqual(ExpectedColumnMappingProjectedColumnNames, batch.Schema.FieldsList.Select(field => field.Name).ToArray());

            int? idValue = ((Int32Array)batch.Column(0)).GetValue(0);
            Assert.IsTrue(idValue.HasValue, "Expected id to have a value.");
            Assert.AreEqual(1, idValue.GetValueOrDefault());
            Assert.AreEqual("Sample Product", GetStringValue(batch.Column(1), 0));
        }

        private static string AcquireBearerToken()
        {
            try
            {
                TokenCredential credential = CreateCertificateCredential();
                AccessToken accessToken = credential.GetToken(
                    new TokenRequestContext(new[] { StorageTokenScope }),
                    default);

                if (string.IsNullOrWhiteSpace(accessToken.Token))
                {
                    Assert.Inconclusive("Failed to acquire a production OneLake bearer token using ClientCertificateCredential.");
                }

                return accessToken.Token;
            }
            catch (AssertInconclusiveException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Assert.Inconclusive($"Production OneLake bearer-token auth was unavailable: {ex.Message}");
                throw;
            }
        }

        private static TokenCredential CreateCertificateCredential()
        {
            X509Certificate2 certificate = FindCertificate();
            return new ClientCertificateCredential(
                GetRequiredEnvironmentVariable(TenantIdEnvironmentVariable),
                GetRequiredEnvironmentVariable(ClientIdEnvironmentVariable),
                certificate);
        }

        private static X509Certificate2 FindCertificate()
        {
            X509Certificate2? certificate = FindCertificate(StoreLocation.CurrentUser) ?? FindCertificate(StoreLocation.LocalMachine);
            if (certificate == null)
            {
                Assert.Inconclusive(
                    "Could not find an installed client certificate matching the OneLake integration test environment configuration. " +
                    $"Set {CertificateThumbprintEnvironmentVariable}, {CertificateNameEnvironmentVariable}, or {CertificateSubjectEnvironmentVariable}.");
            }

            return certificate!;
        }

        private static X509Certificate2? FindCertificate(StoreLocation location)
        {
            using var store = new X509Store(StoreName.My, location);
            store.Open(OpenFlags.ReadOnly);

            return store.Certificates
                .Cast<X509Certificate2>()
                .Where(cert => cert.HasPrivateKey)
                .Where(cert => cert.NotAfter > DateTime.Now)
                .FirstOrDefault(IsConfiguredCertificateMatch);
        }

        private static bool IsConfiguredCertificateMatch(X509Certificate2 certificate)
        {
            string certificateThumbprint = GetOptionalEnvironmentVariable(CertificateThumbprintEnvironmentVariable);
            string certificateName = GetOptionalEnvironmentVariable(CertificateNameEnvironmentVariable);
            string certificateSubject = GetOptionalEnvironmentVariable(CertificateSubjectEnvironmentVariable);
            if (string.IsNullOrWhiteSpace(certificateThumbprint) &&
                string.IsNullOrWhiteSpace(certificateName) &&
                string.IsNullOrWhiteSpace(certificateSubject))
            {
                Assert.Inconclusive(
                    $"Set one of {CertificateThumbprintEnvironmentVariable}, {CertificateNameEnvironmentVariable}, or {CertificateSubjectEnvironmentVariable} before running OneLake integration tests.");
            }

            return MatchesThumbprint(certificate, certificateThumbprint) ||
                ContainsIgnoreCase(certificate.FriendlyName, certificateName) ||
                ContainsIgnoreCase(certificate.Subject, certificateSubject);
        }

        private static bool ContainsIgnoreCase(string? value, string search)
        {
            return !string.IsNullOrWhiteSpace(search) && value?.IndexOf(search, StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private static bool MatchesThumbprint(X509Certificate2 certificate, string thumbprint)
        {
            if (string.IsNullOrWhiteSpace(thumbprint))
            {
                return false;
            }

            return string.Equals(
                certificate.Thumbprint?.Replace(" ", string.Empty),
                thumbprint.Replace(" ", string.Empty),
                StringComparison.OrdinalIgnoreCase);
        }

        private static string GetRequiredEnvironmentVariable(string name)
        {
            string? value = Environment.GetEnvironmentVariable(name);
            if (string.IsNullOrWhiteSpace(value))
            {
                Assert.Inconclusive($"Set environment variable '{name}' before running OneLake integration tests.");
            }

            return value!;
        }

        private static string GetOptionalEnvironmentVariable(string name)
        {
            return Environment.GetEnvironmentVariable(name) ?? string.Empty;
        }

        private static void AssertExpectedSchema(Schema schema)
        {
            Assert.AreEqual(ExpectedColumnNames.Length, schema.FieldsList.Count, "Unexpected OneLake schema column count.");
            CollectionAssert.AreEqual(ExpectedColumnNames, schema.FieldsList.Select(field => field.Name).ToArray());

            Assert.IsInstanceOfType(schema.FieldsList[0].DataType, typeof(Int8Type));
            Assert.IsInstanceOfType(schema.FieldsList[1].DataType, typeof(Int16Type));
            Assert.IsInstanceOfType(schema.FieldsList[2].DataType, typeof(Int32Type));
            Assert.IsInstanceOfType(schema.FieldsList[3].DataType, typeof(Int64Type));
            Assert.IsInstanceOfType(schema.FieldsList[4].DataType, typeof(FloatType));
            Assert.IsInstanceOfType(schema.FieldsList[5].DataType, typeof(DoubleType));

            var decimalType = schema.FieldsList[6].DataType as Decimal128Type;
            Assert.IsNotNull(decimalType, "Expected c_decimal to be Decimal128Type.");
            Assert.AreEqual(20, decimalType!.Precision);
            Assert.AreEqual(5, decimalType.Scale);

            Assert.IsTrue(IsStringLikeType(schema.FieldsList[7].DataType), "Expected c_string to use an Arrow string-compatible type.");
            Assert.IsInstanceOfType(schema.FieldsList[8].DataType, typeof(BooleanType));
            Assert.IsTrue(IsBinaryLikeType(schema.FieldsList[9].DataType), "Expected c_binary to use an Arrow binary-compatible type.");
            Assert.IsInstanceOfType(schema.FieldsList[10].DataType, typeof(Date32Type));

            var timestampNtzType = schema.FieldsList[11].DataType as TimestampType;
            Assert.IsNotNull(timestampNtzType, "Expected c_timestamp_ntz to be TimestampType.");
            Assert.IsTrue(string.IsNullOrEmpty(timestampNtzType!.Timezone), "Expected c_timestamp_ntz to have no timezone.");

            var timestampType = schema.FieldsList[12].DataType as TimestampType;
            Assert.IsNotNull(timestampType, "Expected c_timestamp to be TimestampType.");
            Assert.IsFalse(string.IsNullOrEmpty(timestampType!.Timezone), "Expected c_timestamp to be timezone-aware.");

            foreach (Field field in schema.FieldsList)
            {
                Assert.IsTrue(field.IsNullable, $"Expected '{field.Name}' to be nullable.");
            }
        }

        private static void AssertExpectedSingleRowResult(IReadOnlyList<RecordBatch> batches)
        {
            Assert.IsTrue(batches.Count > 0, "Expected at least one Arrow record batch.");

            int rowCount = batches.Sum(batch => checked((int)batch.Length));
            Assert.AreEqual(1, rowCount, "Expected exactly one inserted OneLake row.");

            RecordBatch batch = batches.FirstOrDefault(candidate => candidate.Length > 0)
                ?? throw new AssertFailedException("Expected at least one non-empty Arrow record batch.");

            Assert.AreEqual(ExpectedColumnNames.Length, batch.ColumnCount, "Unexpected OneLake result column count.");
            AssertExpectedSchema(batch.Schema);

            sbyte? byteValue = ((Int8Array)batch.Column(0)).GetValue(0);
            short? shortValue = ((Int16Array)batch.Column(1)).GetValue(0);
            int? intValue = ((Int32Array)batch.Column(2)).GetValue(0);
            long? longValue = ((Int64Array)batch.Column(3)).GetValue(0);
            Assert.IsTrue(byteValue.HasValue, "Expected c_byte to have a value.");
            Assert.IsTrue(shortValue.HasValue, "Expected c_short to have a value.");
            Assert.IsTrue(intValue.HasValue, "Expected c_int to have a value.");
            Assert.IsTrue(longValue.HasValue, "Expected c_long to have a value.");
            Assert.AreEqual((sbyte)1, byteValue.GetValueOrDefault());
            Assert.AreEqual((short)10, shortValue.GetValueOrDefault());
            Assert.AreEqual(100, intValue.GetValueOrDefault());
            Assert.AreEqual(1000L, longValue.GetValueOrDefault());

            float? floatValue = ((FloatArray)batch.Column(4)).GetValue(0);
            Assert.IsTrue(floatValue.HasValue, "Expected c_float to have a value.");
            Assert.IsTrue(Math.Abs(floatValue.GetValueOrDefault() - 1.23f) < 0.0001f, $"Unexpected c_float value: {floatValue.GetValueOrDefault()}");

            double? doubleValue = ((DoubleArray)batch.Column(5)).GetValue(0);
            Assert.IsTrue(doubleValue.HasValue, "Expected c_double to have a value.");
            Assert.IsTrue(Math.Abs(doubleValue.GetValueOrDefault() - 2.3456d) < 0.0000001d, $"Unexpected c_double value: {doubleValue.GetValueOrDefault()}");

            Assert.AreEqual(12345.67890m, ((Decimal128Array)batch.Column(6)).GetValue(0));
            Assert.AreEqual("sample_string", GetStringValue(batch.Column(7), 0));

            bool? booleanValue = ((BooleanArray)batch.Column(8)).GetValue(0);
            Assert.IsTrue(booleanValue.HasValue, "Expected c_boolean to have a value.");
            Assert.AreEqual(true, booleanValue.GetValueOrDefault());

            CollectionAssert.AreEqual(ExpectedBinaryPayload, GetBinaryValue(batch.Column(9), 0));

            DateTimeOffset? dateValue = ((Date32Array)batch.Column(10)).GetDateTimeOffset(0);
            Assert.IsTrue(dateValue.HasValue, "Expected c_date to have a value.");
            DateTimeOffset expectedDateOffset = new DateTimeOffset(ExpectedDate, TimeSpan.Zero);
            Assert.AreEqual(expectedDateOffset, dateValue.GetValueOrDefault());

            var timestampNtzArray = (TimestampArray)batch.Column(11);
            DateTimeOffset? timestampNtzValue = timestampNtzArray.GetTimestamp(0);
            Assert.IsTrue(timestampNtzValue.HasValue, "Expected c_timestamp_ntz to have a value.");
            Assert.AreEqual(ExpectedTimestampNtz, timestampNtzValue.GetValueOrDefault().DateTime);

            var timestampArray = (TimestampArray)batch.Column(12);
            DateTimeOffset? timestampValue = timestampArray.GetTimestamp(0);
            Assert.IsTrue(timestampValue.HasValue, "Expected c_timestamp to have a value.");
            Assert.AreEqual(ExpectedTimestampLtz, timestampValue.GetValueOrDefault().ToUniversalTime());
        }

        private static IReadOnlyList<RecordBatch> ReadAllBatches(Apache.Arrow.Ipc.IArrowArrayStream stream)
        {
            var batches = new List<RecordBatch>();

            while (true)
            {
                RecordBatch? batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();
                if (batch == null)
                {
                    break;
                }

                batches.Add(batch);
            }

            return batches;
        }

        private static async Task<IReadOnlyList<RecordBatch>> ReadAllBatchesAsync(Apache.Arrow.Ipc.IArrowArrayStream stream)
        {
            var batches = new List<RecordBatch>();

            while (true)
            {
                RecordBatch? batch = await stream.ReadNextRecordBatchAsync().AsTask().ConfigureAwait(false);
                if (batch == null)
                {
                    break;
                }

                batches.Add(batch);
            }

            return batches;
        }

        private static bool IsStringLikeType(IArrowType dataType)
        {
            return dataType is StringType or StringViewType or LargeStringType;
        }

        private static bool IsBinaryLikeType(IArrowType dataType)
        {
            return dataType is BinaryType or BinaryViewType or LargeBinaryType;
        }

        private static string? GetStringValue(IArrowArray array, int index)
        {
            return array switch
            {
                StringArray stringArray => stringArray.GetString(index),
                StringViewArray stringViewArray => stringViewArray.GetString(index),
                LargeStringArray largeStringArray => largeStringArray.GetString(index),
                _ => throw new AssertFailedException($"Unexpected string array type: {array.GetType().FullName}"),
            };
        }

        private static byte[] GetBinaryValue(IArrowArray array, int index)
        {
            return array switch
            {
                BinaryArray binaryArray => binaryArray.GetBytes(index).ToArray(),
                BinaryViewArray binaryViewArray => binaryViewArray.GetBytes(index).ToArray(),
                LargeBinaryArray largeBinaryArray => largeBinaryArray.GetBytes(index).ToArray(),
                _ => throw new AssertFailedException($"Unexpected binary array type: {array.GetType().FullName}"),
            };
        }

        private static object? ReadInfoValue(RecordBatch batch, AdbcInfoCode code)
        {
            var infoNames = (UInt32Array)batch.Column(0);
            var infoValues = (DenseUnionArray)batch.Column(1);

            for (int i = 0; i < batch.Length; i++)
            {
                if (infoNames.GetValue(i) == (uint)code)
                {
                    int childIndex = infoValues.TypeIds[i];
                    int valueOffset = infoValues.ValueOffsets[i];

                    switch (childIndex)
                    {
                        case 0:
                            return ((StringArray)infoValues.Fields[0]).GetString(valueOffset);
                        case 1:
                            return ((BooleanArray)infoValues.Fields[1]).GetValue(valueOffset);
                        case 2:
                            return ((Int64Array)infoValues.Fields[2]).GetValue(valueOffset);
                        case 3:
                            return ((Int32Array)infoValues.Fields[3]).GetValue(valueOffset);
                    }
                }
            }

            Assert.Fail($"Info code '{code}' was not present in the metadata batch.");
            return null;
        }

        private static void AssertListLength(ListArray array, int index, int expectedLength)
        {
            int actualLength = array.ValueOffsets[index + 1] - array.ValueOffsets[index];
            Assert.AreEqual(expectedLength, actualLength);
        }

        private sealed class OpenedOneLakeConnection : IDisposable
        {
            private readonly AdbcDatabase _database;
            private readonly AdbcDriver _driver;

            public OpenedOneLakeConnection(AdbcDriver driver, AdbcDatabase database, AdbcConnection connection)
            {
                _driver = driver;
                _database = database;
                Connection = connection;
            }

            public AdbcConnection Connection { get; }

            public void Dispose()
            {
                Connection.Dispose();
                _database.Dispose();
                _driver.Dispose();
            }
        }
    }
}
