using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using DeltaLakeSharp.Adbc.Internal;
using DeltaLakeSharp.Client;
using DeltaLakeSharp.Client.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DeltaLakeSharp.Adbc.Tests
{
    [TestClass]
    [TestCategory("Integration")]
    [TestCategory("V3")]
    [TestCategory("V3Native")]
    public class DeltaAdbcIntegrationTests
    {
        private string? _tempDir;

        [TestCleanup]
        public void TestCleanup()
        {
            if (!string.IsNullOrEmpty(_tempDir) && Directory.Exists(_tempDir))
            {
                try
                {
                    Directory.Delete(_tempDir, recursive: true);
                }
                catch
                {
                }
            }
        }

        [TestMethod]
        public void Driver_OpenConnectReadAndQuery_Succeeds()
        {
            using OpenedConnection opened = OpenConnection();
            AdbcConnection connection = opened.Connection;

            try
            {
                Schema schema = connection.GetTableSchema(null, null, DeltaAdbcConnectOptions.LogicalTableName);
                Assert.AreEqual(2, schema.FieldsList.Count);
                Assert.AreEqual("id", schema.FieldsList[0].Name);
                Assert.AreEqual(Int32Type.Default.TypeId, schema.FieldsList[0].DataType.TypeId);
                Assert.AreEqual("name", schema.FieldsList[1].Name);

                var readStatement = connection.CreateStatement();
                try
                {
                    QueryResult readResult = readStatement.ExecuteQuery();
                    var readStream = readResult.Stream ?? throw new AssertFailedException("Read stream should not be null.");
                    try
                    {
                        List<(int id, string name)> rows = ReadAllRowsSorted(readStream);
                        Assert.AreEqual(3, rows.Count);
                        Assert.AreEqual((1, "a"), rows[0]);
                        Assert.AreEqual((2, "b"), rows[1]);
                        Assert.AreEqual((3, "c"), rows[2]);
                    }
                    finally
                    {
                        readStream.Dispose();
                    }
                }
                finally
                {
                    readStatement.Dispose();
                }

                var sqlStatement = connection.CreateStatement();
                try
                {
                    sqlStatement.SqlQuery = "SELECT id, name FROM delta_table WHERE id >= 2 ORDER BY id";
                    QueryResult sqlResult = sqlStatement.ExecuteQuery();
                    var sqlStream = sqlResult.Stream ?? throw new AssertFailedException("SQL stream should not be null.");
                    try
                    {
                        List<(int id, string name)> rows = ReadAllRowsSorted(sqlStream);
                        Assert.AreEqual(2, rows.Count);
                        Assert.AreEqual((2, "b"), rows[0]);
                        Assert.AreEqual((3, "c"), rows[1]);
                    }
                    finally
                    {
                        sqlStream.Dispose();
                    }
                }
                finally
                {
                    sqlStatement.Dispose();
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_GetInfo_ReturnsExpectedMetadata()
        {
            using OpenedConnection opened = OpenConnection();
            AdbcConnection connection = opened.Connection;

            try
            {
                using var stream = connection.GetInfo(System.Array.Empty<AdbcInfoCode>());
                RecordBatch? batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();

                Assert.IsNotNull(batch);
                Assert.AreEqual(8, batch.Length);
                Assert.AreEqual("Delta Lake", ReadInfoValue(batch, AdbcInfoCode.VendorName));
                Assert.AreEqual(true, ReadInfoValue(batch, AdbcInfoCode.VendorSql));
                Assert.AreEqual("DeltaLakeSharp.Adbc", ReadInfoValue(batch, AdbcInfoCode.DriverName));
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_OpenWithBatchSize_ReturnsMultipleSingleRowBatches()
        {
            using OpenedConnection opened = OpenConnection();
            AdbcConnection connection = opened.Connection;

            try
            {
                var statement = connection.CreateStatement();
                try
                {
                    statement.SetOption(DeltaAdbcStatementOptions.BatchSizeOptionKey, "1");
                    statement.SqlQuery = "SELECT id, name FROM delta_table ORDER BY id";
                    QueryResult sqlResult = statement.ExecuteQuery();
                    var sqlStream = sqlResult.Stream ?? throw new AssertFailedException("SQL stream should not be null.");
                    try
                    {
                        var batches = ReadAllBatches(sqlStream);

                        Assert.IsTrue(batches.Count >= 3, "Expected multiple batches when delta.batch_size=1.");
                        Assert.IsTrue(batches.TrueForAll(batch => batch.Length <= 1), "Expected each batch to contain at most one row.");

                        List<(int id, string name)> rows = ReadAllRowsSorted(batches);

                        Assert.AreEqual(3, rows.Count);
                        Assert.AreEqual((1, "a"), rows[0]);
                        Assert.AreEqual((2, "b"), rows[1]);
                        Assert.AreEqual((3, "c"), rows[2]);
                    }
                    finally
                    {
                        sqlStream.Dispose();
                    }
                }
                finally
                {
                    statement.Dispose();
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_OpenReadTableWithBatchSize_ReturnsMultipleSingleRowBatches()
        {
            using OpenedConnection opened = OpenConnection();
            AdbcConnection connection = opened.Connection;

            try
            {
                var statement = connection.CreateStatement();
                try
                {
                    statement.SetOption(DeltaAdbcStatementOptions.BatchSizeOptionKey, "1");
                    QueryResult readResult = statement.ExecuteQuery();
                    var readStream = readResult.Stream ?? throw new AssertFailedException("Read stream should not be null.");
                    try
                    {
                        var batches = ReadAllBatches(readStream);

                        Assert.IsTrue(batches.Count >= 3, "Expected multiple batches when delta.batch_size=1.");
                        Assert.IsTrue(batches.TrueForAll(batch => batch.Length <= 1), "Expected each batch to contain at most one row.");

                        List<(int id, string name)> rows = ReadAllRowsSorted(batches);
                        Assert.AreEqual(3, rows.Count);
                        Assert.AreEqual((1, "a"), rows[0]);
                        Assert.AreEqual((2, "b"), rows[1]);
                        Assert.AreEqual((3, "c"), rows[2]);
                    }
                    finally
                    {
                        readStream.Dispose();
                    }
                }
                finally
                {
                    statement.Dispose();
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_OpenReadTable_WithStatementVersion_ReadsHistoricalSnapshot()
        {
            using OpenedConnection opened = OpenConnectionWithTimeTravel();
            AdbcConnection connection = opened.Connection;

            try
            {
                var statement = connection.CreateStatement();
                try
                {
                    statement.SetOption(DeltaAdbcStatementOptions.VersionOptionKey, "0");
                    QueryResult readResult = statement.ExecuteQuery();
                    var readStream = readResult.Stream ?? throw new AssertFailedException("Read stream should not be null.");
                    try
                    {
                        List<(int id, string name)> rows = ReadAllRowsSorted(readStream);
                        Assert.AreEqual(2, rows.Count);
                        Assert.AreEqual((1, "v0_a"), rows[0]);
                        Assert.AreEqual((2, "v0_b"), rows[1]);
                    }
                    finally
                    {
                        readStream.Dispose();
                    }
                }
                finally
                {
                    statement.Dispose();
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_GetTableSchema_WithConnectionVersion_ReturnsHistoricalSchema()
        {
            using OpenedConnection opened = OpenConnectionWithSchemaEvolution(new Dictionary<string, string>
            {
                [DeltaAdbcStatementOptions.VersionOptionKey] = "1",
            });
            AdbcConnection connection = opened.Connection;

            try
            {
                Schema schema = connection.GetTableSchema(null, null, DeltaAdbcConnectOptions.LogicalTableName);

                Assert.AreEqual(2, schema.FieldsList.Count);
                Assert.AreEqual("id", schema.FieldsList[0].Name);
                Assert.AreEqual("name", schema.FieldsList[1].Name);
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_GetObjects_WithConnectionVersion_ReturnsHistoricalColumns()
        {
            using OpenedConnection opened = OpenConnectionWithSchemaEvolution(new Dictionary<string, string>
            {
                [DeltaAdbcStatementOptions.VersionOptionKey] = "1",
            });
            AdbcConnection connection = opened.Connection;

            try
            {
                using var stream = connection.GetObjects(
                    AdbcConnection.GetObjectsDepth.All,
                    null,
                    null,
                    null,
                    null,
                    null);
                RecordBatch? batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();

                Assert.IsNotNull(batch);

                StructArray columnValues = GetColumnStructArray(batch!);
                var columnNames = (StringArray)columnValues.Fields[0];

                Assert.AreEqual(2, columnValues.Length);
                Assert.AreEqual("id", columnNames.GetString(0));
                Assert.AreEqual("name", columnNames.GetString(1));
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_ConnectionVersion_DefaultsStatementReadsToHistoricalSnapshot()
        {
            using OpenedConnection opened = OpenConnectionWithSchemaEvolution(new Dictionary<string, string>
            {
                [DeltaAdbcStatementOptions.VersionOptionKey] = "1",
            });
            AdbcConnection connection = opened.Connection;

            try
            {
                using var statement = connection.CreateStatement();
                QueryResult readResult = statement.ExecuteQuery();
                var readStream = readResult.Stream ?? throw new AssertFailedException("Read stream should not be null.");
                try
                {
                    List<(int id, string name)> rows = ReadAllRowsSorted(readStream);
                    Assert.AreEqual(2, rows.Count);
                    Assert.AreEqual((1, "alice"), rows[0]);
                    Assert.AreEqual((2, "bob"), rows[1]);
                }
                finally
                {
                    readStream.Dispose();
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_StatementVersion_OverridesConnectionVersion_ForSchemaEvolutionFixture()
        {
            using OpenedConnection opened = OpenConnectionWithSchemaEvolution(new Dictionary<string, string>
            {
                [DeltaAdbcStatementOptions.VersionOptionKey] = "1",
            });
            AdbcConnection connection = opened.Connection;

            try
            {
                using var statement = connection.CreateStatement();
                statement.SetOption(DeltaAdbcStatementOptions.VersionOptionKey, "2");
                QueryResult readResult = statement.ExecuteQuery();
                var readStream = readResult.Stream ?? throw new AssertFailedException("Read stream should not be null.");
                try
                {
                    var rows = ReadAllDictionaryRows(readStream).OrderBy(row => Convert.ToInt32(row["id"])).ToList();
                    Assert.AreEqual(2, rows.Count);
                    Assert.IsTrue(rows.All(row => row.ContainsKey("city")));
                    Assert.IsTrue(rows.All(row => row.ContainsKey("active")));
                    Assert.AreEqual("Seattle", rows[0]["city"]);
                    Assert.AreEqual(true, rows[0]["active"]);
                }
                finally
                {
                    readStream.Dispose();
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_ExecutePartitioned_AndReadPartitions_ReturnsAllRowsForPartitionedFixture()
        {
            using OpenedConnection opened = OpenConnectionWithPartitionedData();
            AdbcConnection connection = opened.Connection;

            try
            {
                var statement = connection.CreateStatement();
                try
                {
                    PartitionedResult result = statement.ExecutePartitioned();

                    Assert.AreEqual(3, result.Schema.FieldsList.Count);
                    Assert.AreEqual("region", result.Schema.FieldsList[2].Name);
                    Assert.IsTrue(result.PartitionDescriptors.Count >= 1);

                    var rows = new List<Dictionary<string, object?>>();
                    foreach (PartitionDescriptor descriptor in result.PartitionDescriptors)
                    {
                        using IArrowArrayStream partitionStream = connection.ReadPartition(descriptor);
                        rows.AddRange(ReadAllDictionaryRows(partitionStream));
                    }

                    Assert.AreEqual(5, rows.Count);
                    Assert.IsTrue(rows.Exists(row => Equals(row["id"], 1) && Equals(row["region"], "us")));
                    Assert.IsTrue(rows.Exists(row => Equals(row["id"], 2) && Equals(row["region"], "eu")));
                    Assert.IsTrue(rows.Exists(row => Equals(row["id"], 5) && Equals(row["region"], "apac")));
                }
                finally
                {
                    statement.Dispose();
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_ExecutePartitioned_AndReadPartitions_ReturnsAllRowsForNonPartitionedFixture()
        {
            using OpenedConnection opened = OpenConnectionWithSimpleData();
            AdbcConnection connection = opened.Connection;

            try
            {
                var statement = connection.CreateStatement();
                try
                {
                    PartitionedResult result = statement.ExecutePartitioned();

                    Assert.AreEqual(2, result.Schema.FieldsList.Count);
                    Assert.AreEqual("name", result.Schema.FieldsList[1].Name);
                    Assert.IsTrue(result.PartitionDescriptors.Count >= 1);

                    var rows = new List<Dictionary<string, object?>>();
                    foreach (PartitionDescriptor descriptor in result.PartitionDescriptors)
                    {
                        using IArrowArrayStream partitionStream = connection.ReadPartition(descriptor);
                        rows.AddRange(ReadAllDictionaryRows(partitionStream));
                    }

                    Assert.AreEqual(3, rows.Count);
                    Assert.IsTrue(rows.Exists(row => Equals(row["id"], 1) && Equals(row["name"], "a")));
                    Assert.IsTrue(rows.Exists(row => Equals(row["id"], 2) && Equals(row["name"], "b")));
                    Assert.IsTrue(rows.Exists(row => Equals(row["id"], 3) && Equals(row["name"], "c")));
                }
                finally
                {
                    statement.Dispose();
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_ExecutePartitioned_AndReadPartitions_ForNonPartitionedMultiFileTable_SplitsAndReturnsAllRows()
        {
            using OpenedConnection opened = OpenConnectionWithMultiFileSimpleData();
            AdbcConnection connection = opened.Connection;

            try
            {
                var statement = connection.CreateStatement();
                try
                {
                    PartitionedResult result = statement.ExecutePartitioned();

                    Assert.AreEqual(2, result.Schema.FieldsList.Count);
                    Assert.AreEqual("name", result.Schema.FieldsList[1].Name);
                    Assert.IsTrue(result.PartitionDescriptors.Count > 1, "Expected multiple planned partitions.");

                    List<(int id, string name)> rows = new();
                    foreach (PartitionDescriptor descriptor in result.PartitionDescriptors)
                    {
                        using IArrowArrayStream partitionStream = connection.ReadPartition(descriptor);
                        rows.AddRange(ReadAllRowsSorted(partitionStream));
                    }

                    rows = rows.OrderBy(row => row.id).ToList();
                    Assert.AreEqual(8, rows.Count);
                    Assert.AreEqual((1, "row_1"), rows[0]);
                    Assert.AreEqual((8, "row_8"), rows[7]);
                }
                finally
                {
                    statement.Dispose();
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_ExecutePartitioned_OnDeletionVectorFixture_FailsFast()
        {
            using OpenedConnection opened = OpenConnectionToExistingFixture("delta_test_deletion_vector");
            AdbcConnection connection = opened.Connection;

            try
            {
                var statement = connection.CreateStatement();
                try
                {
                    Exception exception = Assert.ThrowsException<InvalidOperationException>(() => statement.ExecutePartitioned());
                    StringAssert.Contains(exception.Message, "partitioned reads are not yet supported");
                    StringAssert.Contains(exception.Message, "deletion vectors");
                }
                finally
                {
                    statement.Dispose();
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_ExecutePartitioned_AndReadPartitions_OnPartitionedDeletionVectorFixture_ReturnsDeletionVectorCorrectRows()
        {
            using OpenedConnection opened = OpenConnectionToExistingFixture("delta_test_partitioned_deletion_vector");
            AdbcConnection connection = opened.Connection;

            try
            {
                var statement = connection.CreateStatement();
                try
                {
                    PartitionedResult result = statement.ExecutePartitioned();

                    Assert.AreEqual(3, result.Schema.FieldsList.Count);
                    Assert.AreEqual("region", result.Schema.FieldsList[2].Name);
                    Assert.IsTrue(result.PartitionDescriptors.Count >= 1);

                    var rows = new List<Dictionary<string, object?>>();
                    foreach (PartitionDescriptor descriptor in result.PartitionDescriptors)
                    {
                        using IArrowArrayStream partitionStream = connection.ReadPartition(descriptor);
                        rows.AddRange(ReadAllDictionaryRows(partitionStream));
                    }

                    Assert.AreEqual(4, rows.Count);
                    Assert.IsTrue(rows.Exists(row => Equals(row["id"], 1L) && Equals(row["region"], "us")));
                    Assert.IsTrue(rows.Exists(row => Equals(row["id"], 2L) && Equals(row["region"], "eu")));
                    Assert.IsTrue(rows.Exists(row => Equals(row["id"], 4L) && Equals(row["region"], "eu")));
                    Assert.IsTrue(rows.Exists(row => Equals(row["id"], 5L) && Equals(row["region"], "apac")));
                    Assert.IsFalse(rows.Exists(row => Equals(row["id"], 3L)), "Deleted DV row should not be returned.");
                }
                finally
                {
                    statement.Dispose();
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_ExecutePartitioned_OnChangeDataFeedTable_FailsFast()
        {
            using OpenedConnection opened = OpenConnectionWithChangeData();
            AdbcConnection connection = opened.Connection;

            try
            {
                var statement = connection.CreateStatement();
                try
                {
                    statement.SetOption(DeltaAdbcStatementOptions.CdfStartingVersionOptionKey, "1");

                    AdbcException exception = Assert.ThrowsException<AdbcException>(() => statement.ExecutePartitioned());

                    Assert.AreEqual(AdbcStatusCode.InvalidArgument, exception.Status);
                    StringAssert.Contains(exception.Message, "Change Data Feed");
                }
                finally
                {
                    statement.Dispose();
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_OpenSqlQuery_WithStatementVersion_ReadsHistoricalSnapshot()
        {
            using OpenedConnection opened = OpenConnectionWithTimeTravel();
            AdbcConnection connection = opened.Connection;

            try
            {
                var statement = connection.CreateStatement();
                try
                {
                    statement.SetOption(DeltaAdbcStatementOptions.VersionOptionKey, "0");
                    statement.SqlQuery = "SELECT id, name FROM delta_table ORDER BY id";
                    QueryResult sqlResult = statement.ExecuteQuery();
                    var sqlStream = sqlResult.Stream ?? throw new AssertFailedException("SQL stream should not be null.");
                    try
                    {
                        List<(int id, string name)> rows = ReadAllRowsSorted(sqlStream);
                        Assert.AreEqual(2, rows.Count);
                        Assert.AreEqual((1, "v0_a"), rows[0]);
                        Assert.AreEqual((2, "v0_b"), rows[1]);
                    }
                    finally
                    {
                        sqlStream.Dispose();
                    }
                }
                finally
                {
                    statement.Dispose();
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_StatementBatchSize_OverridesConnectionDefault()
        {
            using OpenedConnection opened = OpenConnection();
            AdbcConnection connection = opened.Connection;

            try
            {
                var statement = connection.CreateStatement();
                try
                {
                    statement.SetOption(DeltaAdbcStatementOptions.BatchSizeOptionKey, "1");
                    QueryResult readResult = statement.ExecuteQuery();
                    var readStream = readResult.Stream ?? throw new AssertFailedException("Read stream should not be null.");
                    try
                    {
                        var batches = ReadAllBatches(readStream);

                        Assert.IsTrue(batches.Count >= 3, "Expected statement batch size to override connection default.");
                        Assert.IsTrue(batches.TrueForAll(batch => batch.Length <= 1), "Expected each batch to contain at most one row.");
                    }
                    finally
                    {
                        readStream.Dispose();
                    }
                }
                finally
                {
                    statement.Dispose();
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_GetObjects_ReturnsLogicalTableAndColumns()
        {
            using OpenedConnection opened = OpenConnection();
            AdbcConnection connection = opened.Connection;

            try
            {
                Schema schema = connection.GetTableSchema(null, null, DeltaAdbcConnectOptions.LogicalTableName);

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
                Assert.AreEqual(string.Empty, ((StringArray)batch.Column(0)).GetString(0));

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
                AssertListLength(columns, 0, 2);

                var columnValues = (StructArray)columns.Values;
                var columnNames = (StringArray)columnValues.Fields[0];
                var ordinalPositions = (Int32Array)columnValues.Fields[1];
                var xdbcTypeNames = (StringArray)columnValues.Fields[4];
                var nullableFlags = (StringArray)columnValues.Fields[13];

                Assert.AreEqual("id", columnNames.GetString(0));
                Assert.AreEqual(1, ordinalPositions.GetValue(0));
                Assert.AreEqual(schema.FieldsList[0].DataType.Name, xdbcTypeNames.GetString(0));
                Assert.AreEqual("NO", nullableFlags.GetString(0));

                Assert.AreEqual("name", columnNames.GetString(1));
                Assert.AreEqual(2, ordinalPositions.GetValue(1));
                Assert.AreEqual(schema.FieldsList[1].DataType.Name, xdbcTypeNames.GetString(1));
                Assert.AreEqual("YES", nullableFlags.GetString(1));

                var constraints = (ListArray)tableValues.Fields[3];
                AssertListLength(constraints, 0, 0);
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_GetTableTypes_ReturnsSingleTableType()
        {
            using OpenedConnection opened = OpenConnection();
            AdbcConnection connection = opened.Connection;

            try
            {
                using var stream = connection.GetTableTypes();
                RecordBatch? batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();

                Assert.IsNotNull(batch);
                Assert.AreEqual(1, batch.Length);
                Assert.AreEqual("TABLE", ((StringArray)batch.Column(0)).GetString(0));
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_GetObjects_WithMismatchedTableTypeFilter_ReturnsEmptyBatch()
        {
            using OpenedConnection opened = OpenConnection();
            AdbcConnection connection = opened.Connection;

            try
            {
                using var stream = connection.GetObjects(
                    AdbcConnection.GetObjectsDepth.All,
                    null,
                    null,
                    null,
                    new[] { "VIEW" },
                    null);
                RecordBatch? batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();

                Assert.IsNotNull(batch);
                Assert.AreEqual(0, batch.Length);
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_GetObjects_WithMismatchedTablePattern_ReturnsEmptyBatch()
        {
            using OpenedConnection opened = OpenConnection();
            AdbcConnection connection = opened.Connection;

            try
            {
                using var stream = connection.GetObjects(
                    AdbcConnection.GetObjectsDepth.All,
                    null,
                    null,
                    "other_table",
                    null,
                    null);
                RecordBatch? batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();

                Assert.IsNotNull(batch);
                Assert.AreEqual(0, batch.Length);
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_GetObjects_WithColumnPattern_FiltersColumns()
        {
            using OpenedConnection opened = OpenConnection();
            AdbcConnection connection = opened.Connection;

            try
            {
                using var stream = connection.GetObjects(
                    AdbcConnection.GetObjectsDepth.All,
                    null,
                    null,
                    null,
                    null,
                    "id");
                RecordBatch? batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();

                Assert.IsNotNull(batch);
                Assert.AreEqual(1, batch.Length);

                var columns = GetColumnStructArray(batch);
                Assert.AreEqual(1, columns.Length);

                var columnNames = (StringArray)columns.Fields[0];
                var ordinalPositions = (Int32Array)columns.Fields[1];
                Assert.AreEqual("id", columnNames.GetString(0));
                Assert.AreEqual(1, ordinalPositions.GetValue(0));
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_GetObjects_WithTablesDepth_OmitsColumnRows()
        {
            using OpenedConnection opened = OpenConnection();
            AdbcConnection connection = opened.Connection;

            try
            {
                using var stream = connection.GetObjects(
                    AdbcConnection.GetObjectsDepth.Tables,
                    null,
                    null,
                    null,
                    null,
                    null);
                RecordBatch? batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();

                Assert.IsNotNull(batch);
                Assert.AreEqual(1, batch.Length);

                var columns = GetColumnStructArray(batch);
                Assert.AreEqual(0, columns.Length);
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_ExecuteQuery_WithStatementCdfDirectRead_ReturnsChangeDataRows()
        {
            using OpenedConnection opened = OpenConnectionWithChangeData();
            AdbcConnection connection = opened.Connection;

            try
            {
                var statement = connection.CreateStatement();
                try
                {
                    statement.SetOption(DeltaAdbcStatementOptions.CdfStartingVersionOptionKey, "1");
                    QueryResult result = statement.ExecuteQuery();
                    var stream = result.Stream ?? throw new AssertFailedException("CDF stream should not be null.");
                    try
                    {
                        List<Dictionary<string, object?>> rows = ReadAllDictionaryRows(stream);
                        Assert.IsTrue(rows.Count >= 3, $"Expected multiple change rows, got {rows.Count}.");
                        Assert.IsTrue(rows.TrueForAll(row => row.ContainsKey("_change_type")));
                        Assert.IsTrue(rows.TrueForAll(row => row.ContainsKey("_commit_version")));
                        Assert.IsTrue(rows.TrueForAll(row => row.ContainsKey("_commit_timestamp")));
                        Assert.IsTrue(rows.Exists(row => Equals(row["id"], 1) && Equals(row["_change_type"], "insert")));
                        Assert.IsTrue(rows.Exists(row => Equals(row["id"], 2) && Equals(row["name"], "b2") && Equals(row["_change_type"], "update_postimage")));
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
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_ExecuteQuery_WithStatementCdfProjectionAndFilter_ReturnsRows()
        {
            using OpenedConnection opened = OpenConnectionWithChangeData();
            AdbcConnection connection = opened.Connection;

            try
            {
                var statement = connection.CreateStatement();
                try
                {
                    statement.SetOption(DeltaAdbcStatementOptions.CdfStartingVersionOptionKey, "1");
                    statement.SqlQuery = "SELECT id, name, _change_type FROM _cdf WHERE _change_type <> 'update_preimage' ORDER BY id, _change_type";
                    QueryResult result = statement.ExecuteQuery();
                    var stream = result.Stream ?? throw new AssertFailedException("CDF SQL stream should not be null.");
                    try
                    {
                        List<Dictionary<string, object?>> rows = ReadAllDictionaryRows(stream);
                        Assert.IsTrue(rows.Count >= 3, $"Expected at least 3 filtered change rows, got {rows.Count}.");
                        Assert.IsTrue(rows.TrueForAll(row => row.ContainsKey("id") && row.ContainsKey("name") && row.ContainsKey("_change_type")));
                        Assert.IsFalse(rows.Exists(row => row.ContainsKey("_commit_version")));
                        Assert.IsFalse(rows.Exists(row => Equals(row["_change_type"], "update_preimage")));
                        Assert.IsTrue(rows.Exists(row => Equals(row["id"], 1) && Equals(row["name"], "a") && Equals(row["_change_type"], "insert")));
                        Assert.IsTrue(rows.Exists(row => Equals(row["id"], 2) && Equals(row["name"], "b2") && Equals(row["_change_type"], "update_postimage")));
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
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_ExecuteQuery_WithCdfEndingVersionLessThanStart_ThrowsInvalidArgument()
        {
            using OpenedConnection opened = OpenConnectionWithChangeData();
            AdbcConnection connection = opened.Connection;

            try
            {
                var statement = connection.CreateStatement();
                try
                {
                    statement.SetOption(DeltaAdbcStatementOptions.CdfStartingVersionOptionKey, "9");

                    AdbcException exception = Assert.ThrowsException<AdbcException>(() => statement.SetOption(DeltaAdbcStatementOptions.CdfEndingVersionOptionKey, "5"));

                    Assert.AreEqual(AdbcStatusCode.InvalidArgument, exception.Status);
                    StringAssert.Contains(exception.Message, DeltaAdbcStatementOptions.CdfEndingVersionOptionKey);
                }
                finally
                {
                    statement.Dispose();
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_ExecuteQuery_WithCdfStatementOptionsAndSqlWithoutCdfReference_ThrowsInvalidArgument()
        {
            using OpenedConnection opened = OpenConnectionWithChangeData();
            AdbcConnection connection = opened.Connection;

            try
            {
                var statement = connection.CreateStatement();
                try
                {
                    statement.SetOption(DeltaAdbcStatementOptions.CdfStartingVersionOptionKey, "1");
                    statement.SqlQuery = "SELECT * FROM delta_table";

                    AdbcException exception = Assert.ThrowsException<AdbcException>(() => statement.ExecuteQuery());

                    Assert.AreEqual(AdbcStatusCode.InvalidArgument, exception.Status);
                    StringAssert.Contains(exception.Message, "_cdf");
                }
                finally
                {
                    statement.Dispose();
                }
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_GetTableSchema_WithCatalog_ThrowsInvalidArgument()
        {
            using OpenedConnection opened = OpenConnection();
            AdbcConnection connection = opened.Connection;

            try
            {
                AdbcException exception = Assert.ThrowsException<AdbcException>(
                    () => connection.GetTableSchema("catalog", null, DeltaAdbcConnectOptions.LogicalTableName));

                Assert.AreEqual(AdbcStatusCode.InvalidArgument, exception.Status);
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_GetTableSchema_WithDbSchema_ThrowsInvalidArgument()
        {
            using OpenedConnection opened = OpenConnection();
            AdbcConnection connection = opened.Connection;

            try
            {
                AdbcException exception = Assert.ThrowsException<AdbcException>(
                    () => connection.GetTableSchema(null, "schema", DeltaAdbcConnectOptions.LogicalTableName));

                Assert.AreEqual(AdbcStatusCode.InvalidArgument, exception.Status);
            }
            finally
            {
                connection.Dispose();
            }
        }

        [TestMethod]
        public void Driver_GetTableSchema_WithUnknownLogicalTable_ThrowsNotFound()
        {
            using OpenedConnection opened = OpenConnection();
            AdbcConnection connection = opened.Connection;

            try
            {
                AdbcException exception = Assert.ThrowsException<AdbcException>(
                    () => connection.GetTableSchema(null, null, "other_table"));

                Assert.AreEqual(AdbcStatusCode.NotFound, exception.Status);
            }
            finally
            {
                connection.Dispose();
            }
        }

        private static List<(int id, string name)> ReadAllRowsSorted(Apache.Arrow.Ipc.IArrowArrayStream stream)
        {
            return ReadAllRowsSorted(ReadAllBatches(stream));
        }

        private static List<Dictionary<string, object?>> ReadAllDictionaryRows(Apache.Arrow.Ipc.IArrowArrayStream stream)
        {
            return ArrowConverter.ToDictionaryList(ReadAllBatches(stream));
        }

        private static List<RecordBatch> ReadAllBatches(Apache.Arrow.Ipc.IArrowArrayStream stream)
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

        private static List<(int id, string name)> ReadAllRowsSorted(IEnumerable<RecordBatch> batches)
        {
            var rows = new List<(int id, string name)>();

            foreach (RecordBatch batch in batches)
            {
                var idArray = (Int32Array)batch.Column(0);
                IArrowArray nameArray = batch.Column(1);

                for (int i = 0; i < batch.Length; i++)
                {
                    int id = idArray.GetValue(i) ?? -1;
                    string name = ReadStringValue(nameArray, i);
                    rows.Add((id, name));
                }
            }

            rows.Sort((left, right) => left.id.CompareTo(right.id));
            return rows;
        }

        private static string ReadStringValue(IArrowArray array, int index)
        {
            return array switch
            {
                StringArray sa => sa.GetString(index),
                StringViewArray sva => sva.GetString(index),
                LargeStringArray lsa => lsa.GetString(index),
                _ => throw new AssertFailedException($"Unexpected string column type: {array.GetType().FullName}"),
            } ?? string.Empty;
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

        private static StructArray GetColumnStructArray(RecordBatch batch)
        {
            var dbSchemas = (ListArray)batch.Column(1);
            var dbSchemaValues = (StructArray)dbSchemas.Values;
            var tables = (ListArray)dbSchemaValues.Fields[1];
            var tableValues = (StructArray)tables.Values;
            var columns = (ListArray)tableValues.Fields[2];
            return (StructArray)columns.Values;
        }

        private OpenedConnection OpenConnection(IReadOnlyDictionary<string, string>? extraOptions = null)
        {
            string? binaryPath = FindRustFixtureBinary();
            if (binaryPath == null)
            {
                Assert.Inconclusive(
                    "Rust fixture binary not found. Build it first: cd src/DeltaLakeSharp.Server/v3 && cargo build");
            }

            _tempDir = Path.Combine(Path.GetTempPath(), $"adbc_v3_{Guid.NewGuid():N}");
            string tablePath = Path.Combine(_tempDir, "test_table");
            CreateTestDeltaTable(binaryPath!, tablePath);

            var options = new Dictionary<string, string>
            {
                [DeltaAdbcConnectOptions.TableUriKey] = tablePath,
            };

            if (extraOptions != null)
            {
                foreach (KeyValuePair<string, string> option in extraOptions)
                {
                    options[option.Key] = option.Value;
                }
            }

            var driver = new DeltaAdbcDriver();
            var database = driver.Open(options);
            var connection = database.Connect(null);

            return new OpenedConnection(driver, database, connection);
        }

        private OpenedConnection OpenConnectionWithChangeData(IReadOnlyDictionary<string, string>? extraOptions = null)
        {
            string? binaryPath = FindRustFixtureBinary();
            if (binaryPath == null)
            {
                Assert.Inconclusive(
                    "Rust fixture binary not found. Build it first: cd src/DeltaLakeSharp.Server/v3 && cargo build");
            }

            _tempDir = Path.Combine(Path.GetTempPath(), $"adbc_v3_cdf_{Guid.NewGuid():N}");
            string tablePath = Path.Combine(_tempDir, "test_table");
            CreateChangeDataDeltaTable(tablePath);

            var options = new Dictionary<string, string>
            {
                [DeltaAdbcConnectOptions.TableUriKey] = tablePath,
            };

            if (extraOptions != null)
            {
                foreach (KeyValuePair<string, string> option in extraOptions)
                {
                    options[option.Key] = option.Value;
                }
            }

            var driver = new DeltaAdbcDriver();
            var database = driver.Open(options);
            var connection = database.Connect(null);

            return new OpenedConnection(driver, database, connection);
        }

        private OpenedConnection OpenConnectionWithTimeTravel(IReadOnlyDictionary<string, string>? extraOptions = null)
        {
            string? binaryPath = FindRustFixtureBinary();
            if (binaryPath == null)
            {
                Assert.Inconclusive(
                    "Rust fixture binary not found. Build it first: cd src/DeltaLakeSharp.Server/v3 && cargo build");
            }

            _tempDir = Path.Combine(Path.GetTempPath(), $"adbc_v3_time_travel_{Guid.NewGuid():N}");
            string tablePath = Path.Combine(_tempDir, "test_table");
            CreateTestDeltaTable(binaryPath!, tablePath, fixtureType: "time-travel");

            var options = new Dictionary<string, string>
            {
                [DeltaAdbcConnectOptions.TableUriKey] = tablePath,
            };

            if (extraOptions != null)
            {
                foreach (KeyValuePair<string, string> option in extraOptions)
                {
                    options[option.Key] = option.Value;
                }
            }

            var driver = new DeltaAdbcDriver();
            var database = driver.Open(options);
            var connection = database.Connect(null);

            return new OpenedConnection(driver, database, connection);
        }

        private OpenedConnection OpenConnectionWithSchemaEvolution(IReadOnlyDictionary<string, string>? extraOptions = null)
        {
            _tempDir = Path.Combine(Path.GetTempPath(), $"adbc_v3_schema_evolution_{Guid.NewGuid():N}");
            string tablePath = Path.Combine(_tempDir, "test_table");
            CreateSchemaEvolutionDeltaTable(tablePath);

            return OpenConnectionForTablePath(tablePath, extraOptions);
        }

        private OpenedConnection OpenConnectionWithPartitionedData(IReadOnlyDictionary<string, string>? extraOptions = null)
        {
            string? binaryPath = FindRustFixtureBinary();
            if (binaryPath == null)
            {
                Assert.Inconclusive(
                    "Rust fixture binary not found. Build it first: cd src/DeltaLakeSharp.Server/v3 && cargo build");
            }

            _tempDir = Path.Combine(Path.GetTempPath(), $"adbc_v3_partitioned_{Guid.NewGuid():N}");
            string tablePath = Path.Combine(_tempDir, "test_table");
            CreateTestDeltaTable(binaryPath!, tablePath, fixtureType: "partitioned");

            return OpenConnectionForTablePath(tablePath, extraOptions);
        }

        private OpenedConnection OpenConnectionWithSimpleData(IReadOnlyDictionary<string, string>? extraOptions = null)
        {
            string? binaryPath = FindRustFixtureBinary();
            if (binaryPath == null)
            {
                Assert.Inconclusive(
                    "Rust fixture binary not found. Build it first: cd src/DeltaLakeSharp.Server/v3 && cargo build");
            }

            _tempDir = Path.Combine(Path.GetTempPath(), $"adbc_v3_simple_{Guid.NewGuid():N}");
            string tablePath = Path.Combine(_tempDir, "test_table");
            CreateTestDeltaTable(binaryPath!, tablePath, fixtureType: "basic");

            return OpenConnectionForTablePath(tablePath, extraOptions);
        }

        private OpenedConnection OpenConnectionWithMultiFileSimpleData(IReadOnlyDictionary<string, string>? extraOptions = null)
        {
            _tempDir = Path.Combine(Path.GetTempPath(), $"adbc_v3_multifile_{Guid.NewGuid():N}");
            string tablePath = Path.Combine(_tempDir, "test_table");
            CreateMultiFileSimpleDeltaTable(tablePath);

            return OpenConnectionForTablePath(tablePath, extraOptions);
        }

        private OpenedConnection OpenConnectionToExistingFixture(string fixtureName, IReadOnlyDictionary<string, string>? extraOptions = null)
        {
            string? fixturePath = FindRepoFixturePath(fixtureName);
            if (fixturePath == null)
            {
                Assert.Inconclusive($"Fixture '{fixtureName}' was not found in the repo.");
            }

            return OpenConnectionForTablePath(fixturePath!, extraOptions);
        }

        private static OpenedConnection OpenConnectionForTablePath(string tablePath, IReadOnlyDictionary<string, string>? extraOptions)
        {
            var options = new Dictionary<string, string>
            {
                [DeltaAdbcConnectOptions.TableUriKey] = tablePath,
            };

            if (extraOptions != null)
            {
                foreach (KeyValuePair<string, string> option in extraOptions)
                {
                    options[option.Key] = option.Value;
                }
            }

            var driver = new DeltaAdbcDriver();
            var database = driver.Open(options);
            var connection = database.Connect(null);

            return new OpenedConnection(driver, database, connection);
        }

        private static void CreateChangeDataDeltaTable(string tablePath)
        {
            var tableSchema = new DeltaLakeSharp.Client.Models.TableSchema(new List<DeltaLakeSharp.Client.Models.ColumnDefinition>
            {
                new DeltaLakeSharp.Client.Models.ColumnDefinition("id", "int32"),
                new DeltaLakeSharp.Client.Models.ColumnDefinition("name", "string"),
            });

            using var client = new DeltaLakeSharp.Client.DeltaTableServiceClient(DeltaLakeSharp.Client.ServiceMode.V3_Rust);

            DeltaLakeSharp.Client.Models.ExecuteResult createResult = client.CreateTableAsync(
                tablePath,
                tableSchema,
                configuration: new Dictionary<string, string>
                {
                    ["delta.enableChangeDataFeed"] = "true",
                }).GetAwaiter().GetResult();
            Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

            Apache.Arrow.RecordBatch initialBatch = ArrowConverter.FromRows(new[]
            {
                new object[] { 1, "a" },
                new object[] { 2, "b" },
            }, tableSchema);
            client.InsertAsync(tablePath, initialBatch.Schema, ArrowConverter.ToAsyncEnumerable(initialBatch), DeltaLakeSharp.Client.Models.SaveMode.Append)
                .GetAwaiter().GetResult();

            DeltaLakeSharp.Client.Models.ExecuteResult updateResult = client.UpdateAsync(
                "UPDATE delta_table SET name = 'b2' WHERE id = 2",
                tablePath,
                DeltaAdbcConnectOptions.LogicalTableName).GetAwaiter().GetResult();
            Assert.IsTrue(updateResult.Success, $"UpdateAsync failed: {updateResult.Message}");

            Apache.Arrow.RecordBatch appendBatch = ArrowConverter.FromRows(new[]
            {
                new object[] { 3, "c" },
            }, tableSchema);
            client.InsertAsync(tablePath, appendBatch.Schema, ArrowConverter.ToAsyncEnumerable(appendBatch), DeltaLakeSharp.Client.Models.SaveMode.Append)
                .GetAwaiter().GetResult();
        }

        private static void CreateMultiFileSimpleDeltaTable(string tablePath)
        {
            var tableSchema = new DeltaLakeSharp.Client.Models.TableSchema(new List<DeltaLakeSharp.Client.Models.ColumnDefinition>
            {
                new DeltaLakeSharp.Client.Models.ColumnDefinition("id", "int32"),
                new DeltaLakeSharp.Client.Models.ColumnDefinition("name", "string"),
            });

            using var client = new DeltaLakeSharp.Client.DeltaTableServiceClient(DeltaLakeSharp.Client.ServiceMode.V3_Rust);

            DeltaLakeSharp.Client.Models.ExecuteResult createResult = client.CreateTableAsync(tablePath, tableSchema)
                .GetAwaiter().GetResult();
            Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

            for (int i = 1; i <= 8; i++)
            {
                Apache.Arrow.RecordBatch batch = ArrowConverter.FromRows(new[]
                {
                    new object[] { i, $"row_{i}" },
                }, tableSchema);

                client.InsertAsync(
                    tablePath,
                    batch.Schema,
                    ArrowConverter.ToAsyncEnumerable(batch),
                    DeltaLakeSharp.Client.Models.SaveMode.Append)
                    .GetAwaiter().GetResult();
            }
        }

        private static void CreateSchemaEvolutionDeltaTable(string tablePath)
        {
            var initialSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });

            using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);

            ExecuteResult createResult = client.CreateTableAsync(tablePath, initialSchema).GetAwaiter().GetResult();
            Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

            RecordBatch initialBatch = ArrowConverter.FromRows(new[]
            {
                new object[] { 1, "alice" },
                new object[] { 2, "bob" },
            }, initialSchema);

            client.InsertAsync(
                tablePath,
                initialBatch.Schema,
                ArrowConverter.ToAsyncEnumerable(initialBatch),
                SaveMode.Append)
                .GetAwaiter().GetResult();

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

            client.InsertAsync(
                tablePath,
                replacementBatch.Schema,
                ArrowConverter.ToAsyncEnumerable(replacementBatch),
                SaveMode.Overwrite,
                schemaMode: WriteSchemaMode.Overwrite)
                .GetAwaiter().GetResult();
        }

        private static string? FindRustFixtureBinary()
        {
            string? dir = AppContext.BaseDirectory;
            while (dir != null)
            {
                string solutionFile = Path.Combine(dir, "DeltaLakeSharp.sln");
                if (File.Exists(solutionFile))
                {
                    string binaryPath = Path.Combine(
                        dir,
                        "src",
                        "DeltaLakeSharp.Server",
                        "v3",
                        "target",
                        "debug",
                        "delta-table-service-v3-fixture.exe");
                    return File.Exists(binaryPath) ? binaryPath : null;
                }

                dir = Path.GetDirectoryName(dir);
            }

            return null;
        }

        private static string? FindRepoFixturePath(string fixtureName)
        {
            string? dir = AppContext.BaseDirectory;
            while (dir != null)
            {
                string solutionFile = Path.Combine(dir, "DeltaLakeSharp.sln");
                if (File.Exists(solutionFile))
                {
                    string fixturePath = Path.Combine(dir, "tests", "DeltaLakeSharp.Tests", "data", fixtureName);
                    return Directory.Exists(fixturePath) ? fixturePath : null;
                }

                dir = Path.GetDirectoryName(dir);
            }

            return null;
        }

        private static void CreateTestDeltaTable(string binaryPath, string tablePath, string fixtureType = "basic")
        {
            Directory.CreateDirectory(Path.GetDirectoryName(tablePath)!);

            var startInfo = new ProcessStartInfo
            {
                FileName = binaryPath,
                Arguments = $"create \"{tablePath}\" --fixture-type {fixtureType}",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            using var process = Process.Start(startInfo)
                ?? throw new InvalidOperationException("Failed to start fixture creation process.");

            string stdout = process.StandardOutput.ReadToEnd();
            string stderr = process.StandardError.ReadToEnd();
            process.WaitForExit(30_000);

            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Fixture creation failed (exit code {process.ExitCode}).\nstdout: {stdout}\nstderr: {stderr}");
            }

            if (!stdout.Contains("TEST_FIXTURE_CREATED", StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Fixture creation did not print expected sentinel.\nstdout: {stdout}\nstderr: {stderr}");
            }
        }

        private sealed class OpenedConnection : IDisposable
        {
            private readonly AdbcDatabase _database;
            private readonly AdbcDriver _driver;

            public OpenedConnection(AdbcDriver driver, AdbcDatabase database, AdbcConnection connection)
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
