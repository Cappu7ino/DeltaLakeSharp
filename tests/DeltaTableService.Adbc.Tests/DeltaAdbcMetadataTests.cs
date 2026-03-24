using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.DI.DeltaTableService.Adbc.Tests
{
    [TestClass]
    public class DeltaAdbcMetadataTests
    {
        [TestMethod]
        public void GetTableTypes_ReturnsSingleTableType()
        {
            using var stream = DeltaAdbcMetadataBuilder.CreateTableTypesStream();
            RecordBatch? batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();

            Assert.IsNotNull(batch);
            Assert.AreEqual(1, batch.Length);
            Assert.AreEqual("TABLE", ((StringArray)batch.Column(0)).GetString(0));
        }

        [TestMethod]
        public void GetInfo_EmptyCodes_ReturnsDefaultDriverMetadata()
        {
            using var stream = DeltaAdbcMetadataBuilder.CreateGetInfoStream(System.Array.Empty<AdbcInfoCode>());
            RecordBatch? batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();

            Assert.IsNotNull(batch);
            Assert.AreEqual(8, batch.Length);
            Assert.AreEqual("Delta Lake", ReadInfoValue(batch, AdbcInfoCode.VendorName));
            Assert.AreEqual(true, ReadInfoValue(batch, AdbcInfoCode.VendorSql));
            Assert.AreEqual("Microsoft.DI.DeltaTableService.Adbc", ReadInfoValue(batch, AdbcInfoCode.DriverName));
        }

        [TestMethod]
        public void GetObjects_AllDepth_ReturnsLogicalTableAndColumns()
        {
            using var stream = DeltaAdbcMetadataBuilder.CreateGetObjectsStream(
                CreateSampleSchema(),
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
            var nullableFlags = (StringArray)columnValues.Fields[13];

            Assert.AreEqual("id", columnNames.GetString(0));
            Assert.AreEqual(1, ordinalPositions.GetValue(0));
            Assert.AreEqual("NO", nullableFlags.GetString(0));

            Assert.AreEqual("name", columnNames.GetString(1));
            Assert.AreEqual(2, ordinalPositions.GetValue(1));
            Assert.AreEqual("YES", nullableFlags.GetString(1));
        }

        [TestMethod]
        public void GetObjects_TablePatternMismatch_ReturnsEmptyBatch()
        {
            using var stream = DeltaAdbcMetadataBuilder.CreateGetObjectsStream(
                CreateSampleSchema(),
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

        private static Schema CreateSampleSchema()
        {
            return new Schema.Builder()
                .Field(f => f.Name("id").DataType(Int32Type.Default).Nullable(false))
                .Field(f => f.Name("name").DataType(StringType.Default).Nullable(true))
                .Build();
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
    }
}
