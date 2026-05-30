using System;
using System.Collections.Generic;
using System.Data;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using DeltaLakeSharp.Client;
using DeltaLakeSharp.Client.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DeltaLakeSharp.Tests
{
    /// <summary>
    /// Unit tests for <see cref="ArrowConverter"/>.
    /// These are pure in-memory tests — no Docker or Flight server required.
    /// </summary>
    [TestClass]
    public class ArrowConverterTests
    {
        // ================================================================== //
        //  Helpers
        // ================================================================== //

        /// <summary>
        /// Creates a simple RecordBatch with int32 "id" and string "name" columns.
        /// </summary>
        private static RecordBatch CreateSimpleBatch(int[] ids, string[] names)
        {
            var idBuilder = new Int32Array.Builder();
            foreach (int id in ids) idBuilder.Append(id);

            var nameBuilder = new StringArray.Builder();
            foreach (string name in names) nameBuilder.Append(name);

            var schema = new Schema(
                new List<Field>
                {
                    new Field("id", Int32Type.Default, nullable: false),
                    new Field("name", StringType.Default, nullable: true),
                },
                null);

            return new RecordBatch(
                schema,
                new IArrowArray[] { idBuilder.Build(), nameBuilder.Build() },
                ids.Length);
        }

        /// <summary>
        /// Creates a RecordBatch covering many Arrow types for comprehensive conversion testing.
        /// </summary>
        private static RecordBatch CreateMultiTypeBatch()
        {
            var fields = new List<Field>
            {
                new Field("col_string", StringType.Default, nullable: true),
                new Field("col_int32", Int32Type.Default, nullable: true),
                new Field("col_int64", Int64Type.Default, nullable: true),
                new Field("col_double", DoubleType.Default, nullable: true),
                new Field("col_float", FloatType.Default, nullable: true),
                new Field("col_bool", BooleanType.Default, nullable: true),
            };

            var stringBuilder = new StringArray.Builder();
            stringBuilder.Append("hello");
            stringBuilder.Append("world");
            stringBuilder.AppendNull();

            var int32Builder = new Int32Array.Builder();
            int32Builder.Append(1);
            int32Builder.Append(2);
            int32Builder.Append((int?)null);

            var int64Builder = new Int64Array.Builder();
            int64Builder.Append(100L);
            int64Builder.Append(200L);
            int64Builder.Append((long?)null);

            var doubleBuilder = new DoubleArray.Builder();
            doubleBuilder.Append(1.5);
            doubleBuilder.Append(2.5);
            doubleBuilder.Append((double?)null);

            var floatBuilder = new FloatArray.Builder();
            floatBuilder.Append(3.14f);
            floatBuilder.Append(2.72f);
            floatBuilder.Append((float?)null);

            var boolBuilder = new BooleanArray.Builder();
            boolBuilder.Append(true);
            boolBuilder.Append(false);
            boolBuilder.AppendNull();

            var schema = new Schema(fields, null);
            return new RecordBatch(
                schema,
                new IArrowArray[]
                {
                    stringBuilder.Build(),
                    int32Builder.Build(),
                    int64Builder.Build(),
                    doubleBuilder.Build(),
                    floatBuilder.Build(),
                    boolBuilder.Build(),
                },
                3);
        }

        // ================================================================== //
        //  ToDataTable tests
        // ================================================================== //

        [TestMethod]
        public void ToDataTable_EmptyList_ReturnsEmptyDataTable()
        {
            DataTable dt = ArrowConverter.ToDataTable(new List<RecordBatch>());
            Assert.AreEqual(0, dt.Columns.Count);
            Assert.AreEqual(0, dt.Rows.Count);
        }

        [TestMethod]
        public void ToDataTable_NullList_ReturnsEmptyDataTable()
        {
            DataTable dt = ArrowConverter.ToDataTable(null!);
            Assert.AreEqual(0, dt.Columns.Count);
            Assert.AreEqual(0, dt.Rows.Count);
        }

        [TestMethod]
        public void ToDataTable_SimpleBatch_CorrectColumnsAndRows()
        {
            RecordBatch batch = CreateSimpleBatch(
                new[] { 1, 2, 3 },
                new[] { "Alice", "Bob", "Charlie" });

            DataTable dt = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });

            Assert.AreEqual(2, dt.Columns.Count);
            Assert.AreEqual("id", dt.Columns[0].ColumnName);
            Assert.AreEqual("name", dt.Columns[1].ColumnName);
            Assert.AreEqual(typeof(int), dt.Columns[0].DataType);
            Assert.AreEqual(typeof(string), dt.Columns[1].DataType);

            Assert.AreEqual(3, dt.Rows.Count);
            Assert.AreEqual(1, dt.Rows[0]["id"]);
            Assert.AreEqual("Alice", dt.Rows[0]["name"]);
            Assert.AreEqual(2, dt.Rows[1]["id"]);
            Assert.AreEqual("Bob", dt.Rows[1]["name"]);
            Assert.AreEqual(3, dt.Rows[2]["id"]);
            Assert.AreEqual("Charlie", dt.Rows[2]["name"]);
        }

        [TestMethod]
        public void ToDataTable_MultipleBatches_CombinesRows()
        {
            RecordBatch batch1 = CreateSimpleBatch(new[] { 1 }, new[] { "A" });
            RecordBatch batch2 = CreateSimpleBatch(new[] { 2, 3 }, new[] { "B", "C" });

            DataTable dt = ArrowConverter.ToDataTable(new List<RecordBatch> { batch1, batch2 });

            Assert.AreEqual(3, dt.Rows.Count);
            Assert.AreEqual(1, dt.Rows[0]["id"]);
            Assert.AreEqual(2, dt.Rows[1]["id"]);
            Assert.AreEqual(3, dt.Rows[2]["id"]);
        }

        [TestMethod]
        public void ToDataTable_MultiTypesBatch_AllTypesConverted()
        {
            RecordBatch batch = CreateMultiTypeBatch();
            DataTable dt = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });

            Assert.AreEqual(6, dt.Columns.Count);
            Assert.AreEqual(3, dt.Rows.Count);

            // Row 0 — all values present
            Assert.AreEqual("hello", dt.Rows[0]["col_string"]);
            Assert.AreEqual(1, dt.Rows[0]["col_int32"]);
            Assert.AreEqual(100L, dt.Rows[0]["col_int64"]);
            Assert.AreEqual(1.5, dt.Rows[0]["col_double"]);
            Assert.AreEqual(3.14f, dt.Rows[0]["col_float"]);
            Assert.AreEqual(true, dt.Rows[0]["col_bool"]);

            // Row 2 — all nulls (should be DBNull.Value)
            Assert.AreEqual(DBNull.Value, dt.Rows[2]["col_string"]);
            Assert.AreEqual(DBNull.Value, dt.Rows[2]["col_int32"]);
            Assert.AreEqual(DBNull.Value, dt.Rows[2]["col_int64"]);
            Assert.AreEqual(DBNull.Value, dt.Rows[2]["col_double"]);
            Assert.AreEqual(DBNull.Value, dt.Rows[2]["col_float"]);
            Assert.AreEqual(DBNull.Value, dt.Rows[2]["col_bool"]);
        }

        // ================================================================== //
        //  ToDictionaryList tests
        // ================================================================== //

        [TestMethod]
        public void ToDictionaryList_EmptyList_ReturnsEmptyList()
        {
            List<Dictionary<string, object>> result =
                ArrowConverter.ToDictionaryList(new List<RecordBatch>());
            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void ToDictionaryList_SimpleBatch_CorrectKeysAndValues()
        {
            RecordBatch batch = CreateSimpleBatch(
                new[] { 10, 20 },
                new[] { "X", "Y" });

            List<Dictionary<string, object>> result =
                ArrowConverter.ToDictionaryList(new List<RecordBatch> { batch });

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(10, result[0]["id"]);
            Assert.AreEqual("X", result[0]["name"]);
            Assert.AreEqual(20, result[1]["id"]);
            Assert.AreEqual("Y", result[1]["name"]);
        }

        // ================================================================== //
        //  ToTableSchema tests
        // ================================================================== //

        [TestMethod]
        public void ToTableSchema_ConvertsArrowSchemaToTableSchema()
        {
            RecordBatch batch = CreateSimpleBatch(new[] { 1 }, new[] { "A" });
            TableSchema schema = ArrowConverter.ToTableSchema(batch.Schema);

            Assert.AreEqual(2, schema.Columns.Count);
            Assert.AreEqual("id", schema.Columns[0].Name);
            Assert.AreEqual("int32", schema.Columns[0].DataType);
            Assert.AreEqual("name", schema.Columns[1].Name);
            Assert.AreEqual("string", schema.Columns[1].DataType);
        }

        [TestMethod]
        public void ToTableSchema_MultiTypes_CorrectTypeNames()
        {
            RecordBatch batch = CreateMultiTypeBatch();
            TableSchema schema = ArrowConverter.ToTableSchema(batch.Schema);

            Assert.AreEqual(6, schema.Columns.Count);
            Assert.AreEqual("string", schema.Columns[0].DataType);
            Assert.AreEqual("int32", schema.Columns[1].DataType);
            Assert.AreEqual("int64", schema.Columns[2].DataType);
            Assert.AreEqual("double", schema.Columns[3].DataType);
            Assert.AreEqual("float", schema.Columns[4].DataType);
            Assert.AreEqual("boolean", schema.Columns[5].DataType);
        }

        // ================================================================== //
        //  FromDataTable tests
        // ================================================================== //

        [TestMethod]
        public void FromDataTable_SimpleTable_RoundTrips()
        {
            var dt = new DataTable();
            dt.Columns.Add("id", typeof(int));
            dt.Columns.Add("name", typeof(string));
            dt.Rows.Add(1, "Alpha");
            dt.Rows.Add(2, "Beta");

            RecordBatch batch = ArrowConverter.FromDataTable(dt);
            Assert.AreEqual(2, batch.Length);
            Assert.AreEqual(2, batch.ColumnCount);
            Assert.AreEqual("id", batch.Schema.FieldsList[0].Name);
            Assert.AreEqual("name", batch.Schema.FieldsList[1].Name);

            // Round-trip back to DataTable
            DataTable result = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });
            Assert.AreEqual(2, result.Rows.Count);
            Assert.AreEqual(1, result.Rows[0]["id"]);
            Assert.AreEqual("Alpha", result.Rows[0]["name"]);
            Assert.AreEqual(2, result.Rows[1]["id"]);
            Assert.AreEqual("Beta", result.Rows[1]["name"]);
        }

        [TestMethod]
        public void FromDataTable_WithNulls_PreservesNulls()
        {
            var dt = new DataTable();
            dt.Columns.Add("value", typeof(string));
            dt.Rows.Add("hello");
            dt.Rows.Add(DBNull.Value);

            RecordBatch batch = ArrowConverter.FromDataTable(dt);
            Assert.AreEqual(2, batch.Length);

            DataTable result = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });
            Assert.AreEqual("hello", result.Rows[0]["value"]);
            Assert.AreEqual(DBNull.Value, result.Rows[1]["value"]);
        }

        [TestMethod]
        public void FromDataTable_BooleanColumn_RoundTrips()
        {
            var dt = new DataTable();
            dt.Columns.Add("flag", typeof(bool));
            dt.Rows.Add(true);
            dt.Rows.Add(false);

            RecordBatch batch = ArrowConverter.FromDataTable(dt);
            DataTable result = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });

            Assert.AreEqual(true, result.Rows[0]["flag"]);
            Assert.AreEqual(false, result.Rows[1]["flag"]);
        }

        [TestMethod]
        public void FromDataTable_NumericColumns_RoundTrips()
        {
            var dt = new DataTable();
            dt.Columns.Add("int_col", typeof(int));
            dt.Columns.Add("long_col", typeof(long));
            dt.Columns.Add("double_col", typeof(double));
            dt.Columns.Add("float_col", typeof(float));
            dt.Rows.Add(42, 100L, 3.14, 2.72f);

            RecordBatch batch = ArrowConverter.FromDataTable(dt);
            DataTable result = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });

            Assert.AreEqual(42, result.Rows[0]["int_col"]);
            Assert.AreEqual(100L, result.Rows[0]["long_col"]);
            Assert.AreEqual(3.14, result.Rows[0]["double_col"]);
            Assert.AreEqual(2.72f, result.Rows[0]["float_col"]);
        }

        // ================================================================== //
        //  FromRows tests
        // ================================================================== //

        [TestMethod]
        public void FromRows_EmptyRows_ReturnsEmptyBatch()
        {
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
            });

            RecordBatch batch = ArrowConverter.FromRows(System.Array.Empty<object[]>(), schema);
            Assert.AreEqual(0, batch.Length);
        }

        [TestMethod]
        public void FromRows_NullRows_ReturnsEmptyBatch()
        {
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
            });

            RecordBatch batch = ArrowConverter.FromRows(null!, schema);
            Assert.AreEqual(0, batch.Length);
        }

        [TestMethod]
        public void FromRows_SimpleData_RoundTrips()
        {
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });

            var rows = new[]
            {
                new object[] { 1, "Alpha" },
                new object[] { 2, "Beta" },
            };

            RecordBatch batch = ArrowConverter.FromRows(rows, schema);
            Assert.AreEqual(2, batch.Length);

            List<Dictionary<string, object>> result =
                ArrowConverter.ToDictionaryList(new List<RecordBatch> { batch });

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(1, result[0]["id"]);
            Assert.AreEqual("Alpha", result[0]["name"]);
            Assert.AreEqual(2, result[1]["id"]);
            Assert.AreEqual("Beta", result[1]["name"]);
        }

        [TestMethod]
        public void FromRows_WithNullValues_HandlesGracefully()
        {
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("value", "string"),
                new ColumnDefinition("count", "int32"),
            });

            var rows = new[]
            {
                new object[] { "hello", 1 },
                new object[] { null!, null! },
            };

            RecordBatch batch = ArrowConverter.FromRows(rows, schema);
            Assert.AreEqual(2, batch.Length);

            DataTable dt = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });
            Assert.AreEqual("hello", dt.Rows[0]["value"]);
            Assert.AreEqual(1, dt.Rows[0]["count"]);
            Assert.AreEqual(DBNull.Value, dt.Rows[1]["value"]);
            Assert.AreEqual(DBNull.Value, dt.Rows[1]["count"]);
        }

        [TestMethod]
        public void FromRows_BooleanType_RoundTrips()
        {
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("flag", "boolean"),
            });

            var rows = new[]
            {
                new object[] { true },
                new object[] { false },
                new object[] { null! },
            };

            RecordBatch batch = ArrowConverter.FromRows(rows, schema);
            Assert.AreEqual(3, batch.Length);

            DataTable dt = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });
            Assert.AreEqual(true, dt.Rows[0]["flag"]);
            Assert.AreEqual(false, dt.Rows[1]["flag"]);
            Assert.AreEqual(DBNull.Value, dt.Rows[2]["flag"]);
        }

        [TestMethod]
        public void FromRows_MultipleNumericTypes_Correct()
        {
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("i32", "int32"),
                new ColumnDefinition("i64", "int64"),
                new ColumnDefinition("f64", "double"),
                new ColumnDefinition("f32", "float"),
            });

            var rows = new[]
            {
                new object[] { 42, 100L, 3.14, 2.72f },
            };

            RecordBatch batch = ArrowConverter.FromRows(rows, schema);
            Assert.AreEqual(1, batch.Length);

            DataTable dt = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });
            Assert.AreEqual(42, dt.Rows[0]["i32"]);
            Assert.AreEqual(100L, dt.Rows[0]["i64"]);
            Assert.AreEqual(3.14, dt.Rows[0]["f64"]);
            Assert.AreEqual(2.72f, dt.Rows[0]["f32"]);
        }

        // ================================================================== //
        //  FromCsv tests
        // ================================================================== //

        [TestMethod]
        public void FromCsv_EmptyString_ReturnsEmptyBatch()
        {
            RecordBatch batch = ArrowConverter.FromCsv("");
            Assert.AreEqual(0, batch.Length);
        }

        [TestMethod]
        public void FromCsv_NullString_ReturnsEmptyBatch()
        {
            RecordBatch batch = ArrowConverter.FromCsv(null!);
            Assert.AreEqual(0, batch.Length);
        }

        [TestMethod]
        public void FromCsv_HeaderOnly_ReturnsEmptyBatchWithSchema()
        {
            RecordBatch batch = ArrowConverter.FromCsv("id,name");
            Assert.AreEqual(0, batch.Length);
            Assert.AreEqual(2, batch.ColumnCount);
            Assert.AreEqual("id", batch.Schema.FieldsList[0].Name);
            Assert.AreEqual("name", batch.Schema.FieldsList[1].Name);
        }

        [TestMethod]
        public void FromCsv_SimpleData_ParsedCorrectly()
        {
            string csv = "id,name,city\n1,Alice,Seattle\n2,Bob,Portland";
            RecordBatch batch = ArrowConverter.FromCsv(csv);

            Assert.AreEqual(2, batch.Length);
            Assert.AreEqual(3, batch.ColumnCount);
            Assert.AreEqual("id", batch.Schema.FieldsList[0].Name);
            Assert.AreEqual("name", batch.Schema.FieldsList[1].Name);
            Assert.AreEqual("city", batch.Schema.FieldsList[2].Name);

            // All CSV columns are strings
            DataTable dt = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });
            Assert.AreEqual("1", dt.Rows[0]["id"]);
            Assert.AreEqual("Alice", dt.Rows[0]["name"]);
            Assert.AreEqual("Seattle", dt.Rows[0]["city"]);
            Assert.AreEqual("2", dt.Rows[1]["id"]);
            Assert.AreEqual("Bob", dt.Rows[1]["name"]);
            Assert.AreEqual("Portland", dt.Rows[1]["city"]);
        }

        [TestMethod]
        public void FromCsv_WhitespaceHandling_TrimmedCorrectly()
        {
            string csv = " id , name \n 1 , Alice \n 2 , Bob ";
            RecordBatch batch = ArrowConverter.FromCsv(csv);

            Assert.AreEqual(2, batch.Length);
            Assert.AreEqual("id", batch.Schema.FieldsList[0].Name);
            Assert.AreEqual("name", batch.Schema.FieldsList[1].Name);

            DataTable dt = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });
            Assert.AreEqual("1", dt.Rows[0]["id"]);
            Assert.AreEqual("Alice", dt.Rows[0]["name"]);
        }

        [TestMethod]
        public void FromCsv_RoundTrip_CsvToBatchToDataTable()
        {
            string csv = "product,quantity\nWidget,100\nGadget,200\nDoohickey,50";
            RecordBatch batch = ArrowConverter.FromCsv(csv);
            DataTable dt = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });

            Assert.AreEqual(3, dt.Rows.Count);
            Assert.AreEqual("Widget", dt.Rows[0]["product"]);
            Assert.AreEqual("100", dt.Rows[0]["quantity"]);
            Assert.AreEqual("Gadget", dt.Rows[1]["product"]);
            Assert.AreEqual("Doohickey", dt.Rows[2]["product"]);
        }

        // ================================================================== //
        //  Full round-trip: DataTable → RecordBatch → DataTable
        // ================================================================== //

        [TestMethod]
        public void FullRoundTrip_DataTable_PreservesAllData()
        {
            var original = new DataTable();
            original.Columns.Add("id", typeof(int));
            original.Columns.Add("name", typeof(string));
            original.Columns.Add("score", typeof(double));
            original.Columns.Add("active", typeof(bool));

            original.Rows.Add(1, "Alice", 95.5, true);
            original.Rows.Add(2, "Bob", 87.3, false);
            original.Rows.Add(3, "Charlie", 92.1, true);

            RecordBatch batch = ArrowConverter.FromDataTable(original);
            DataTable result = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });

            Assert.AreEqual(original.Rows.Count, result.Rows.Count);
            Assert.AreEqual(original.Columns.Count, result.Columns.Count);

            for (int i = 0; i < original.Rows.Count; i++)
            {
                Assert.AreEqual(original.Rows[i]["id"], result.Rows[i]["id"]);
                Assert.AreEqual(original.Rows[i]["name"], result.Rows[i]["name"]);
                Assert.AreEqual(original.Rows[i]["score"], result.Rows[i]["score"]);
                Assert.AreEqual(original.Rows[i]["active"], result.Rows[i]["active"]);
            }
        }

        // ================================================================== //
        //  Full round-trip: object[][] → RecordBatch → DictionaryList
        // ================================================================== //

        [TestMethod]
        public void FullRoundTrip_Rows_ToDictionaryList()
        {
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("name", "string"),
                new ColumnDefinition("age", "int32"),
            });

            var rows = new[]
            {
                new object[] { "Alice", 30 },
                new object[] { "Bob", 25 },
            };

            RecordBatch batch = ArrowConverter.FromRows(rows, schema);
            List<Dictionary<string, object>> result =
                ArrowConverter.ToDictionaryList(new List<RecordBatch> { batch });

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual("Alice", result[0]["name"]);
            Assert.AreEqual(30, result[0]["age"]);
            Assert.AreEqual("Bob", result[1]["name"]);
            Assert.AreEqual(25, result[1]["age"]);
        }

        [TestMethod]
        public void ToDictionaryList_DictionaryEncodedStringColumn_DecodesValues()
        {
            IArrowArray dictionary = new StringArray.Builder()
                .AppendRange(new[] { "us", "eu", "apac" })
                .Build();
            IArrowArray indices = new Int32Array.Builder()
                .AppendRange(new[] { 0, 1, 2 })
                .Build();
            var dictionaryType = new DictionaryType(Int32Type.Default, StringType.Default, ordered: false);
            IArrowArray encoded = new DictionaryArray(dictionaryType, indices, dictionary);

            RecordBatch batch = new RecordBatch.Builder()
                .Append("region", nullable: true, encoded)
                .Build();

            List<Dictionary<string, object>> result =
                ArrowConverter.ToDictionaryList(new List<RecordBatch> { batch });

            Assert.AreEqual(3, result.Count);
            Assert.AreEqual("us", result[0]["region"]);
            Assert.AreEqual("eu", result[1]["region"]);
            Assert.AreEqual("apac", result[2]["region"]);
        }

        // ================================================================== //
        //  Timestamp type mapping tests
        // ================================================================== //

        [TestMethod]
        public void FromDataTable_DateTimeColumn_ProducesTimestampNtz()
        {
            var dt = new DataTable();
            dt.Columns.Add("ts", typeof(DateTime));
            var dateVal = new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Unspecified);
            dt.Rows.Add(dateVal);

            RecordBatch batch = ArrowConverter.FromDataTable(dt);

            // Schema should be timestamp with no timezone (timestamp_ntz)
            var tsType = (TimestampType)batch.Schema.FieldsList[0].DataType;
            Assert.AreEqual(TimeUnit.Microsecond, tsType.Unit);
            Assert.IsTrue(string.IsNullOrEmpty(tsType.Timezone),
                "DateTime should produce a tz-naive TimestampType");

            // The array should be a TimestampArray, not a StringArray
            Assert.IsInstanceOfType(batch.Column(0), typeof(TimestampArray));
        }

        [TestMethod]
        public void FromDataTable_DateTimeOffsetColumn_ProducesTimestampWithUtc()
        {
            var dt = new DataTable();
            dt.Columns.Add("ts", typeof(DateTimeOffset));
            var dtoVal = new DateTimeOffset(2025, 6, 15, 10, 30, 0, TimeSpan.Zero);
            dt.Rows.Add(dtoVal);

            RecordBatch batch = ArrowConverter.FromDataTable(dt);

            // Schema should be timestamp with UTC timezone
            var tsType = (TimestampType)batch.Schema.FieldsList[0].DataType;
            Assert.AreEqual(TimeUnit.Microsecond, tsType.Unit);
            Assert.AreEqual("UTC", tsType.Timezone);

            Assert.IsInstanceOfType(batch.Column(0), typeof(TimestampArray));
        }

        [TestMethod]
        public void FromRows_TimestampType_ProducesTimestampWithUtc()
        {
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("ts", "timestamp"),
            });

            var dateVal = new DateTimeOffset(2025, 1, 1, 12, 0, 0, TimeSpan.Zero);
            var rows = new[] { new object[] { dateVal } };

            RecordBatch batch = ArrowConverter.FromRows(rows, schema);

            var tsType = (TimestampType)batch.Schema.FieldsList[0].DataType;
            Assert.AreEqual("UTC", tsType.Timezone);
            Assert.IsInstanceOfType(batch.Column(0), typeof(TimestampArray));
        }

        [TestMethod]
        public void FromRows_TimestampNtzType_ProducesTimestampWithoutTz()
        {
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("ts", "timestamp_ntz"),
            });

            var dateVal = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Unspecified);
            var rows = new[] { new object[] { dateVal } };

            RecordBatch batch = ArrowConverter.FromRows(rows, schema);

            var tsType = (TimestampType)batch.Schema.FieldsList[0].DataType;
            Assert.IsTrue(string.IsNullOrEmpty(tsType.Timezone),
                "timestamp_ntz should produce a tz-naive TimestampType");
            Assert.IsInstanceOfType(batch.Column(0), typeof(TimestampArray));
        }

        // ================================================================== //
        //  Timestamp round-trip tests
        // ================================================================== //

        [TestMethod]
        public void RoundTrip_DateTime_ThroughTimestampNtz()
        {
            // DateTime → FromDataTable (timestamp_ntz) → ToDataTable → DateTime
            var dt = new DataTable();
            dt.Columns.Add("ts", typeof(DateTime));

            var date1 = new DateTime(2025, 6, 15, 10, 30, 45, DateTimeKind.Unspecified);
            var date2 = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
            dt.Rows.Add(date1);
            dt.Rows.Add(date2);

            RecordBatch batch = ArrowConverter.FromDataTable(dt);
            DataTable result = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });

            // Column type should be DateTime (tz-naive → DateTime)
            Assert.AreEqual(typeof(DateTime), result.Columns[0].DataType);
            // Values should round-trip (microsecond precision is fine for these values)
            Assert.AreEqual(date1, result.Rows[0]["ts"]);
            Assert.AreEqual(date2, result.Rows[1]["ts"]);
            Assert.AreEqual(DateTimeKind.Unspecified, ((DateTime)result.Rows[0]["ts"]).Kind,
                "tz-naive round-tripped DateTime should have Kind=Unspecified.");
            Assert.AreEqual(DateTimeKind.Unspecified, ((DateTime)result.Rows[1]["ts"]).Kind,
                "tz-naive round-tripped DateTime should have Kind=Unspecified.");
        }

        [TestMethod]
        public void RoundTrip_DateTimeOffset_ThroughTimestampUtc()
        {
            // DateTimeOffset → FromDataTable (timestamp UTC) → ToDataTable → DateTimeOffset
            var dt = new DataTable();
            dt.Columns.Add("ts", typeof(DateTimeOffset));

            var dto1 = new DateTimeOffset(2025, 6, 15, 10, 30, 45, TimeSpan.Zero);
            var dto2 = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);
            dt.Rows.Add(dto1);
            dt.Rows.Add(dto2);

            RecordBatch batch = ArrowConverter.FromDataTable(dt);
            DataTable result = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });

            // Column type should be DateTimeOffset (tz-aware → DateTimeOffset)
            Assert.AreEqual(typeof(DateTimeOffset), result.Columns[0].DataType);
            Assert.AreEqual(dto1, result.Rows[0]["ts"]);
            Assert.AreEqual(dto2, result.Rows[1]["ts"]);
        }

        [TestMethod]
        public void RoundTrip_FromRows_TimestampNtz_ReturnsDateTime()
        {
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("ts", "timestamp_ntz"),
            });

            var dateVal = new DateTime(2025, 3, 20, 8, 15, 30, DateTimeKind.Unspecified);
            var rows = new[] { new object[] { dateVal } };

            RecordBatch batch = ArrowConverter.FromRows(rows, schema);
            DataTable result = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });

            Assert.AreEqual(typeof(DateTime), result.Columns[0].DataType);
            Assert.AreEqual(dateVal, result.Rows[0]["ts"]);
            Assert.AreEqual(DateTimeKind.Unspecified, ((DateTime)result.Rows[0]["ts"]).Kind,
                "tz-naive round-tripped DateTime should have Kind=Unspecified.");
        }

        [TestMethod]
        public void RoundTrip_FromRows_Timestamp_ReturnsDateTimeOffset()
        {
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("ts", "timestamp"),
            });

            var dtoVal = new DateTimeOffset(2025, 3, 20, 8, 15, 30, TimeSpan.Zero);
            var rows = new[] { new object[] { dtoVal } };

            RecordBatch batch = ArrowConverter.FromRows(rows, schema);
            DataTable result = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });

            Assert.AreEqual(typeof(DateTimeOffset), result.Columns[0].DataType);
            Assert.AreEqual(dtoVal, result.Rows[0]["ts"]);
        }

        // ================================================================== //
        //  Timestamp null handling tests
        // ================================================================== //

        [TestMethod]
        public void FromDataTable_TimestampNullValues_PreservesNulls()
        {
            var dt = new DataTable();
            dt.Columns.Add("ts", typeof(DateTime));
            dt.Rows.Add(new DateTime(2025, 1, 1));
            dt.Rows.Add(DBNull.Value);

            RecordBatch batch = ArrowConverter.FromDataTable(dt);
            Assert.AreEqual(2, batch.Length);

            DataTable result = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });
            Assert.AreNotEqual(DBNull.Value, result.Rows[0]["ts"]);
            Assert.AreEqual(DBNull.Value, result.Rows[1]["ts"]);
        }

        [TestMethod]
        public void FromRows_TimestampNullValues_PreservesNulls()
        {
            var schema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("ts", "timestamp"),
            });

            var rows = new[]
            {
                new object[] { new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero) },
                new object[] { null! },
            };

            RecordBatch batch = ArrowConverter.FromRows(rows, schema);
            Assert.AreEqual(2, batch.Length);

            DataTable result = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });
            Assert.AreNotEqual(DBNull.Value, result.Rows[0]["ts"]);
            Assert.AreEqual(DBNull.Value, result.Rows[1]["ts"]);
        }

        // ================================================================== //
        //  Timestamp schema string tests
        // ================================================================== //

        [TestMethod]
        public void ToTableSchema_TimestampWithTz_ReturnsTimestampString()
        {
            var fields = new List<Field>
            {
                new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: true),
            };
            var schema = new Schema(fields, null);

            TableSchema tableSchema = ArrowConverter.ToTableSchema(schema);

            Assert.AreEqual("timestamp", tableSchema.Columns[0].DataType);
        }

        [TestMethod]
        public void ToTableSchema_TimestampNtz_ReturnsTimestampNtzString()
        {
            var fields = new List<Field>
            {
                new Field("ts", new TimestampType(TimeUnit.Microsecond, (string)null), nullable: true),
            };
            var schema = new Schema(fields, null);

            TableSchema tableSchema = ArrowConverter.ToTableSchema(schema);

            Assert.AreEqual("timestamp_ntz", tableSchema.Columns[0].DataType);
        }

        // ================================================================== //
        //  Timestamp read-path tests (direct Arrow array construction)
        // ================================================================== //

        [TestMethod]
        public void ToDataTable_TzAwareTimestamp_ReturnsDateTimeOffset()
        {
            // Directly construct a tz-aware TimestampArray and verify read path
            var tsType = new TimestampType(TimeUnit.Microsecond, "UTC");
            var builder = new TimestampArray.Builder(tsType);
            var dto = new DateTimeOffset(2025, 7, 4, 14, 30, 0, TimeSpan.Zero);
            builder.Append(dto);

            var schema = new Schema(
                new List<Field> { new Field("ts", tsType, nullable: true) }, null);
            var batch = new RecordBatch(schema,
                new IArrowArray[] { builder.Build() }, 1);

            DataTable result = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });

            Assert.AreEqual(typeof(DateTimeOffset), result.Columns[0].DataType);
            Assert.IsInstanceOfType(result.Rows[0]["ts"], typeof(DateTimeOffset));
            Assert.AreEqual(dto, (DateTimeOffset)result.Rows[0]["ts"]);
        }

        [TestMethod]
        public void ToDataTable_TzNaiveTimestamp_ReturnsDateTime()
        {
            // Directly construct a tz-naive TimestampArray and verify read path
            var tsType = new TimestampType(TimeUnit.Microsecond, (string)null);
            var builder = new TimestampArray.Builder(tsType);
            var dateVal = new DateTime(2025, 7, 4, 14, 30, 0, DateTimeKind.Unspecified);
            builder.Append(new DateTimeOffset(dateVal, TimeSpan.Zero));

            var schema = new Schema(
                new List<Field> { new Field("ts", tsType, nullable: true) }, null);
            var batch = new RecordBatch(schema,
                new IArrowArray[] { builder.Build() }, 1);

            DataTable result = ArrowConverter.ToDataTable(new List<RecordBatch> { batch });

            Assert.AreEqual(typeof(DateTime), result.Columns[0].DataType);
            Assert.IsInstanceOfType(result.Rows[0]["ts"], typeof(DateTime));
            Assert.AreEqual(dateVal, (DateTime)result.Rows[0]["ts"]);
            Assert.AreEqual(DateTimeKind.Unspecified, ((DateTime)result.Rows[0]["ts"]).Kind,
                "tz-naive timestamp should produce DateTime with Kind=Unspecified.");
        }

        [TestMethod]
        public void ToDictionaryList_TzAwareTimestamp_ReturnsDateTimeOffset()
        {
            var tsType = new TimestampType(TimeUnit.Microsecond, "UTC");
            var builder = new TimestampArray.Builder(tsType);
            var dto = new DateTimeOffset(2025, 7, 4, 14, 30, 0, TimeSpan.Zero);
            builder.Append(dto);

            var schema = new Schema(
                new List<Field> { new Field("ts", tsType, nullable: true) }, null);
            var batch = new RecordBatch(schema,
                new IArrowArray[] { builder.Build() }, 1);

            var result = ArrowConverter.ToDictionaryList(new List<RecordBatch> { batch });

            Assert.AreEqual(1, result.Count);
            Assert.IsInstanceOfType(result[0]["ts"], typeof(DateTimeOffset));
            Assert.AreEqual(dto, (DateTimeOffset)result[0]["ts"]);
        }

        [TestMethod]
        public void ToDictionaryList_TzNaiveTimestamp_ReturnsDateTime()
        {
            var tsType = new TimestampType(TimeUnit.Microsecond, (string)null);
            var builder = new TimestampArray.Builder(tsType);
            var dateVal = new DateTime(2025, 7, 4, 14, 30, 0, DateTimeKind.Unspecified);
            builder.Append(new DateTimeOffset(dateVal, TimeSpan.Zero));

            var schema = new Schema(
                new List<Field> { new Field("ts", tsType, nullable: true) }, null);
            var batch = new RecordBatch(schema,
                new IArrowArray[] { builder.Build() }, 1);

            var result = ArrowConverter.ToDictionaryList(new List<RecordBatch> { batch });

            Assert.AreEqual(1, result.Count);
            Assert.IsInstanceOfType(result[0]["ts"], typeof(DateTime));
            Assert.AreEqual(dateVal, (DateTime)result[0]["ts"]);
            Assert.AreEqual(DateTimeKind.Unspecified, ((DateTime)result[0]["ts"]).Kind,
                "tz-naive timestamp should produce DateTime with Kind=Unspecified.");
        }
    }
}
