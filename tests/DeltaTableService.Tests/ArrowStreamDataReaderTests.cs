// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Microsoft.DI.DeltaTableService.Client.Internal;
using Microsoft.DI.DeltaTableService.Client.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.DI.DeltaTableService.Tests
{
    [TestClass]
    public class ArrowStreamDataReaderTests
    {
        [TestMethod]
        public void GetValue_BeforeRead_ThrowsInvalidOperationException()
        {
            using var reader = CreateReader(
                new Schema.Builder()
                    .Field(new Field("id", Int32Type.Default, nullable: false))
                    .Build());

            Assert.ThrowsException<InvalidOperationException>(() => reader.GetValue(0));
        }

        [TestMethod]
        public void Read_AdvancesAcrossBatches_AndDisposeClosesStream()
        {
            Schema schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Build();

            RecordBatch firstBatch = new RecordBatch.Builder()
                .Append("id", nullable: false, new Int32Array.Builder().Append(1).Build())
                .Build();
            RecordBatch secondBatch = new RecordBatch.Builder()
                .Append("id", nullable: false, new Int32Array.Builder().Append(2).Build())
                .Build();

            using var stream = new TrackingArrowArrayStream(schema, firstBatch, secondBatch);
            using var reader = new ArrowStreamDataReader(new ArrowStreamResult(schema, stream));

            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetInt32(0));

            Assert.IsTrue(reader.Read());
            Assert.AreEqual(2, reader.GetInt32(0));

            Assert.IsFalse(reader.Read());

            reader.Dispose();
            Assert.AreEqual(1, stream.DisposeCount);
        }

        [TestMethod]
        public void Dispose_ClosesReaderAndDisposesStream()
        {
            Schema schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Build();

            RecordBatch batch = new RecordBatch.Builder()
                .Append("id", nullable: false, new Int32Array.Builder().Append(1).Build())
                .Build();

            var stream = new TrackingArrowArrayStream(schema, batch);
            var reader = new ArrowStreamDataReader(new ArrowStreamResult(schema, stream));

            Assert.IsTrue(reader.Read());
            reader.Dispose();

            Assert.AreEqual(1, stream.DisposeCount);
            Assert.IsTrue(reader.IsClosed);
        }

        [TestMethod]
        public void GetFieldType_OverflowDecimalAsString_ReturnsStringForPrecision38()
        {
            Schema schema = new Schema.Builder()
                .Field(new Field("amount", new Decimal128Type(38, 2), nullable: true))
                .Build();

            using var reader = CreateReader(
                schema,
                new DeltaDataReaderOptions
                {
                    DecimalBehavior = DeltaDataReaderDecimalBehavior.OverflowDecimalAsString,
                });

            Assert.AreEqual(typeof(string), reader.GetFieldType(0));
            Assert.AreEqual("decimal(38,2)", reader.GetDataTypeName(0));

            DbColumn column = reader.GetColumnSchema()[0];
            Assert.AreEqual(typeof(string), column.DataType);
            Assert.AreEqual(38, column.NumericPrecision);
            Assert.AreEqual(2, column.NumericScale);
        }

        [TestMethod]
        public void Read_SkipsEmptyBatch()
        {
            Schema schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Build();

            RecordBatch emptyBatch = new RecordBatch.Builder()
                .Append("id", nullable: false, new Int32Array.Builder().Build())
                .Build();
            RecordBatch dataBatch = new RecordBatch.Builder()
                .Append("id", nullable: false, new Int32Array.Builder().Append(7).Build())
                .Build();

            using var reader = CreateReader(schema, null, emptyBatch, dataBatch);

            Assert.IsTrue(reader.Read());
            Assert.AreEqual(7, reader.GetInt32(0));
            Assert.IsFalse(reader.Read());
        }

        [TestMethod]
        public void GetSchemaTable_AndColumnSchema_ReturnExpectedMetadata()
        {
            Schema schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Field(new Field("name", StringType.Default, nullable: true))
                .Field(new Field("amount", new Decimal128Type(18, 2), nullable: true))
                .Build();

            using var reader = CreateReader(schema);

            DataTable schemaTable = reader.GetSchemaTable();
            Assert.AreEqual(3, schemaTable.Rows.Count);
            Assert.AreEqual("id", schemaTable.Rows[0]["ColumnName"]);
            Assert.AreEqual(typeof(int), schemaTable.Rows[0]["DataType"]);
            Assert.AreEqual(DBNull.Value, schemaTable.Rows[0]["NumericPrecision"]);
            Assert.AreEqual("amount", schemaTable.Rows[2]["ColumnName"]);
            Assert.AreEqual(typeof(SqlDecimal), schemaTable.Rows[2]["DataType"]);
            Assert.AreEqual(18, schemaTable.Rows[2]["NumericPrecision"]);
            Assert.AreEqual(2, schemaTable.Rows[2]["NumericScale"]);

            var columns = reader.GetColumnSchema();
            Assert.AreEqual(3, columns.Count);
            Assert.AreEqual("name", columns[1].ColumnName);
            Assert.AreEqual(typeof(string), columns[1].DataType);
            Assert.AreEqual(true, columns[1].AllowDBNull);
            Assert.AreEqual("decimal(18,2)", columns[2].DataTypeName);
            Assert.AreEqual(18, columns[2].NumericPrecision);
            Assert.AreEqual(2, columns[2].NumericScale);
        }

        [TestMethod]
        public void GetValues_GetOrdinal_GetBytes_And_GetChars_WorkAsExpected()
        {
            Schema schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Field(new Field("name", StringType.Default, nullable: false))
                .Field(new Field("payload", BinaryType.Default, nullable: false))
                .Build();

            RecordBatch batch = new RecordBatch.Builder()
                .Append("id", nullable: false, new Int32Array.Builder().Append(42).Build())
                .Append("name", nullable: false, new StringArray.Builder().Append("delta").Build())
                .Append("payload", nullable: false, new BinaryArray.Builder().Append(new byte[] { 10, 20, 30, 40 }).Build())
                .Build();

            using var reader = CreateReader(schema, null, batch);

            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetOrdinal("name"));
            Assert.ThrowsException<IndexOutOfRangeException>(() => reader.GetOrdinal("Name"));

            object[] values = new object[3];
            Assert.AreEqual(3, reader.GetValues(values));
            Assert.AreEqual(42, values[0]);
            Assert.AreEqual("delta", values[1]);
            CollectionAssert.AreEqual(new byte[] { 10, 20, 30, 40 }, (byte[])values[2]);

            Assert.AreEqual(4L, reader.GetBytes(2, 0, null, 0, 0));
            var payloadBuffer = new byte[2];
            Assert.AreEqual(2L, reader.GetBytes(2, 1, payloadBuffer, 0, 2));
            CollectionAssert.AreEqual(new byte[] { 20, 30 }, payloadBuffer);

            Assert.AreEqual(5L, reader.GetChars(1, 0, null, 0, 0));
            var charBuffer = new char[3];
            Assert.AreEqual(3L, reader.GetChars(1, 1, charBuffer, 0, 3));
            CollectionAssert.AreEqual(new[] { 'e', 'l', 't' }, charBuffer);
        }

        [TestMethod]
        public void NullValue_GetString_ThrowsAndIsDBNullReturnsTrue()
        {
            Schema schema = new Schema.Builder()
                .Field(new Field("name", StringType.Default, nullable: true))
                .Build();

            RecordBatch batch = new RecordBatch.Builder()
                .Append("name", nullable: true, new StringArray.Builder().AppendNull().Build())
                .Build();

            using var reader = CreateReader(schema, null, batch);

            Assert.IsTrue(reader.Read());
            Assert.IsTrue(reader.IsDBNull(0));
            Assert.AreEqual(DBNull.Value, reader.GetValue(0));
            Assert.ThrowsException<InvalidCastException>(() => reader.GetString(0));
        }

        [TestMethod]
        public async Task ReadAsync_WithCanceledToken_ThrowsOperationCanceledException()
        {
            Schema schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Build();

            using var reader = CreateReader(schema);
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsExceptionAsync<OperationCanceledException>(() => reader.ReadAsync(cts.Token));
        }

        [TestMethod]
        public void GetEnumerator_ThrowsNotSupportedException()
        {
            Schema schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Build();

            using var reader = CreateReader(schema);

            Assert.ThrowsException<NotSupportedException>(() => reader.GetEnumerator());
        }

        [TestMethod]
        public void TimestampWithTimezone_GetValueAndGetDateTime_ReturnExpectedTypes()
        {
            TimestampType timestampType = new TimestampType(TimeUnit.Microsecond, "UTC");
            DateTimeOffset dto = new DateTimeOffset(2025, 7, 4, 14, 30, 0, TimeSpan.Zero);

            Schema schema = new Schema.Builder()
                .Field(new Field("ts", timestampType, nullable: false))
                .Build();

            RecordBatch batch = new RecordBatch.Builder()
                .Append("ts", nullable: false, new TimestampArray.Builder(timestampType).Append(dto).Build())
                .Build();

            using var reader = CreateReader(schema, null, batch);

            Assert.IsTrue(reader.Read());
            Assert.AreEqual(typeof(DateTimeOffset), reader.GetFieldType(0));
            Assert.AreEqual(dto, reader.GetValue(0));
            Assert.AreEqual(dto.UtcDateTime, reader.GetDateTime(0));
        }

        [TestMethod]
        public void TimestampWithoutTimezone_GetValueAndGetDateTime_ReturnDateTime()
        {
            TimestampType timestampType = new TimestampType(TimeUnit.Microsecond, (string)null);
            DateTime value = new DateTime(2025, 7, 4, 14, 30, 0, DateTimeKind.Unspecified);

            Schema schema = new Schema.Builder()
                .Field(new Field("ts", timestampType, nullable: false))
                .Build();

            RecordBatch batch = new RecordBatch.Builder()
                .Append("ts", nullable: false, new TimestampArray.Builder(timestampType).Append(new DateTimeOffset(value, TimeSpan.Zero)).Build())
                .Build();

            using var reader = CreateReader(schema, null, batch);

            Assert.IsTrue(reader.Read());
            Assert.AreEqual(typeof(DateTime), reader.GetFieldType(0));
            Assert.AreEqual(value, reader.GetValue(0));
            Assert.AreEqual(value, reader.GetDateTime(0));
        }

        [TestMethod]
        public void GetGuid_ParsesStringRepresentation()
        {
            Guid expected = Guid.NewGuid();
            Schema schema = new Schema.Builder()
                .Field(new Field("id", StringType.Default, nullable: false))
                .Build();

            RecordBatch batch = new RecordBatch.Builder()
                .Append("id", nullable: false, new StringArray.Builder().Append(expected.ToString()).Build())
                .Build();

            using var reader = CreateReader(schema, null, batch);

            Assert.IsTrue(reader.Read());
            Assert.AreEqual(expected, reader.GetGuid(0));
        }

        [TestMethod]
        public void DecimalBehavior_UseSqlDecimal_ReturnsSqlDecimalAndTypedConversions()
        {
            Schema schema = new Schema.Builder()
                .Field(new Field("amount", new Decimal128Type(18, 2), nullable: false))
                .Build();

            RecordBatch batch = new RecordBatch.Builder()
                .Append("amount", nullable: false, new Decimal128Array.Builder(new Decimal128Type(18, 2)).Append(12.34m).Build())
                .Build();

            using var reader = CreateReader(
                schema,
                new DeltaDataReaderOptions { DecimalBehavior = DeltaDataReaderDecimalBehavior.UseSqlDecimal },
                batch);

            Assert.IsTrue(reader.Read());
            Assert.AreEqual(typeof(SqlDecimal), reader.GetFieldType(0));
            Assert.AreEqual(new SqlDecimal(12.34m), reader.GetValue(0));
            Assert.AreEqual(new SqlDecimal(12.34m), reader.GetSqlDecimal(0));
            Assert.AreEqual(12.34m, reader.GetDecimal(0));
        }

        [TestMethod]
        public void DecimalBehavior_UseDecimal_ReturnsDecimalAndGetSqlDecimalConverts()
        {
            Schema schema = new Schema.Builder()
                .Field(new Field("amount", new Decimal128Type(18, 2), nullable: false))
                .Build();

            RecordBatch batch = new RecordBatch.Builder()
                .Append("amount", nullable: false, new Decimal128Array.Builder(new Decimal128Type(18, 2)).Append(12.34m).Build())
                .Build();

            using var reader = CreateReader(
                schema,
                new DeltaDataReaderOptions { DecimalBehavior = DeltaDataReaderDecimalBehavior.UseDecimal },
                batch);

            Assert.IsTrue(reader.Read());
            Assert.AreEqual(typeof(decimal), reader.GetFieldType(0));
            Assert.AreEqual(12.34m, reader.GetValue(0));
            Assert.AreEqual(12.34m, reader.GetDecimal(0));
            Assert.AreEqual(new SqlDecimal(12.34m), reader.GetSqlDecimal(0));
        }

        [TestMethod]
        public void DecimalBehavior_OverflowDecimalAsString_ReturnsStringValueForPrecision38()
        {
            SqlDecimal value = SqlDecimal.Parse("123456789012345678901234567890123456.78");
            Schema schema = new Schema.Builder()
                .Field(new Field("amount", new Decimal128Type(38, 2), nullable: false))
                .Build();

            RecordBatch batch = new RecordBatch.Builder()
                .Append("amount", nullable: false, new Decimal128Array.Builder(new Decimal128Type(38, 2)).Append(value).Build())
                .Build();

            using var reader = CreateReader(
                schema,
                new DeltaDataReaderOptions { DecimalBehavior = DeltaDataReaderDecimalBehavior.OverflowDecimalAsString },
                batch);

            Assert.IsTrue(reader.Read());
            Assert.AreEqual(typeof(string), reader.GetFieldType(0));
            Assert.AreEqual("123456789012345678901234567890123456.78", reader.GetValue(0));
            Assert.AreEqual("123456789012345678901234567890123456.78", reader.GetString(0));
        }

        [TestMethod]
        public void DecimalBehavior_ThrowOnOverflow_ThrowsOverflowExceptionForPrecision38()
        {
            SqlDecimal value = SqlDecimal.Parse("123456789012345678901234567890123456.78");
            Schema schema = new Schema.Builder()
                .Field(new Field("amount", new Decimal128Type(38, 2), nullable: false))
                .Build();

            RecordBatch batch = new RecordBatch.Builder()
                .Append("amount", nullable: false, new Decimal128Array.Builder(new Decimal128Type(38, 2)).Append(value).Build())
                .Build();

            using var reader = CreateReader(
                schema,
                new DeltaDataReaderOptions { DecimalBehavior = DeltaDataReaderDecimalBehavior.ThrowOnOverflow },
                batch);

            Assert.IsTrue(reader.Read());
            Assert.AreEqual(typeof(decimal), reader.GetFieldType(0));
            Assert.ThrowsException<OverflowException>(() => reader.GetValue(0));
            Assert.ThrowsException<OverflowException>(() => reader.GetDecimal(0));
        }

        private static ArrowStreamDataReader CreateReader(
            Schema schema,
            DeltaDataReaderOptions? options = null,
            params RecordBatch[] batches)
        {
            return new ArrowStreamDataReader(
                new ArrowStreamResult(schema, new TrackingArrowArrayStream(schema, batches)),
                options);
        }

        private sealed class TrackingArrowArrayStream : IArrowArrayStream
        {
            private readonly Queue<RecordBatch> _batches;

            public TrackingArrowArrayStream(Schema schema, params RecordBatch[] batches)
            {
                Schema = schema;
                _batches = new Queue<RecordBatch>(batches);
            }

            public Schema Schema { get; }

            public int DisposeCount { get; private set; }

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return new ValueTask<RecordBatch?>(_batches.Count > 0 ? _batches.Dequeue() : null);
            }

            public void Dispose()
            {
                DisposeCount++;
            }
        }

    }
}
