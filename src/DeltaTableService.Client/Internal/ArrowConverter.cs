// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.DI.DeltaTableService.Client.Models;

namespace Microsoft.DI.DeltaTableService.Client
{
    /// <summary>
    /// Converts between Apache Arrow <see cref="RecordBatch"/> and common .NET
    /// data representations (<see cref="DataTable"/>, dictionaries, object arrays,
    /// CSV strings).
    /// </summary>
    public static class ArrowConverter
    {
        // ------------------------------------------------------------------ //
        //  RecordBatch  →  DataTable
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Converts one or more <see cref="RecordBatch"/> instances into a
        /// single <see cref="DataTable"/>.
        /// </summary>
        public static DataTable ToDataTable(IReadOnlyList<RecordBatch> batches)
        {
            if (batches == null || batches.Count == 0)
            {
                return new DataTable();
            }

            var dt = new DataTable();
            Schema schema = batches[0].Schema;

            // Create columns from the Arrow schema.
            foreach (Field field in schema.FieldsList)
            {
                Type clrType = ArrowTypeToCLR(field.DataType);
                dt.Columns.Add(field.Name, clrType);
            }

            // Populate rows from each batch.
            foreach (RecordBatch batch in batches)
            {
                for (int row = 0; row < batch.Length; row++)
                {
                    DataRow dr = dt.NewRow();
                    for (int col = 0; col < batch.ColumnCount; col++)
                    {
                        dr[col] = GetValue(batch.Column(col), row) ?? DBNull.Value;
                    }
                    dt.Rows.Add(dr);
                }
            }

            return dt;
        }

        // ------------------------------------------------------------------ //
        //  RecordBatch  →  List<Dictionary<string, object>>
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Converts one or more <see cref="RecordBatch"/> instances into a list
        /// of row dictionaries.
        /// </summary>
        public static List<Dictionary<string, object>> ToDictionaryList(IReadOnlyList<RecordBatch> batches)
        {
            var result = new List<Dictionary<string, object>>();
            if (batches == null || batches.Count == 0)
            {
                return result;
            }

            foreach (RecordBatch batch in batches)
            {
                Schema schema = batch.Schema;
                for (int row = 0; row < batch.Length; row++)
                {
                    var dict = new Dictionary<string, object>(batch.ColumnCount);
                    for (int col = 0; col < batch.ColumnCount; col++)
                    {
                        dict[schema.FieldsList[col].Name] = GetValue(batch.Column(col), row);
                    }
                    result.Add(dict);
                }
            }

            return result;
        }

        // ------------------------------------------------------------------ //
        //  Arrow Schema  →  TableSchema
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Converts an Arrow <see cref="Schema"/> to the public <see cref="TableSchema"/> model.
        /// </summary>
        public static TableSchema ToTableSchema(Schema schema)
        {
            var columns = new List<ColumnDefinition>(schema.FieldsList.Count);
            foreach (Field field in schema.FieldsList)
            {
                columns.Add(new ColumnDefinition(field.Name, ArrowTypeToString(field.DataType)));
            }
            return new TableSchema(columns);
        }

        // ------------------------------------------------------------------ //
        //  DataTable  →  RecordBatch
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Converts a <see cref="DataTable"/> to an Arrow <see cref="RecordBatch"/>.
        /// </summary>
        public static RecordBatch FromDataTable(DataTable dt)
        {
            var fields = new List<Field>(dt.Columns.Count);
            var arrays = new List<IArrowArray>(dt.Columns.Count);

            foreach (DataColumn column in dt.Columns)
            {
                IArrowType arrowType = CLRTypeToArrow(column.DataType);
                fields.Add(new Field(column.ColumnName, arrowType, nullable: true));
                arrays.Add(BuildArray(dt, column.Ordinal, arrowType));
            }

            var schema = new Schema(fields, null);
            return new RecordBatch(schema, arrays, dt.Rows.Count);
        }

        // ------------------------------------------------------------------ //
        //  object[][]  →  RecordBatch
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Converts row-major <c>object[][]</c> data with a <see cref="TableSchema"/>
        /// into an Arrow <see cref="RecordBatch"/>.
        /// </summary>
        public static RecordBatch FromRows(object[][] rows, TableSchema tableSchema)
        {
            if (rows == null || rows.Length == 0 || tableSchema == null || tableSchema.Columns.Count == 0)
            {
                return new RecordBatch(new Schema(new List<Field>(), null), System.Array.Empty<IArrowArray>(), 0);
            }

            var fields = new List<Field>(tableSchema.Columns.Count);
            var arrays = new List<IArrowArray>(tableSchema.Columns.Count);

            for (int col = 0; col < tableSchema.Columns.Count; col++)
            {
                ColumnDefinition colDef = tableSchema.Columns[col];
                IArrowType arrowType = StringToArrowType(colDef.DataType);
                fields.Add(new Field(colDef.Name, arrowType, nullable: true));
                arrays.Add(BuildArrayFromRows(rows, col, arrowType));
            }

            var schema = new Schema(fields, null);
            return new RecordBatch(schema, arrays, rows.Length);
        }

        // ------------------------------------------------------------------ //
        //  CSV string  →  RecordBatch
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Parses a CSV string (with header row) into an Arrow <see cref="RecordBatch"/>.
        /// All columns are treated as UTF-8 strings.
        /// </summary>
        public static RecordBatch FromCsv(string csv)
        {
            if (string.IsNullOrWhiteSpace(csv))
            {
                return new RecordBatch(new Schema(new List<Field>(), null), System.Array.Empty<IArrowArray>(), 0);
            }

            var lines = new List<string[]>();
            using (var reader = new StringReader(csv))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (!string.IsNullOrWhiteSpace(line))
                    {
                        lines.Add(line.Split(','));
                    }
                }
            }

            if (lines.Count < 1)
            {
                return new RecordBatch(new Schema(new List<Field>(), null), System.Array.Empty<IArrowArray>(), 0);
            }

            string[] headers = lines[0];
            int colCount = headers.Length;
            int rowCount = lines.Count - 1;

            var fields = new List<Field>(colCount);
            var arrays = new List<IArrowArray>(colCount);

            for (int col = 0; col < colCount; col++)
            {
                fields.Add(new Field(headers[col].Trim(), StringType.Default, nullable: true));
                var builder = new StringArray.Builder();
                for (int row = 1; row <= rowCount; row++)
                {
                    string value = col < lines[row].Length ? lines[row][col].Trim() : null;
                    builder.Append(value);
                }
                arrays.Add(builder.Build());
            }

            var schema = new Schema(fields, null);
            return new RecordBatch(schema, arrays, rowCount);
        }

        // ------------------------------------------------------------------ //
        //  RecordBatch[]  →  IAsyncEnumerable<RecordBatch>
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Wraps one or more <see cref="RecordBatch"/> instances into an
        /// <see cref="IAsyncEnumerable{RecordBatch}"/> suitable for passing to
        /// streaming APIs such as <c>InsertAsync</c> and <c>MergeDataAsync</c>.
        /// </summary>
        public static async IAsyncEnumerable<RecordBatch> ToAsyncEnumerable(
            params RecordBatch[] batches)
        {
            foreach (RecordBatch batch in batches)
            {
                yield return batch;
            }

            await System.Threading.Tasks.Task.CompletedTask; // satisfy async requirement
        }

        // ================================================================== //
        //  Private helpers
        // ================================================================== //

        /// <summary>
        /// Extracts a scalar value from the given Arrow array at the specified row index.
        /// </summary>
        private static object GetValue(IArrowArray array, int index)
        {
            if (array.IsNull(index))
            {
                return null;
            }

            switch (array)
            {
                case StringArray sa:
                    return sa.GetString(index);
                case StringViewArray sva:
                    return sva.GetString(index);
                case LargeStringArray lsa:
                    return lsa.GetString(index);
                case Int8Array i8:
                    return i8.GetValue(index);
                case Int16Array i16:
                    return i16.GetValue(index);
                case Int32Array i32:
                    return i32.GetValue(index);
                case Int64Array i64:
                    return i64.GetValue(index);
                case UInt8Array u8:
                    return u8.GetValue(index);
                case UInt16Array u16:
                    return u16.GetValue(index);
                case UInt32Array u32:
                    return u32.GetValue(index);
                case UInt64Array u64:
                    return u64.GetValue(index);
                case FloatArray f32:
                    return f32.GetValue(index);
                case DoubleArray f64:
                    return f64.GetValue(index);
                case BooleanArray ba:
                    return ba.GetValue(index);
                case Date32Array d32:
                    return d32.GetDateTimeOffset(index)?.DateTime;
                case Date64Array d64:
                    return d64.GetDateTimeOffset(index)?.DateTime;
                case TimestampArray ts:
                {
                    DateTimeOffset? dto = ts.GetTimestamp(index);
                    if (dto == null) return null;
                    // Return DateTimeOffset for tz-aware timestamps, DateTime for tz-naive (timestamp_ntz).
                    var tsType = (TimestampType)ts.Data.DataType;
                    return string.IsNullOrEmpty(tsType.Timezone)
                        ? (object)dto.Value.DateTime
                        : (object)dto.Value;
                }
                case BinaryArray bin:
                    return bin.GetBytes(index).ToArray();
                case LargeBinaryArray lbin:
                    return lbin.GetBytes(index).ToArray();
                default:
                    // Fallback: convert to string representation.
                    return array.GetType()
                        .GetMethod("GetValue")
                        ?.Invoke(array, new object[] { index })
                        ?.ToString();
            }
        }

        /// <summary>
        /// Maps an Arrow <see cref="IArrowType"/> to the closest CLR <see cref="Type"/>.
        /// </summary>
        private static Type ArrowTypeToCLR(IArrowType arrowType)
        {
            switch (arrowType.TypeId)
            {
                case ArrowTypeId.String:
                case ArrowTypeId.LargeString:
                    return typeof(string);
                case ArrowTypeId.Int8:
                    return typeof(sbyte);
                case ArrowTypeId.Int16:
                    return typeof(short);
                case ArrowTypeId.Int32:
                    return typeof(int);
                case ArrowTypeId.Int64:
                    return typeof(long);
                case ArrowTypeId.UInt8:
                    return typeof(byte);
                case ArrowTypeId.UInt16:
                    return typeof(ushort);
                case ArrowTypeId.UInt32:
                    return typeof(uint);
                case ArrowTypeId.UInt64:
                    return typeof(ulong);
                case ArrowTypeId.Float:
                    return typeof(float);
                case ArrowTypeId.Double:
                    return typeof(double);
                case ArrowTypeId.Boolean:
                    return typeof(bool);
                case ArrowTypeId.Date32:
                case ArrowTypeId.Date64:
                    return typeof(DateTime);
                case ArrowTypeId.Timestamp:
                    return arrowType is TimestampType tt && !string.IsNullOrEmpty(tt.Timezone)
                        ? typeof(DateTimeOffset)
                        : typeof(DateTime);
                case ArrowTypeId.Binary:
                case ArrowTypeId.LargeBinary:
                    return typeof(byte[]);
                default:
                    return typeof(string);
            }
        }

        /// <summary>
        /// Returns a human-readable string name for an Arrow type.
        /// </summary>
        private static string ArrowTypeToString(IArrowType arrowType)
        {
            switch (arrowType.TypeId)
            {
                case ArrowTypeId.String: return "string";
                case ArrowTypeId.LargeString: return "large_utf8";
                case ArrowTypeId.Int8: return "int8";
                case ArrowTypeId.Int16: return "int16";
                case ArrowTypeId.Int32: return "int32";
                case ArrowTypeId.Int64: return "int64";
                case ArrowTypeId.UInt8: return "uint8";
                case ArrowTypeId.UInt16: return "uint16";
                case ArrowTypeId.UInt32: return "uint32";
                case ArrowTypeId.UInt64: return "uint64";
                case ArrowTypeId.Float: return "float";
                case ArrowTypeId.Double: return "double";
                case ArrowTypeId.Boolean: return "boolean";
                case ArrowTypeId.Date32: return "date";
                case ArrowTypeId.Date64: return "date";
                case ArrowTypeId.Timestamp:
                    return arrowType is TimestampType tts && !string.IsNullOrEmpty(tts.Timezone)
                        ? "timestamp"
                        : "timestamp_ntz";
                case ArrowTypeId.Binary: return "binary";
                case ArrowTypeId.LargeBinary: return "large_binary";
                default: return arrowType.Name ?? "unknown";
            }
        }

        /// <summary>
        /// Converts a CLR <see cref="Type"/> to the corresponding <see cref="IArrowType"/>.
        /// </summary>
        private static IArrowType CLRTypeToArrow(Type clrType)
        {
            if (clrType == typeof(string)) return StringType.Default;
            if (clrType == typeof(sbyte)) return Int8Type.Default;
            if (clrType == typeof(short)) return Int16Type.Default;
            if (clrType == typeof(int)) return Int32Type.Default;
            if (clrType == typeof(long)) return Int64Type.Default;
            if (clrType == typeof(byte)) return UInt8Type.Default;
            if (clrType == typeof(ushort)) return UInt16Type.Default;
            if (clrType == typeof(uint)) return UInt32Type.Default;
            if (clrType == typeof(ulong)) return UInt64Type.Default;
            if (clrType == typeof(float)) return FloatType.Default;
            if (clrType == typeof(double)) return DoubleType.Default;
            if (clrType == typeof(bool)) return BooleanType.Default;
            if (clrType == typeof(DateTime)) return new TimestampType(TimeUnit.Microsecond, (string)null);
            if (clrType == typeof(DateTimeOffset)) return new TimestampType(TimeUnit.Microsecond, "UTC");
            if (clrType == typeof(byte[])) return BinaryType.Default;
            return StringType.Default;
        }

        /// <summary>
        /// Converts a type name string to an Arrow type.
        /// </summary>
        private static IArrowType StringToArrowType(string typeName)
        {
            switch ((typeName ?? "string").ToLowerInvariant())
            {
                case "string": return StringType.Default;
                case "large_utf8": case "large_string": return LargeStringType.Default;
                case "int": case "int32": case "integer": return Int32Type.Default;
                case "long": case "int64": case "bigint": return Int64Type.Default;
                case "short": case "int16": case "smallint": return Int16Type.Default;
                case "byte": case "int8": case "tinyint": return Int8Type.Default;
                case "uint8": return UInt8Type.Default;
                case "uint16": return UInt16Type.Default;
                case "uint32": return UInt32Type.Default;
                case "uint64": return UInt64Type.Default;
                case "float": return FloatType.Default;
                case "double": return DoubleType.Default;
                case "boolean": case "bool": return BooleanType.Default;
                case "date": return Date32Type.Default;
                case "timestamp": return new TimestampType(TimeUnit.Microsecond, "UTC");
                case "timestamp_ntz": return new TimestampType(TimeUnit.Microsecond, (string)null);
                case "binary": return BinaryType.Default;
                case "large_binary": return LargeBinaryType.Default;
                default: return StringType.Default;
            }
        }

        /// <summary>
        /// Builds an <see cref="IArrowArray"/> from a <see cref="DataTable"/> column.
        /// </summary>
        private static IArrowArray BuildArray(DataTable dt, int colIndex, IArrowType arrowType)
        {
            switch (arrowType.TypeId)
            {
                case ArrowTypeId.String:
                {
                    var b = new StringArray.Builder();
                    foreach (DataRow row in dt.Rows) b.Append(row[colIndex] == DBNull.Value ? null : Convert.ToString(row[colIndex]));
                    return b.Build();
                }
                case ArrowTypeId.Int32:
                {
                    var b = new Int32Array.Builder();
                    foreach (DataRow row in dt.Rows)
                        b.Append(row[colIndex] == DBNull.Value ? (int?)null : Convert.ToInt32(row[colIndex]));
                    return b.Build();
                }
                case ArrowTypeId.Int64:
                {
                    var b = new Int64Array.Builder();
                    foreach (DataRow row in dt.Rows)
                        b.Append(row[colIndex] == DBNull.Value ? (long?)null : Convert.ToInt64(row[colIndex]));
                    return b.Build();
                }
                case ArrowTypeId.Double:
                {
                    var b = new DoubleArray.Builder();
                    foreach (DataRow row in dt.Rows)
                        b.Append(row[colIndex] == DBNull.Value ? (double?)null : Convert.ToDouble(row[colIndex]));
                    return b.Build();
                }
                case ArrowTypeId.Float:
                {
                    var b = new FloatArray.Builder();
                    foreach (DataRow row in dt.Rows)
                        b.Append(row[colIndex] == DBNull.Value ? (float?)null : Convert.ToSingle(row[colIndex]));
                    return b.Build();
                }
                case ArrowTypeId.Boolean:
                {
                    var b = new BooleanArray.Builder();
                    foreach (DataRow row in dt.Rows)
                    {
                        if (row[colIndex] == DBNull.Value)
                            b.AppendNull();
                        else
                            b.Append(Convert.ToBoolean(row[colIndex]));
                    }
                    return b.Build();
                }
                case ArrowTypeId.Timestamp:
                {
                    var tsType = (TimestampType)arrowType;
                    var b = new TimestampArray.Builder(tsType);
                    foreach (DataRow row in dt.Rows)
                    {
                        if (row[colIndex] == DBNull.Value)
                        {
                            b.AppendNull();
                        }
                        else if (row[colIndex] is DateTimeOffset dto)
                        {
                            b.Append(dto);
                        }
                        else
                        {
                            var dtVal = Convert.ToDateTime(row[colIndex]);
                            b.Append(new DateTimeOffset(dtVal, TimeSpan.Zero));
                        }
                    }
                    return b.Build();
                }
                default:
                {
                    // Fallback: serialize everything as strings.
                    var b = new StringArray.Builder();
                    foreach (DataRow row in dt.Rows)
                        b.Append(row[colIndex] == DBNull.Value ? null : Convert.ToString(row[colIndex]));
                    return b.Build();
                }
            }
        }

        /// <summary>
        /// Builds an <see cref="IArrowArray"/> from <c>object[][]</c> rows at the given column index.
        /// </summary>
        private static IArrowArray BuildArrayFromRows(object[][] rows, int colIndex, IArrowType arrowType)
        {
            switch (arrowType.TypeId)
            {
                case ArrowTypeId.String:
                {
                    var b = new StringArray.Builder();
                    foreach (object[] row in rows) b.Append(colIndex < row.Length && row[colIndex] != null ? Convert.ToString(row[colIndex]) : null);
                    return b.Build();
                }
                case ArrowTypeId.Int32:
                {
                    var b = new Int32Array.Builder();
                    foreach (object[] row in rows)
                    {
                        object val = colIndex < row.Length ? row[colIndex] : null;
                        b.Append(val == null ? (int?)null : Convert.ToInt32(val));
                    }
                    return b.Build();
                }
                case ArrowTypeId.Int64:
                {
                    var b = new Int64Array.Builder();
                    foreach (object[] row in rows)
                    {
                        object val = colIndex < row.Length ? row[colIndex] : null;
                        b.Append(val == null ? (long?)null : Convert.ToInt64(val));
                    }
                    return b.Build();
                }
                case ArrowTypeId.Double:
                {
                    var b = new DoubleArray.Builder();
                    foreach (object[] row in rows)
                    {
                        object val = colIndex < row.Length ? row[colIndex] : null;
                        b.Append(val == null ? (double?)null : Convert.ToDouble(val));
                    }
                    return b.Build();
                }
                case ArrowTypeId.Float:
                {
                    var b = new FloatArray.Builder();
                    foreach (object[] row in rows)
                    {
                        object val = colIndex < row.Length ? row[colIndex] : null;
                        b.Append(val == null ? (float?)null : Convert.ToSingle(val));
                    }
                    return b.Build();
                }
                case ArrowTypeId.Boolean:
                {
                    var b = new BooleanArray.Builder();
                    foreach (object[] row in rows)
                    {
                        object val = colIndex < row.Length ? row[colIndex] : null;
                        if (val == null)
                            b.AppendNull();
                        else
                            b.Append(Convert.ToBoolean(val));
                    }
                    return b.Build();
                }
                case ArrowTypeId.Timestamp:
                {
                    var tsType = (TimestampType)arrowType;
                    var b = new TimestampArray.Builder(tsType);
                    foreach (object[] row in rows)
                    {
                        object val = colIndex < row.Length ? row[colIndex] : null;
                        if (val == null)
                        {
                            b.AppendNull();
                        }
                        else if (val is DateTimeOffset dto)
                        {
                            b.Append(dto);
                        }
                        else
                        {
                            var dtVal = Convert.ToDateTime(val);
                            b.Append(new DateTimeOffset(dtVal, TimeSpan.Zero));
                        }
                    }
                    return b.Build();
                }
                case ArrowTypeId.Date32:
                {
                    // Date32 stores the number of days since Unix epoch (1970-01-01).
                    var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                    var b = new Date32Array.Builder();
                    foreach (object[] row in rows)
                    {
                        object val = colIndex < row.Length ? row[colIndex] : null;
                        if (val == null)
                        {
                            b.AppendNull();
                        }
                        else
                        {
                            DateTime dtVal = val is DateTime d ? d : Convert.ToDateTime(val);
                            b.Append(dtVal);
                        }
                    }
                    return b.Build();
                }
                default:
                {
                    var b = new StringArray.Builder();
                    foreach (object[] row in rows) b.Append(colIndex < row.Length && row[colIndex] != null ? Convert.ToString(row[colIndex]) : null);
                    return b.Build();
                }
            }
        }
    }
}
