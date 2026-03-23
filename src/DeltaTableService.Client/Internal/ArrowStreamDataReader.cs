// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Microsoft.DI.DeltaTableService.Client.Models;

namespace Microsoft.DI.DeltaTableService.Client.Internal
{
    internal sealed class ArrowStreamDataReader : DbDataReader, IDbColumnSchemaGenerator
    {
        private readonly ArrowStreamResult _streamResult;
        private readonly Schema _schema;
        private readonly DeltaDataReaderOptions _options;
        private readonly FieldAccessor[] _fields;
        private readonly Dictionary<string, int> _ordinals;
        private RecordBatch? _currentBatch;
        private IArrowArray[] _currentColumns = System.Array.Empty<IArrowArray>();
        private int _currentRowIndex;
        private bool _hasCurrentRow;
        private bool _isClosed;
        private DataTable? _schemaTable;
        private ReadOnlyCollection<DbColumn>? _columnSchema;

        private delegate object? ValueAccessor(IArrowArray array, int index);

        private readonly struct FieldAccessor
        {
            public FieldAccessor(
                string name,
                Type fieldType,
                string dataTypeName,
                bool allowDbNull,
                int? precision,
                int? scale,
                ValueAccessor accessor)
            {
                Name = name;
                FieldType = fieldType;
                DataTypeName = dataTypeName;
                AllowDbNull = allowDbNull;
                Precision = precision;
                Scale = scale;
                Accessor = accessor;
            }

            public string Name { get; }

            public Type FieldType { get; }

            public string DataTypeName { get; }

            public bool AllowDbNull { get; }

            public int? Precision { get; }

            public int? Scale { get; }

            public ValueAccessor Accessor { get; }
        }

        internal ArrowStreamDataReader(ArrowStreamResult streamResult, DeltaDataReaderOptions? options = null)
        {
            _streamResult = streamResult ?? throw new ArgumentNullException(nameof(streamResult));
            _schema = streamResult.Schema;
            _options = options ?? new DeltaDataReaderOptions();
            _fields = new FieldAccessor[_schema.FieldsList.Count];
            _ordinals = new Dictionary<string, int>(_schema.FieldsList.Count, StringComparer.Ordinal);

            for (int i = 0; i < _schema.FieldsList.Count; i++)
            {
                Field field = _schema.FieldsList[i];
                _fields[i] = BuildFieldAccessor(field);
                _ordinals[field.Name] = i;
            }
        }

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => GetValue(GetOrdinal(name));

        public override int Depth => 0;

        public override int FieldCount => _fields.Length;

        public override bool HasRows => !_isClosed;

        public override bool IsClosed => _isClosed;

        public override int RecordsAffected => -1;

        internal Schema ArrowSchema => _schema;

        internal DeltaDataReaderDecimalBehavior DecimalBehavior => _options.DecimalBehavior;

        public override void Close()
        {
            Dispose(true);
        }

        public override bool Read()
        {
            EnsureOpen();
            return AdvanceToNextRowAsync(CancellationToken.None, sync: true).GetAwaiter().GetResult();
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            EnsureOpen();
            return AdvanceToNextRowAsync(cancellationToken, sync: false);
        }

        public override bool NextResult()
        {
            return false;
        }

        public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(false);
        }

        public override string GetName(int ordinal)
        {
            ValidateOrdinal(ordinal);
            return _fields[ordinal].Name;
        }

        public override string GetDataTypeName(int ordinal)
        {
            ValidateOrdinal(ordinal);
            return _fields[ordinal].DataTypeName;
        }

        public override Type GetFieldType(int ordinal)
        {
            ValidateOrdinal(ordinal);
            return _fields[ordinal].FieldType;
        }

        public override int GetOrdinal(string name)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (_ordinals.TryGetValue(name, out int ordinal))
            {
                return ordinal;
            }

            throw new IndexOutOfRangeException($"Column '{name}' was not found.");
        }

        public override object GetValue(int ordinal)
        {
            ValidateActiveRow();
            ValidateOrdinal(ordinal);

            object? value = _fields[ordinal].Accessor(_currentColumns[ordinal], _currentRowIndex);
            return value ?? DBNull.Value;
        }

        public SqlDecimal GetSqlDecimal(int ordinal)
        {
            object value = GetValue(ordinal);
            if (value == DBNull.Value)
            {
                throw new InvalidCastException("Column value is null.");
            }

            if (value is SqlDecimal sqlDecimal)
            {
                return sqlDecimal;
            }

            return new SqlDecimal(Convert.ToDecimal(value, CultureInfo.InvariantCulture));
        }

        public override int GetValues(object[] values)
        {
            if (values == null)
            {
                throw new ArgumentNullException(nameof(values));
            }

            int count = Math.Min(values.Length, FieldCount);
            for (int i = 0; i < count; i++)
            {
                values[i] = GetValue(i);
            }

            return count;
        }

        public override bool IsDBNull(int ordinal)
        {
            ValidateActiveRow();
            ValidateOrdinal(ordinal);
            return _currentColumns[ordinal].IsNull(_currentRowIndex);
        }

        public override bool GetBoolean(int ordinal) => Convert.ToBoolean(GetNonNullValue(ordinal), CultureInfo.InvariantCulture);

        public override byte GetByte(int ordinal) => Convert.ToByte(GetNonNullValue(ordinal), CultureInfo.InvariantCulture);

        public override char GetChar(int ordinal) => Convert.ToChar(GetNonNullValue(ordinal), CultureInfo.InvariantCulture);

        public override DateTime GetDateTime(int ordinal)
        {
            object value = GetNonNullValue(ordinal);
            if (value is DateTimeOffset dto)
            {
                return dto.UtcDateTime;
            }

            return Convert.ToDateTime(value, CultureInfo.InvariantCulture);
        }

        public override decimal GetDecimal(int ordinal)
        {
            object value = GetNonNullValue(ordinal);
            if (value is SqlDecimal sqlDecimal)
            {
                return sqlDecimal.Value;
            }

            return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        }

        public override double GetDouble(int ordinal) => Convert.ToDouble(GetNonNullValue(ordinal), CultureInfo.InvariantCulture);

        public override float GetFloat(int ordinal) => Convert.ToSingle(GetNonNullValue(ordinal), CultureInfo.InvariantCulture);

        public override Guid GetGuid(int ordinal)
        {
            object value = GetNonNullValue(ordinal);
            if (value is Guid guid)
            {
                return guid;
            }

            return Guid.Parse(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);
        }

        public override short GetInt16(int ordinal) => Convert.ToInt16(GetNonNullValue(ordinal), CultureInfo.InvariantCulture);

        public override int GetInt32(int ordinal) => Convert.ToInt32(GetNonNullValue(ordinal), CultureInfo.InvariantCulture);

        public override long GetInt64(int ordinal) => Convert.ToInt64(GetNonNullValue(ordinal), CultureInfo.InvariantCulture);

        public override string GetString(int ordinal) => Convert.ToString(GetNonNullValue(ordinal), CultureInfo.InvariantCulture) ?? string.Empty;

        public override IEnumerator GetEnumerator()
        {
            throw new NotSupportedException("Forward-only reader does not support IEnumerable enumeration.");
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            byte[] source = (byte[])GetNonNullValue(ordinal);
            return CopyToBuffer(source, dataOffset, buffer, bufferOffset, length);
        }

        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            string source = GetString(ordinal);
            return CopyToBuffer(source.ToCharArray(), dataOffset, buffer, bufferOffset, length);
        }

        public override DataTable GetSchemaTable()
        {
            if (_schemaTable != null)
            {
                return _schemaTable;
            }

            var table = new DataTable("SchemaTable")
            {
                Locale = CultureInfo.InvariantCulture,
            };

            table.Columns.Add("ColumnName", typeof(string));
            table.Columns.Add("ColumnOrdinal", typeof(int));
            table.Columns.Add("DataType", typeof(Type));
            table.Columns.Add("DataTypeName", typeof(string));
            table.Columns.Add("AllowDBNull", typeof(bool));
            table.Columns.Add("NumericPrecision", typeof(int));
            table.Columns.Add("NumericScale", typeof(int));

            for (int i = 0; i < _fields.Length; i++)
            {
                DataRow row = table.NewRow();
            object precisionValue = _fields[i].Precision.HasValue ? (object)_fields[i].Precision.Value : DBNull.Value;
            object scaleValue = _fields[i].Scale.HasValue ? (object)_fields[i].Scale.Value : DBNull.Value;
                row["ColumnName"] = _fields[i].Name;
                row["ColumnOrdinal"] = i;
                row["DataType"] = _fields[i].FieldType;
                row["DataTypeName"] = _fields[i].DataTypeName;
                row["AllowDBNull"] = _fields[i].AllowDbNull;
                row["NumericPrecision"] = precisionValue;
                row["NumericScale"] = scaleValue;
                table.Rows.Add(row);
            }

            _schemaTable = table;
            return table;
        }

        public ReadOnlyCollection<DbColumn> GetColumnSchema()
        {
            if (_columnSchema != null)
            {
                return _columnSchema;
            }

            var columns = new List<DbColumn>(_fields.Length);
            for (int i = 0; i < _fields.Length; i++)
            {
                columns.Add(new ArrowDataReaderColumn(
                    _fields[i].Name,
                    i,
                    _fields[i].FieldType,
                    _fields[i].DataTypeName,
                    _fields[i].AllowDbNull,
                    _fields[i].Precision,
                    _fields[i].Scale));
            }

            _columnSchema = columns.AsReadOnly();
            return _columnSchema;
        }

        protected override void Dispose(bool disposing)
        {
            if (_isClosed)
            {
                return;
            }

            if (disposing)
            {
                DisposeCurrentBatch();
                _streamResult.Dispose();
            }

            _isClosed = true;
            _hasCurrentRow = false;
            base.Dispose(disposing);
        }

#if NET8_0_OR_GREATER
        public override async ValueTask DisposeAsync()
        {
            Dispose(true);
            await Task.CompletedTask.ConfigureAwait(false);
        }
#endif

        private async Task<bool> AdvanceToNextRowAsync(CancellationToken cancellationToken, bool sync)
        {
            if (_currentBatch != null && _currentRowIndex + 1 < _currentBatch.Length)
            {
                _currentRowIndex++;
                _hasCurrentRow = true;
                return true;
            }

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                DisposeCurrentBatch();

                RecordBatch? nextBatch = sync
                    ? _streamResult.Stream.ReadNextRecordBatchAsync(cancellationToken).GetAwaiter().GetResult()
                    : await _streamResult.Stream.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);

                if (nextBatch == null)
                {
                    _hasCurrentRow = false;
                    return false;
                }

                if (nextBatch.Length == 0)
                {
                    nextBatch.Dispose();
                    continue;
                }

                _currentBatch = nextBatch;
                _currentColumns = new IArrowArray[nextBatch.ColumnCount];
                for (int i = 0; i < nextBatch.ColumnCount; i++)
                {
                    _currentColumns[i] = nextBatch.Column(i);
                }

                _currentRowIndex = 0;
                _hasCurrentRow = true;
                return true;
            }
        }

        private void DisposeCurrentBatch()
        {
            _currentBatch?.Dispose();
            _currentBatch = null;
            _currentColumns = System.Array.Empty<IArrowArray>();
        }

        private object GetNonNullValue(int ordinal)
        {
            object value = GetValue(ordinal);
            if (value == DBNull.Value)
            {
                throw new InvalidCastException($"Column '{GetName(ordinal)}' contains null.");
            }

            return value;
        }

        private void EnsureOpen()
        {
            if (_isClosed)
            {
                throw new InvalidOperationException("The data reader is closed.");
            }
        }

        private void ValidateActiveRow()
        {
            EnsureOpen();
            if (!_hasCurrentRow || _currentBatch == null)
            {
                throw new InvalidOperationException("Call Read() before accessing row values.");
            }
        }

        private void ValidateOrdinal(int ordinal)
        {
            if ((uint)ordinal >= (uint)_fields.Length)
            {
                throw new IndexOutOfRangeException($"Column ordinal {ordinal} is out of range.");
            }
        }

        private FieldAccessor BuildFieldAccessor(Field field)
        {
            (int? precision, int? scale) = GetPrecisionScale(field.DataType);
            string dataTypeName = field.DataType switch
            {
                Decimal128Type d128 => $"decimal({d128.Precision},{d128.Scale})",
                Decimal256Type d256 => $"decimal({d256.Precision},{d256.Scale})",
                Decimal64Type d64 => $"decimal({d64.Precision},{d64.Scale})",
                Decimal32Type d32 => $"decimal({d32.Precision},{d32.Scale})",
                _ => field.DataType.Name ?? field.DataType.TypeId.ToString(),
            };

            Type fieldType = GetFieldType(field.DataType, precision);

            return new FieldAccessor(
                field.Name,
                fieldType,
                dataTypeName,
                field.IsNullable,
                precision,
                scale,
                BuildValueAccessor(field.DataType));
        }

        private Type GetFieldType(IArrowType dataType, int? precision)
        {
            switch (dataType.TypeId)
            {
                case ArrowTypeId.Boolean: return typeof(bool);
                case ArrowTypeId.Int8: return typeof(sbyte);
                case ArrowTypeId.Int16: return typeof(short);
                case ArrowTypeId.Int32: return typeof(int);
                case ArrowTypeId.Int64: return typeof(long);
                case ArrowTypeId.UInt8: return typeof(byte);
                case ArrowTypeId.UInt16: return typeof(ushort);
                case ArrowTypeId.UInt32: return typeof(uint);
                case ArrowTypeId.UInt64: return typeof(ulong);
                case ArrowTypeId.Float: return typeof(float);
                case ArrowTypeId.Double: return typeof(double);
                case ArrowTypeId.String:
                case ArrowTypeId.StringView:
                case ArrowTypeId.LargeString: return typeof(string);
                case ArrowTypeId.Binary:
                case ArrowTypeId.LargeBinary: return typeof(byte[]);
                case ArrowTypeId.Date32:
                case ArrowTypeId.Date64: return typeof(DateTime);
                case ArrowTypeId.Timestamp:
                    return dataType is TimestampType timestampType && !string.IsNullOrEmpty(timestampType.Timezone)
                        ? typeof(DateTimeOffset)
                        : typeof(DateTime);
                case ArrowTypeId.Decimal32:
                case ArrowTypeId.Decimal64:
                case ArrowTypeId.Decimal128:
                case ArrowTypeId.Decimal256:
                    if (_options.DecimalBehavior == DeltaDataReaderDecimalBehavior.UseSqlDecimal)
                    {
                        return typeof(SqlDecimal);
                    }

                    if (_options.DecimalBehavior == DeltaDataReaderDecimalBehavior.OverflowDecimalAsString
                        && precision.HasValue
                        && !SupportsSystemDecimal(precision.Value))
                    {
                        return typeof(string);
                    }

                    return typeof(decimal);
                default:
                    return typeof(object);
            }
        }

        private static bool SupportsSystemDecimal(int precision)
        {
            return precision <= 28;
        }

        private static (int? precision, int? scale) GetPrecisionScale(IArrowType dataType)
        {
            return dataType switch
            {
                Decimal128Type d128 => (d128.Precision, d128.Scale),
                Decimal256Type d256 => (d256.Precision, d256.Scale),
                Decimal64Type d64 => (d64.Precision, d64.Scale),
                Decimal32Type d32 => (d32.Precision, d32.Scale),
                _ => (null, null),
            };
        }

        private ValueAccessor BuildValueAccessor(IArrowType dataType)
        {
            return dataType.TypeId switch
            {
                ArrowTypeId.Boolean => (array, index) => ((BooleanArray)array).GetValue(index),
                ArrowTypeId.Int8 => (array, index) => ((Int8Array)array).GetValue(index),
                ArrowTypeId.Int16 => (array, index) => ((Int16Array)array).GetValue(index),
                ArrowTypeId.Int32 => (array, index) => ((Int32Array)array).GetValue(index),
                ArrowTypeId.Int64 => (array, index) => ((Int64Array)array).GetValue(index),
                ArrowTypeId.UInt8 => (array, index) => ((UInt8Array)array).GetValue(index),
                ArrowTypeId.UInt16 => (array, index) => ((UInt16Array)array).GetValue(index),
                ArrowTypeId.UInt32 => (array, index) => ((UInt32Array)array).GetValue(index),
                ArrowTypeId.UInt64 => (array, index) => ((UInt64Array)array).GetValue(index),
                ArrowTypeId.Float => (array, index) => ((FloatArray)array).GetValue(index),
                ArrowTypeId.Double => (array, index) => ((DoubleArray)array).GetValue(index),
                ArrowTypeId.String => (array, index) => GetStringValue(array, index),
                ArrowTypeId.StringView => (array, index) => GetStringValue(array, index),
                ArrowTypeId.LargeString => (array, index) => ((LargeStringArray)array).GetString(index),
                ArrowTypeId.Binary => (array, index) => ((BinaryArray)array).GetBytes(index).ToArray(),
                ArrowTypeId.LargeBinary => (array, index) => ((LargeBinaryArray)array).GetBytes(index).ToArray(),
                ArrowTypeId.Date32 => (array, index) => ((Date32Array)array).GetDateTimeOffset(index)?.DateTime,
                ArrowTypeId.Date64 => (array, index) => ((Date64Array)array).GetDateTimeOffset(index)?.DateTime,
                ArrowTypeId.Timestamp => (array, index) => GetTimestampValue((TimestampArray)array, index),
                ArrowTypeId.Decimal32 => (array, index) => ConvertDecimalValue(((Decimal32Array)array).GetDecimal(index), ((Decimal32Array)array).GetString(index)),
                ArrowTypeId.Decimal64 => (array, index) => ConvertDecimalValue(((Decimal64Array)array).GetDecimal(index), ((Decimal64Array)array).GetString(index)),
                ArrowTypeId.Decimal128 => (array, index) => ConvertDecimal128Value((Decimal128Array)array, index),
                ArrowTypeId.Decimal256 => (array, index) => ConvertDecimal256Value((Decimal256Array)array, index),
                _ => (array, index) => null,
            };
        }

        private object? ConvertDecimal128Value(Decimal128Array array, int index)
        {
            SqlDecimal? sqlValue = array.GetSqlDecimal(index);
            if (!sqlValue.HasValue)
            {
                return null;
            }

            string? stringValue = array.GetString(index);
            switch (_options.DecimalBehavior)
            {
                case DeltaDataReaderDecimalBehavior.UseSqlDecimal:
                    return sqlValue.Value;
                case DeltaDataReaderDecimalBehavior.UseDecimal:
                    return array.GetValue(index)
                        ?? throw new OverflowException($"Decimal value '{stringValue ?? sqlValue.Value.ToString()}' cannot be represented as System.Decimal.");
                case DeltaDataReaderDecimalBehavior.OverflowDecimalAsString:
                    try
                    {
                        decimal? decimalValue = array.GetValue(index);
                        return decimalValue.HasValue ? (object)decimalValue.Value : stringValue ?? sqlValue.Value.ToString();
                    }
                    catch (OverflowException)
                    {
                        return stringValue ?? sqlValue.Value.ToString();
                    }
                case DeltaDataReaderDecimalBehavior.ThrowOnOverflow:
                    return array.GetValue(index)
                        ?? throw new OverflowException($"Decimal value '{stringValue ?? sqlValue.Value.ToString()}' cannot be represented as System.Decimal.");
                default:
                    return sqlValue.Value;
            }
        }

        private object? ConvertDecimal256Value(Decimal256Array array, int index)
        {
            decimal? value;
            string? stringValue = array.GetString(index);
            try
            {
                value = array.GetValue(index);
            }
            catch (OverflowException) when (_options.DecimalBehavior == DeltaDataReaderDecimalBehavior.OverflowDecimalAsString)
            {
                return stringValue;
            }

            return ConvertDecimalValue(value, stringValue);
        }

        private object? ConvertDecimalValue(decimal? value, string? stringValue)
        {
            if (!value.HasValue)
            {
                return null;
            }

            return _options.DecimalBehavior switch
            {
                DeltaDataReaderDecimalBehavior.UseSqlDecimal => new SqlDecimal(value.Value),
                DeltaDataReaderDecimalBehavior.UseDecimal => value.Value,
                DeltaDataReaderDecimalBehavior.OverflowDecimalAsString => value.Value,
                DeltaDataReaderDecimalBehavior.ThrowOnOverflow => value.Value,
                _ => value.Value,
            };
        }

        private object? ConvertDecimalValue(SqlDecimal? sqlValue, decimal? decimalValue, string? stringValue)
        {
            if (!sqlValue.HasValue)
            {
                return null;
            }

            switch (_options.DecimalBehavior)
            {
                case DeltaDataReaderDecimalBehavior.UseSqlDecimal:
                    return sqlValue.Value;
                case DeltaDataReaderDecimalBehavior.UseDecimal:
                    if (decimalValue.HasValue)
                    {
                        return decimalValue.Value;
                    }

                    throw new OverflowException($"Decimal value '{stringValue ?? sqlValue.Value.ToString()}' cannot be represented as System.Decimal.");
                case DeltaDataReaderDecimalBehavior.OverflowDecimalAsString:
                    return decimalValue.HasValue ? (object)decimalValue.Value : stringValue;
                case DeltaDataReaderDecimalBehavior.ThrowOnOverflow:
                    if (decimalValue.HasValue)
                    {
                        return decimalValue.Value;
                    }

                    throw new OverflowException($"Decimal value '{stringValue ?? sqlValue.Value.ToString()}' cannot be represented as System.Decimal.");
                default:
                    return sqlValue.Value;
            }
        }

        private static object? GetTimestampValue(TimestampArray array, int index)
        {
            DateTimeOffset? dto = array.GetTimestamp(index);
            if (!dto.HasValue)
            {
                return null;
            }

            TimestampType timestampType = (TimestampType)array.Data.DataType;
            return string.IsNullOrEmpty(timestampType.Timezone)
                ? (object)dto.Value.DateTime
                : dto.Value;
        }

        private static string? GetStringValue(IArrowArray array, int index)
        {
            return array switch
            {
                StringArray stringArray => stringArray.GetString(index),
                StringViewArray stringViewArray => stringViewArray.GetString(index),
                LargeStringArray largeStringArray => largeStringArray.GetString(index),
                _ => null,
            };
        }

        private static long CopyToBuffer<T>(T[] source, long dataOffset, T[]? destination, int bufferOffset, int length)
        {
            if (dataOffset < 0 || dataOffset > source.LongLength)
            {
                throw new ArgumentOutOfRangeException(nameof(dataOffset));
            }

            long available = source.LongLength - dataOffset;
            if (destination == null)
            {
                return available;
            }

            if (bufferOffset < 0 || length < 0 || bufferOffset + length > destination.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(bufferOffset));
            }

            int toCopy = (int)Math.Min(available, length);
            System.Array.Copy(source, (int)dataOffset, destination, bufferOffset, toCopy);
            return toCopy;
        }

        private sealed class ArrowDataReaderColumn : DbColumn
        {
            public ArrowDataReaderColumn(
                string name,
                int ordinal,
                Type dataType,
                string dataTypeName,
                bool allowDBNull,
                int? precision,
                int? scale)
            {
                ColumnName = name;
                ColumnOrdinal = ordinal;
                DataType = dataType;
                DataTypeName = dataTypeName;
                AllowDBNull = allowDBNull;
                NumericPrecision = precision;
                NumericScale = scale;
            }
        }
    }
}
