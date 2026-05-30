using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace DeltaLakeSharp.Adbc
{
    internal static class DeltaAdbcMetadataBuilder
    {
        public static IArrowArrayStream CreateTableTypesStream()
        {
            RecordBatch batch = new RecordBatch(
                StandardSchemas.TableTypesSchema,
                new IArrowArray[]
                {
                    new StringArray.Builder().Append("TABLE").Build(),
                },
                1);
            return new SingleBatchArrowArrayStream(StandardSchemas.TableTypesSchema, batch);
        }

        public static IArrowArrayStream CreateGetInfoStream(IReadOnlyList<AdbcInfoCode> codes)
        {
            List<AdbcInfoCode> effectiveCodes = codes == null || codes.Count == 0
                ? new List<AdbcInfoCode>
                {
                    AdbcInfoCode.VendorName,
                    AdbcInfoCode.VendorVersion,
                    AdbcInfoCode.VendorArrowVersion,
                    AdbcInfoCode.VendorSql,
                    AdbcInfoCode.DriverName,
                    AdbcInfoCode.DriverVersion,
                    AdbcInfoCode.DriverArrowVersion,
                    AdbcInfoCode.DriverAdbcVersion,
                }
                : codes.ToList();

            var infoNameBuilder = new UInt32Array.Builder();
            var typeIdBuilder = new Int8Array.Builder();
            var offsetBuilder = new Int32Array.Builder();
            var stringBuilder = new StringArray.Builder();
            var boolBuilder = new BooleanArray.Builder();
            var int64Builder = new Int64Array.Builder();
            var int32BitmaskBuilder = new Int32Array.Builder();
            ListArray stringListArray = BuildEmptyStringListArray(effectiveCodes.Count);
            ListArray int32MapArray = BuildEmptyInt32MapArray(effectiveCodes.Count);

            int stringCount = 0;
            int boolCount = 0;
            int int64Count = 0;

            foreach (AdbcInfoCode code in effectiveCodes)
            {
                infoNameBuilder.Append((uint)code);
                switch (code)
                {
                    case AdbcInfoCode.VendorName:
                        AppendString(stringBuilder, typeIdBuilder, offsetBuilder, ref stringCount, "Delta Lake");
                        break;
                    case AdbcInfoCode.VendorVersion:
                        AppendString(stringBuilder, typeIdBuilder, offsetBuilder, ref stringCount, "delta-rs via DeltaLakeSharp V3");
                        break;
                    case AdbcInfoCode.VendorArrowVersion:
                        AppendString(stringBuilder, typeIdBuilder, offsetBuilder, ref stringCount, typeof(Schema).Assembly.GetName().Version?.ToString() ?? "22.1.0");
                        break;
                    case AdbcInfoCode.VendorSql:
                        AppendBoolean(boolBuilder, typeIdBuilder, offsetBuilder, ref boolCount, true);
                        break;
                    case AdbcInfoCode.VendorSubstrait:
                        AppendBoolean(boolBuilder, typeIdBuilder, offsetBuilder, ref boolCount, false);
                        break;
                    case AdbcInfoCode.DriverName:
                        AppendString(stringBuilder, typeIdBuilder, offsetBuilder, ref stringCount, "DeltaLakeSharp.Adbc");
                        break;
                    case AdbcInfoCode.DriverVersion:
                        AppendString(stringBuilder, typeIdBuilder, offsetBuilder, ref stringCount, typeof(DeltaAdbcDriver).Assembly.GetName().Version?.ToString() ?? "1.0.0");
                        break;
                    case AdbcInfoCode.DriverArrowVersion:
                        AppendString(stringBuilder, typeIdBuilder, offsetBuilder, ref stringCount, typeof(Schema).Assembly.GetName().Version?.ToString() ?? "22.1.0");
                        break;
                    case AdbcInfoCode.DriverAdbcVersion:
                        AppendInt64(int64Builder, typeIdBuilder, offsetBuilder, ref int64Count, AdbcVersion.Version_1_0_0);
                        break;
                    default:
                        AppendString(stringBuilder, typeIdBuilder, offsetBuilder, ref stringCount, string.Empty);
                        break;
                }

                int32BitmaskBuilder.AppendNull();
            }

            DenseUnionArray unionArray = new DenseUnionArray(
                new UnionType(
                    ((UnionType)StandardSchemas.GetInfoSchema.GetFieldByIndex(1).DataType).Fields,
                    new[] { 0, 1, 2, 3, 4, 5 },
                    UnionMode.Dense),
                effectiveCodes.Count,
                new IArrowArray[]
                {
                    stringBuilder.Build(),
                    boolBuilder.Build(),
                    int64Builder.Build(),
                    int32BitmaskBuilder.Build(),
                    stringListArray,
                    int32MapArray,
                },
                typeIdBuilder.Build().ValueBuffer,
                offsetBuilder.Build().ValueBuffer,
                0,
                0);

            RecordBatch batch = new RecordBatch(StandardSchemas.GetInfoSchema, new IArrowArray[]
            {
                infoNameBuilder.Build(),
                unionArray,
            }, effectiveCodes.Count);

            return new SingleBatchArrowArrayStream(StandardSchemas.GetInfoSchema, batch);
        }

        public static IArrowArrayStream CreateGetObjectsStream(
            Schema schema,
            AdbcConnection.GetObjectsDepth depth,
            string? catalogPattern,
            string? dbSchemaPattern,
            string? tableNamePattern,
            IReadOnlyList<string>? tableTypes,
            string? columnNamePattern)
        {
            bool includeTable = depth == AdbcConnection.GetObjectsDepth.All || depth == AdbcConnection.GetObjectsDepth.Tables;
            bool includeColumns = depth == AdbcConnection.GetObjectsDepth.All;

            if (!PatternMatches(catalogPattern, string.Empty) || !PatternMatches(dbSchemaPattern, string.Empty) || !PatternMatches(tableNamePattern, DeltaAdbcConnectOptions.LogicalTableName))
            {
                return CreateEmptyGetObjectsStream();
            }

            if (tableTypes != null && tableTypes.Count > 0 && !tableTypes.Contains("TABLE", StringComparer.OrdinalIgnoreCase))
            {
                return CreateEmptyGetObjectsStream();
            }

            IArrowArray[] columnStructs = includeColumns
                ? BuildColumnStructs(schema, columnNamePattern)
                : System.Array.Empty<IArrowArray>();

            StructArray tableStruct = BuildSingleTableStruct(includeTable, columnStructs);
            ListArray tablesList = BuildSingleItemList(tableStruct);
            StructArray schemaStruct = new StructArray(
                new StructType(StandardSchemas.DbSchemaSchema),
                1,
                new IArrowArray[]
                {
                    new StringArray.Builder().Append(string.Empty).Build(),
                    tablesList,
                },
                ArrowBuffer.Empty,
                0,
                0);
            ListArray schemasList = BuildSingleItemList(schemaStruct);

            RecordBatch batch = new RecordBatch(StandardSchemas.GetObjectsSchema, new IArrowArray[]
            {
                new StringArray.Builder().Append(string.Empty).Build(),
                schemasList,
            }, 1);

            return new SingleBatchArrowArrayStream(StandardSchemas.GetObjectsSchema, batch);
        }

        private static StructArray BuildSingleTableStruct(bool includeTable, IArrowArray[] columnStructs)
        {
            ListArray columnsList = BuildList(columnStructs, new StructType(StandardSchemas.ColumnSchema));
            ListArray constraintsList = BuildList(System.Array.Empty<IArrowArray>(), new StructType(StandardSchemas.ConstraintSchema));

            return new StructArray(
                new StructType(StandardSchemas.TableSchema),
                includeTable ? 1 : 0,
                new IArrowArray[]
                {
                    new StringArray.Builder().Append(DeltaAdbcConnectOptions.LogicalTableName).Build(),
                    new StringArray.Builder().Append("TABLE").Build(),
                    columnsList,
                    constraintsList,
                },
                ArrowBuffer.Empty,
                0,
                0);
        }

        private static IArrowArray[] BuildColumnStructs(Schema schema, string? columnNamePattern)
        {
            var structs = new List<IArrowArray>();
            for (int i = 0; i < schema.FieldsList.Count; i++)
            {
                Field field = schema.FieldsList[i];
                if (!PatternMatches(columnNamePattern, field.Name))
                {
                    continue;
                }

                StructArray columnStruct = new StructArray(
                    new StructType(StandardSchemas.ColumnSchema),
                    1,
                    new IArrowArray[]
                    {
                        new StringArray.Builder().Append(field.Name).Build(),
                        new Int32Array.Builder().Append(i + 1).Build(),
                        new StringArray.Builder().AppendNull().Build(),
                        new Int16Array.Builder().AppendNull().Build(),
                        new StringArray.Builder().Append(field.DataType.Name).Build(),
                        new Int32Array.Builder().AppendNull().Build(),
                        new Int16Array.Builder().AppendNull().Build(),
                        new Int16Array.Builder().AppendNull().Build(),
                        new Int16Array.Builder().AppendNull().Build(),
                        new StringArray.Builder().AppendNull().Build(),
                        new Int16Array.Builder().AppendNull().Build(),
                        new Int16Array.Builder().AppendNull().Build(),
                        new Int32Array.Builder().AppendNull().Build(),
                        new StringArray.Builder().Append(field.IsNullable ? "YES" : "NO").Build(),
                        new StringArray.Builder().AppendNull().Build(),
                        new StringArray.Builder().AppendNull().Build(),
                        new StringArray.Builder().AppendNull().Build(),
                        new BooleanArray.Builder().AppendNull().Build(),
                        new BooleanArray.Builder().AppendNull().Build(),
                    },
                    ArrowBuffer.Empty,
                    0,
                    0);
                structs.Add(columnStruct);
            }

            return structs.ToArray();
        }

        private static IArrowArrayStream CreateEmptyGetObjectsStream()
        {
            RecordBatch batch = new RecordBatch(StandardSchemas.GetObjectsSchema, new IArrowArray[]
            {
                new StringArray.Builder().Build(),
                BuildList(System.Array.Empty<IArrowArray>(), new StructType(StandardSchemas.DbSchemaSchema)),
            }, 0);
            return new SingleBatchArrowArrayStream(StandardSchemas.GetObjectsSchema, batch);
        }

        private static void AppendString(StringArray.Builder builder, Int8Array.Builder typeIds, Int32Array.Builder offsets, ref int currentOffset, string value)
        {
            typeIds.Append(0);
            offsets.Append(currentOffset++);
            builder.Append(value);
        }

        private static void AppendBoolean(BooleanArray.Builder builder, Int8Array.Builder typeIds, Int32Array.Builder offsets, ref int currentOffset, bool value)
        {
            typeIds.Append(1);
            offsets.Append(currentOffset++);
            builder.Append(value);
        }

        private static void AppendInt64(Int64Array.Builder builder, Int8Array.Builder typeIds, Int32Array.Builder offsets, ref int currentOffset, long value)
        {
            typeIds.Append(2);
            offsets.Append(currentOffset++);
            builder.Append(value);
        }

        private static ListArray BuildEmptyStringListArray(int length)
        {
            return BuildRepeatedEmptyList(length, StringType.Default, new StringArray.Builder().Build());
        }

        private static ListArray BuildEmptyInt32MapArray(int length)
        {
            StructType entriesType = new StructType(new[]
            {
                new Field("key", Int32Type.Default, false),
                new Field("value", Int32Type.Default, true),
            });
            StructArray entries = new StructArray(entriesType, 0, new IArrowArray[]
            {
                new Int32Array.Builder().Build(),
                new Int32Array.Builder().Build(),
            }, ArrowBuffer.Empty, 0, 0);
            return BuildRepeatedEmptyList(length, entriesType, entries);
        }

        private static ListArray BuildRepeatedEmptyList(int length, IArrowType valueType, IArrowArray values)
        {
            int[] offsets = Enumerable.Repeat(0, length + 1).ToArray();
            return new ListArray(new ListType(new Field("item", valueType, true)), length, new ArrowBuffer.Builder<int>().AppendRange(offsets).Build(), values, ArrowBuffer.Empty);
        }

        private static ListArray BuildSingleItemList(IArrowArray values)
        {
            int[] offsets = new[] { 0, 1 };
            return new ListArray(new ListType(new Field("item", values.Data.DataType, true)), 1, new ArrowBuffer.Builder<int>().AppendRange(offsets).Build(), values, ArrowBuffer.Empty);
        }

        private static ListArray BuildList(IArrowArray[] values, IArrowType? explicitValueType = null)
        {
            IArrowType valueType = explicitValueType ?? (values.Length > 0 ? values[0].Data.DataType : new StructType(System.Array.Empty<Field>()));
            IArrowArray innerValues = values.Length == 0
                ? new StructArray((StructType)valueType, 0, System.Array.Empty<IArrowArray>(), ArrowBuffer.Empty, 0, 0)
                : ConcatenateStructArrays(values.Cast<StructArray>().ToArray());
            int[] offsets = new[] { 0, values.Length };
            return new ListArray(new ListType(new Field("item", valueType, true)), 1, new ArrowBuffer.Builder<int>().AppendRange(offsets).Build(), innerValues, ArrowBuffer.Empty);
        }

        private static StructArray ConcatenateStructArrays(StructArray[] arrays)
        {
            if (arrays.Length == 0)
            {
                throw new ArgumentException("At least one struct array is required.", nameof(arrays));
            }

            StructType type = (StructType)arrays[0].Data.DataType;
            var children = new List<IArrowArray>(type.Fields.Count);
            for (int childIndex = 0; childIndex < type.Fields.Count; childIndex++)
            {
                List<object?> values = new List<object?>();
                foreach (StructArray array in arrays)
                {
                    values.Add(V3LikeRead(array.Fields[childIndex], 0));
                }

                children.Add(BuildPrimitiveArray(type.Fields[childIndex], values));
            }

            return new StructArray(type, arrays.Length, children.ToArray(), ArrowBuffer.Empty, 0, 0);
        }

        private static object? V3LikeRead(IArrowArray array, int index)
        {
            return array switch
            {
                StringArray a => a.GetString(index),
                Int32Array a => a.GetValue(index),
                Int16Array a => a.GetValue(index),
                BooleanArray a => a.GetValue(index),
                _ => null,
            };
        }

        private static IArrowArray BuildPrimitiveArray(Field field, List<object?> values)
        {
            switch (field.DataType.TypeId)
            {
                case ArrowTypeId.String:
                    var strings = new StringArray.Builder();
                    foreach (object? value in values)
                    {
                        if (value == null) strings.AppendNull(); else strings.Append((string)value);
                    }
                    return strings.Build();
                case ArrowTypeId.Int32:
                    var ints = new Int32Array.Builder();
                    foreach (object? value in values)
                    {
                        if (value == null) ints.AppendNull(); else ints.Append((int)value);
                    }
                    return ints.Build();
                case ArrowTypeId.Int16:
                    var shorts = new Int16Array.Builder();
                    foreach (object? value in values)
                    {
                        if (value == null) shorts.AppendNull(); else shorts.Append((short)value);
                    }
                    return shorts.Build();
                case ArrowTypeId.Boolean:
                    var bools = new BooleanArray.Builder();
                    foreach (object? value in values)
                    {
                        if (value == null) bools.AppendNull(); else bools.Append((bool)value);
                    }
                    return bools.Build();
                default:
                    throw new NotSupportedException($"Metadata field type '{field.DataType}' is not supported.");
            }
        }

        private static bool PatternMatches(string? pattern, string value)
        {
            if (string.IsNullOrEmpty(pattern))
            {
                return true;
            }

            return string.Equals(pattern, value, StringComparison.Ordinal) || string.Equals(pattern, "%", StringComparison.Ordinal);
        }

        private sealed class SingleBatchArrowArrayStream : IArrowArrayStream
        {
            private readonly RecordBatch _batch;
            private bool _consumed;

            public SingleBatchArrowArrayStream(Schema schema, RecordBatch batch)
            {
                Schema = schema;
                _batch = batch;
            }

            public Schema Schema { get; }

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(System.Threading.CancellationToken cancellationToken = default)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_consumed)
                {
                    return new ValueTask<RecordBatch?>((RecordBatch?)null);
                }

                _consumed = true;
                return new ValueTask<RecordBatch?>(_batch);
            }

            public void Dispose()
            {
                _batch.Dispose();
            }
        }
    }
}
