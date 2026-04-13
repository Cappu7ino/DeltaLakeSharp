using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Microsoft.DI.DeltaTableService.Adbc.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.DI.DeltaTableService.Adbc.Tests
{
    [TestClass]
    public class DeltaAdbcConnectionTests
    {
        [TestMethod]
        public void GetTableSchema_RejectsUnknownLogicalTable()
        {
            using var connection = new DeltaAdbcConnection(new TestAdapter(CreateSampleSchema()));

            AdbcException exception = Assert.ThrowsException<AdbcException>(
                () => connection.GetTableSchema(null, null, "other_table"));

            Assert.AreEqual(AdbcStatusCode.NotFound, exception.Status); 
        }

        [TestMethod]
        public void GetTableSchema_RejectsCatalogAndSchema()
        {
            using var connection = new DeltaAdbcConnection(new TestAdapter(CreateSampleSchema()));

            AdbcException catalogException = Assert.ThrowsException<AdbcException>(
                () => connection.GetTableSchema("catalog", null, DeltaAdbcConnectOptions.LogicalTableName));
            Assert.AreEqual(AdbcStatusCode.InvalidArgument, catalogException.Status);

            AdbcException schemaException = Assert.ThrowsException<AdbcException>(
                () => connection.GetTableSchema(null, "schema", DeltaAdbcConnectOptions.LogicalTableName));
            Assert.AreEqual(AdbcStatusCode.InvalidArgument, schemaException.Status);
        }

        [TestMethod]
        public void GetTableSchema_ReturnsSchemaFromAdapter()
        {
            Schema schema = CreateSampleSchema();
            using var connection = new DeltaAdbcConnection(new TestAdapter(schema));

            Schema result = connection.GetTableSchema(null, null, DeltaAdbcConnectOptions.LogicalTableName);

            Assert.AreSame(schema, result);
        }

        [TestMethod]
        public void GetTableSchema_WithConnectionVersion_ForwardsDefaultStatementVersion()
        {
            Schema schema = CreateSampleSchema();
            using var adapter = new TestAdapter(schema);
            using var connection = new DeltaAdbcConnection(new DeltaAdbcConnectOptionsBuilder().WithVersion(4).Build(), adapter);

            Schema result = connection.GetTableSchema(null, null, DeltaAdbcConnectOptions.LogicalTableName);

            Assert.AreSame(schema, result);
            Assert.IsNotNull(adapter.LastSchemaStatementOptions);
            Assert.AreEqual(4L, adapter.LastSchemaStatementOptions!.Version);
        }

        [TestMethod]
        public void CreateStatement_WithConnectionVersion_UsesVersionForDirectReads()
        {
            Schema schema = CreateSampleSchema();
            using var adapter = new TestAdapter(schema);
            using var connection = new DeltaAdbcConnection(new DeltaAdbcConnectOptionsBuilder().WithVersion(6).Build(), adapter);
            using var statement = connection.CreateStatement();

            QueryResult result = statement.ExecuteQuery();

            Assert.AreSame(adapter.ReadStream, result.Stream);
            Assert.IsNotNull(adapter.LastReadStatementOptions);
            Assert.AreEqual(6L, adapter.LastReadStatementOptions!.Version);
        }

        [TestMethod]
        public void CreateStatement_WithConnectionVersion_AllowsStatementOverride()
        {
            Schema schema = CreateSampleSchema();
            using var adapter = new TestAdapter(schema);
            using var connection = new DeltaAdbcConnection(new DeltaAdbcConnectOptionsBuilder().WithVersion(6).Build(), adapter);
            using var statement = connection.CreateStatement();
            statement.SetOption(DeltaAdbcStatementOptions.VersionOptionKey, "2");

            QueryResult result = statement.ExecuteQuery();

            Assert.AreSame(adapter.ReadStream, result.Stream);
            Assert.IsNotNull(adapter.LastReadStatementOptions);
            Assert.AreEqual(2L, adapter.LastReadStatementOptions!.Version);
        }

        [TestMethod]
        public void GetObjects_WithConnectionVersion_UsesHistoricalSchema()
        {
            Schema schema = CreateSampleSchema();
            using var adapter = new TestAdapter(schema);
            using var connection = new DeltaAdbcConnection(new DeltaAdbcConnectOptionsBuilder().WithVersion(5).Build(), adapter);

            using IArrowArrayStream stream = connection.GetObjects(AdbcConnection.GetObjectsDepth.All, null, null, null, null, null);

            Assert.IsNotNull(stream);
            Assert.IsNotNull(adapter.LastSchemaStatementOptions);
            Assert.AreEqual(5L, adapter.LastSchemaStatementOptions!.Version);
        }

        [TestMethod]
        public void ReadPartition_UsesAdapterWithDecodedTokenAndBatchSize()
        {
            Schema schema = CreateSampleSchema();
            using var adapter = new TestAdapter(schema);
            using var connection = new DeltaAdbcConnection(adapter);

            byte[] descriptorBytes = Encoding.UTF8.GetBytes("{\"Token\":\"opaque-token\",\"BatchSize\":32}");
            IArrowArrayStream result = connection.ReadPartition(new PartitionDescriptor(descriptorBytes));

            Assert.AreSame(adapter.PartitionStream, result);
            Assert.AreEqual("opaque-token", adapter.LastPartitionToken);
            Assert.AreEqual(32, adapter.LastPartitionBatchSize);
        }

        [TestMethod]
        public void ReadPartition_WithRawTokenPayload_UsesAdapter()
        {
            Schema schema = CreateSampleSchema();
            using var adapter = new TestAdapter(schema);
            using var connection = new DeltaAdbcConnection(adapter);

            IArrowArrayStream result = connection.ReadPartition(new PartitionDescriptor(Encoding.UTF8.GetBytes("opaque-token")));

            Assert.AreSame(adapter.PartitionStream, result);
            Assert.AreEqual("opaque-token", adapter.LastPartitionToken);
            Assert.IsNull(adapter.LastPartitionBatchSize);
        }

        private static Schema CreateSampleSchema()
        {
            return new Schema.Builder()
                .Field(f => f.Name("id").DataType(Int32Type.Default).Nullable(false))
                .Build();
        }

        private sealed class DeltaAdbcConnectOptionsBuilder
        {
            private readonly Dictionary<string, string> _parameters = new()
            {
                [DeltaAdbcConnectOptions.TableUriKey] = "C:/tables/foo",
            };

            public DeltaAdbcConnectOptionsBuilder WithVersion(long version)
            {
                _parameters[DeltaAdbcStatementOptions.VersionOptionKey] = version.ToString(System.Globalization.CultureInfo.InvariantCulture);
                return this;
            }

            public DeltaAdbcConnectOptions Build()
            {
                return DeltaAdbcConnectOptions.Parse(_parameters);
            }
        }

        private sealed class TestAdapter : IDeltaAdbcClientAdapter
        {
            private readonly Schema _schema;

            public TestAdapter(Schema schema)
            {
                _schema = schema;
                PartitionStream = new TestArrowArrayStream(schema);
                ReadStream = new TestArrowArrayStream(schema);
            }

            public IArrowArrayStream PartitionStream { get; }

            public IArrowArrayStream ReadStream { get; }

            public string? LastPartitionToken { get; private set; }

            public int? LastPartitionBatchSize { get; private set; }

            public DeltaAdbcStatementOptions? LastSchemaStatementOptions { get; private set; }

            public DeltaAdbcStatementOptions? LastReadStatementOptions { get; private set; }

            public void Dispose()
            {
                PartitionStream.Dispose();
                ReadStream.Dispose();
            }

            public IReadOnlyList<Client.Models.DeltaReadPartition> GetReadPartitions(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                throw new NotSupportedException();
            }

            public Task<IReadOnlyList<Client.Models.DeltaReadPartition>> GetReadPartitionsAsync(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                throw new NotSupportedException();
            }

            public Schema GetSchema(DeltaAdbcStatementOptions? statementOptions, CancellationToken cancellationToken)
            {
                LastSchemaStatementOptions = statementOptions?.Clone();
                return _schema;
            }

            public Task<Schema> GetSchemaAsync(DeltaAdbcStatementOptions? statementOptions, CancellationToken cancellationToken)
            {
                LastSchemaStatementOptions = statementOptions?.Clone();
                return Task.FromResult(_schema);
            }

            public IArrowArrayStream OpenQueryStream(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                throw new NotSupportedException();
            }

            public IArrowArrayStream OpenChangeDataStream(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                throw new NotSupportedException();
            }

            public Task<IArrowArrayStream> OpenChangeDataStreamAsync(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                throw new NotSupportedException();
            }

            public IArrowArrayStream OpenChangeDataQueryStream(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                throw new NotSupportedException();
            }

            public Task<IArrowArrayStream> OpenChangeDataQueryStreamAsync(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                throw new NotSupportedException();
            }

            public Task<IArrowArrayStream> OpenQueryStreamAsync(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                throw new NotSupportedException();
            }

            public IArrowArrayStream OpenReadPartitionStream(string partitionToken, int? batchSize, CancellationToken cancellationToken)
            {
                LastPartitionToken = partitionToken;
                LastPartitionBatchSize = batchSize;
                return PartitionStream;
            }

            public IArrowArrayStream OpenReadTableStream(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                LastReadStatementOptions = statementOptions.Clone();
                return ReadStream;
            }

            public Task<IArrowArrayStream> OpenReadPartitionStreamAsync(string partitionToken, int? batchSize, CancellationToken cancellationToken)
            {
                LastPartitionToken = partitionToken;
                LastPartitionBatchSize = batchSize;
                return Task.FromResult(PartitionStream);
            }

            public Task<IArrowArrayStream> OpenReadTableStreamAsync(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                LastReadStatementOptions = statementOptions.Clone();
                return Task.FromResult(ReadStream);
            }

            private sealed class TestArrowArrayStream : IArrowArrayStream
            {
                public TestArrowArrayStream(Schema schema)
                {
                    Schema = schema;
                }

                public Schema Schema { get; }

                public void Dispose()
                {
                }

                public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
                {
                    return new ValueTask<RecordBatch?>((RecordBatch?)null);
                }
            }
        }
    }
}
