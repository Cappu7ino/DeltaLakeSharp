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

        private sealed class TestAdapter : IDeltaAdbcClientAdapter
        {
            private readonly Schema _schema;

            public TestAdapter(Schema schema)
            {
                _schema = schema;
                PartitionStream = new TestArrowArrayStream(schema);
            }

            public IArrowArrayStream PartitionStream { get; }

            public string? LastPartitionToken { get; private set; }

            public int? LastPartitionBatchSize { get; private set; }

            public void Dispose()
            {
                PartitionStream.Dispose();
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
                return _schema;
            }

            public Task<Schema> GetSchemaAsync(DeltaAdbcStatementOptions? statementOptions, CancellationToken cancellationToken)
            {
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
                throw new NotSupportedException();
            }

            public Task<IArrowArrayStream> OpenReadPartitionStreamAsync(string partitionToken, int? batchSize, CancellationToken cancellationToken)
            {
                LastPartitionToken = partitionToken;
                LastPartitionBatchSize = batchSize;
                return Task.FromResult(PartitionStream);
            }

            public Task<IArrowArrayStream> OpenReadTableStreamAsync(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                throw new NotSupportedException();
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
