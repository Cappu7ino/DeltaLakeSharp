using System;
using System.Collections.Generic;
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
            }

            public void Dispose()
            {
            }

            public Schema GetSchema(CancellationToken cancellationToken)
            {
                return _schema;
            }

            public Task<Schema> GetSchemaAsync(CancellationToken cancellationToken)
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

            public IArrowArrayStream OpenReadTableStream(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                throw new NotSupportedException();
            }

            public Task<IArrowArrayStream> OpenReadTableStreamAsync(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                throw new NotSupportedException();
            }
        }
    }
}
