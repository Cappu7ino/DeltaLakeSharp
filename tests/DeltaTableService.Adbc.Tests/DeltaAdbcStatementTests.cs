using System;
using System.Collections.Generic;
using System.Linq;
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
    public class DeltaAdbcStatementTests
    {
        [TestMethod]
        public void ExecuteQuery_WithoutSql_UsesTableReadStream()
        {
            using var adapter = new StatementTestAdapter();
            using var statement = new DeltaAdbcStatement(adapter);

            QueryResult result = statement.ExecuteQuery();

            Assert.AreEqual(-1L, result.RowCount);
            Assert.AreSame(adapter.ReadStream, result.Stream);
            Assert.AreEqual(1, adapter.ReadCalls);
            Assert.AreEqual(0, adapter.QueryCalls);
        }

        [TestMethod]
        public void ExecuteQuery_WithSql_UsesQueryStream()
        {
            using var adapter = new StatementTestAdapter();
            using var statement = new DeltaAdbcStatement(adapter)
            {
                SqlQuery = "select * from delta_table",
            };

            QueryResult result = statement.ExecuteQuery();

            Assert.AreEqual(-1L, result.RowCount);
            Assert.AreSame(adapter.QueryStream, result.Stream);
            Assert.AreEqual(0, adapter.ReadCalls);
            Assert.AreEqual(1, adapter.QueryCalls);
            Assert.AreEqual("select * from delta_table", adapter.LastSql);
        }

        [TestMethod]
        public void ExecutePartitioned_WithoutSql_UsesPartitionPlanning()
        {
            using var adapter = new StatementTestAdapter();
            using var statement = new DeltaAdbcStatement(adapter);

            PartitionedResult result = statement.ExecutePartitioned();

            Assert.AreEqual(-1L, result.AffectedRows);
            Assert.AreEqual(1, adapter.GetPartitionsCalls);
            Assert.AreEqual(2, result.PartitionDescriptors.Count);

            string firstPayload = Encoding.UTF8.GetString(result.PartitionDescriptors[0].Descriptor.ToArray());
            using JsonDocument document = JsonDocument.Parse(firstPayload);
            Assert.AreEqual("token-0", document.RootElement.GetProperty("Token").GetString());
            Assert.IsFalse(document.RootElement.TryGetProperty("BatchSize", out JsonElement batchSizeElement) && batchSizeElement.ValueKind != JsonValueKind.Null);
            Assert.AreEqual(1, result.Schema.FieldsList.Count);
            Assert.AreEqual("value", result.Schema.FieldsList[0].Name);
        }

        [TestMethod]
        public void ExecutePartitioned_WithBatchSize_EmbedsBatchSizeInDescriptor()
        {
            using var adapter = new StatementTestAdapter();
            using var statement = new DeltaAdbcStatement(adapter);
            statement.SetOption(DeltaAdbcStatementOptions.BatchSizeOptionKey, "256");

            PartitionedResult result = statement.ExecutePartitioned();

            string payload = Encoding.UTF8.GetString(result.PartitionDescriptors[0].Descriptor.ToArray());
            using JsonDocument document = JsonDocument.Parse(payload);
            Assert.AreEqual(256, document.RootElement.GetProperty("BatchSize").GetInt32());
        }

        [TestMethod]
        public void ExecutePartitioned_WithSql_ThrowsInvalidArgument()
        {
            using var adapter = new StatementTestAdapter();
            using var statement = new DeltaAdbcStatement(adapter)
            {
                SqlQuery = "select * from delta_table",
            };

            AdbcException exception = Assert.ThrowsException<AdbcException>(() => statement.ExecutePartitioned());

            Assert.AreEqual(AdbcStatusCode.InvalidArgument, exception.Status);
            StringAssert.Contains(exception.Message, "direct Delta table reads");
        }

        [TestMethod]
        public void ExecutePartitioned_WithMaxRows_ThrowsInvalidArgument()
        {
            using var adapter = new StatementTestAdapter();
            using var statement = new DeltaAdbcStatement(adapter);
            statement.SetOption(DeltaAdbcStatementOptions.MaxRowsOptionKey, "2");

            AdbcException exception = Assert.ThrowsException<AdbcException>(() => statement.ExecutePartitioned());

            Assert.AreEqual(AdbcStatusCode.InvalidArgument, exception.Status);
            StringAssert.Contains(exception.Message, DeltaAdbcStatementOptions.MaxRowsOptionKey);
        }

        [TestMethod]
        public void ExecutePartitioned_WithCdf_ThrowsInvalidArgument()
        {
            using var adapter = new StatementTestAdapter();
            using var statement = new DeltaAdbcStatement(adapter);
            statement.SetOption(DeltaAdbcStatementOptions.CdfStartingVersionOptionKey, "1");

            AdbcException exception = Assert.ThrowsException<AdbcException>(() => statement.ExecutePartitioned());

            Assert.AreEqual(AdbcStatusCode.InvalidArgument, exception.Status);
            StringAssert.Contains(exception.Message, "Change Data Feed");
        }

        [TestMethod]
        public void ExecuteQuery_WithStatementBatchSize_OverridesAdapterDefaults()
        {
            using var adapter = new StatementTestAdapter();
            using var statement = new DeltaAdbcStatement(
                adapter,
                new DeltaAdbcStatementOptions().WithDefaults(version: 2, maxRows: null, batchSize: 64))
            {
                SqlQuery = "select * from delta_table",
            };
            statement.SetOption(DeltaAdbcStatementOptions.BatchSizeOptionKey, "2048");
            statement.SetOption(DeltaAdbcStatementOptions.VersionOptionKey, "5");

            QueryResult result = statement.ExecuteQuery();

            Assert.AreEqual(-1L, result.RowCount);
            Assert.AreSame(adapter.QueryStream, result.Stream);
            Assert.AreEqual(2048, adapter.LastStatementOptions!.BatchSize);
            Assert.AreEqual(5L, adapter.LastStatementOptions.Version);
            Assert.IsNull(adapter.LastStatementOptions.MaxRows);
        }

        [TestMethod]
        public void ExecuteQuery_WithStatementMaxRowsOnSql_ThrowsInvalidArgument()
        {
            using var adapter = new StatementTestAdapter();
            using var statement = new DeltaAdbcStatement(adapter)
            {
                SqlQuery = "select * from delta_table",
            };
            statement.SetOption(DeltaAdbcStatementOptions.MaxRowsOptionKey, "2");

            AdbcException exception = Assert.ThrowsException<AdbcException>(() => statement.ExecuteQuery());

            Assert.AreEqual(AdbcStatusCode.InvalidArgument, exception.Status);
            StringAssert.Contains(exception.Message, DeltaAdbcStatementOptions.MaxRowsOptionKey);
        }

        [TestMethod]
        public void SetOption_WithUnknownOption_ThrowsInvalidArgument()
        {
            using var statement = new DeltaAdbcStatement(new StatementTestAdapter());

            AdbcException exception = Assert.ThrowsException<AdbcException>(() => statement.SetOption("delta.unknown", "1"));

            Assert.AreEqual(AdbcStatusCode.InvalidArgument, exception.Status);
        }

        [TestMethod]
        public void SetOption_WithInvalidBatchSize_ThrowsInvalidArgument()
        {
            using var statement = new DeltaAdbcStatement(new StatementTestAdapter());

            AdbcException exception = Assert.ThrowsException<AdbcException>(() => statement.SetOption(DeltaAdbcStatementOptions.BatchSizeOptionKey, "0"));

            Assert.AreEqual(AdbcStatusCode.InvalidArgument, exception.Status);
        }

        [TestMethod]
        public void ExecuteQuery_WithCdfStartingVersionAndNoSql_UsesChangeDataReadStream()
        {
            using var adapter = new StatementTestAdapter();
            using var statement = new DeltaAdbcStatement(adapter);
            statement.SetOption(DeltaAdbcStatementOptions.CdfStartingVersionOptionKey, "3");
            statement.SetOption(DeltaAdbcStatementOptions.CdfEndingVersionOptionKey, "5");

            QueryResult result = statement.ExecuteQuery();

            Assert.AreEqual(-1L, result.RowCount);
            Assert.AreSame(adapter.ChangeDataReadStream, result.Stream);
            Assert.AreEqual(1, adapter.ChangeDataReadCalls);
            Assert.AreEqual(3L, adapter.LastStartingVersion);
            Assert.AreEqual(5L, adapter.LastEndingVersion);
        }

        [TestMethod]
        public void ExecuteQuery_WithCdfOptionsAndProjectedSql_UsesCdfQuery()
        {
            using var adapter = new StatementTestAdapter();
            using var statement = new DeltaAdbcStatement(adapter)
            {
                SqlQuery = "SELECT id, _change_type FROM _cdf WHERE _change_type <> 'update_preimage'",
            };
            statement.SetOption(DeltaAdbcStatementOptions.CdfStartingVersionOptionKey, "7");

            QueryResult result = statement.ExecuteQuery();

            Assert.AreEqual(-1L, result.RowCount);
            Assert.AreSame(adapter.ChangeDataQueryStream, result.Stream);
            Assert.AreEqual(1, adapter.ChangeDataQueryCalls);
            Assert.AreEqual("SELECT id, _change_type FROM _cdf WHERE _change_type <> 'update_preimage'", adapter.LastChangeDataSql);
            Assert.AreEqual(7L, adapter.LastStartingVersion);
            Assert.IsNull(adapter.LastEndingVersion);
        }

        [TestMethod]
        public void ExecuteQuery_WithCdfOptionsAndSqlWithoutCdfReference_ThrowsInvalidArgument()
        {
            using var adapter = new StatementTestAdapter();
            using var statement = new DeltaAdbcStatement(adapter)
            {
                SqlQuery = "SELECT * FROM delta_table",
            };
            statement.SetOption(DeltaAdbcStatementOptions.CdfStartingVersionOptionKey, "1");

            AdbcException exception = Assert.ThrowsException<AdbcException>(() => statement.ExecuteQuery());

            Assert.AreEqual(AdbcStatusCode.InvalidArgument, exception.Status);
            StringAssert.Contains(exception.Message, "_cdf");
        }

        [TestMethod]
        public void ExecuteQuery_WithOnlyCdfEndingVersion_ThrowsInvalidArgument()
        {
            using var adapter = new StatementTestAdapter();
            using var statement = new DeltaAdbcStatement(adapter);
            statement.SetOption(DeltaAdbcStatementOptions.CdfEndingVersionOptionKey, "5");

            AdbcException exception = Assert.ThrowsException<AdbcException>(() => statement.ExecuteQuery());

            Assert.AreEqual(AdbcStatusCode.InvalidArgument, exception.Status);
            StringAssert.Contains(exception.Message, DeltaAdbcStatementOptions.CdfStartingVersionOptionKey);
        }

        [TestMethod]
        public void ExecuteUpdate_IsNotImplemented()
        {
            using var statement = new DeltaAdbcStatement(new StatementTestAdapter());

            Assert.ThrowsException<AdbcException>(() => statement.ExecuteUpdate());
        }

        [TestMethod]
        public void Prepare_IsNotImplemented()
        {
            using var statement = new DeltaAdbcStatement(new StatementTestAdapter());

            Assert.ThrowsException<AdbcException>(() => statement.Prepare());
        }

        private sealed class StatementTestAdapter : IDeltaAdbcClientAdapter
        {
            private static readonly Schema StreamSchema = new Schema.Builder()
                .Field(f => f.Name("value").DataType(StringType.Default).Nullable(true))
                .Build();

            public StatementTestAdapter()
            {
                ReadStream = CreateStream("table");
                QueryStream = CreateStream("query");
                ChangeDataReadStream = CreateStream("cdf-read");
                ChangeDataQueryStream = CreateStream("cdf-query");
                PartitionStream = CreateStream("partition");
                Partitions = new[]
                {
                    new Client.Models.DeltaReadPartition("token-0", version: 5, ordinal: 0, totalPartitions: 2, fileCount: 1),
                    new Client.Models.DeltaReadPartition("token-1", version: 5, ordinal: 1, totalPartitions: 2, fileCount: 1),
                };
            }

            public int ReadCalls { get; private set; }

            public int QueryCalls { get; private set; }

            public int ChangeDataReadCalls { get; private set; }

            public int ChangeDataQueryCalls { get; private set; }

            public int GetPartitionsCalls { get; private set; }

            public int ReadPartitionCalls { get; private set; }

            public string? LastSql { get; private set; }

            public string? LastChangeDataSql { get; private set; }

            public DeltaAdbcStatementOptions? LastStatementOptions { get; private set; }

            public long LastStartingVersion { get; private set; }

            public long? LastEndingVersion { get; private set; }

            public string? LastPartitionToken { get; private set; }

            public int? LastPartitionBatchSize { get; private set; }

            public IArrowArrayStream ReadStream { get; }

            public IArrowArrayStream QueryStream { get; }

            public IArrowArrayStream ChangeDataReadStream { get; }

            public IArrowArrayStream ChangeDataQueryStream { get; }

            public IArrowArrayStream PartitionStream { get; }

            public IReadOnlyList<Client.Models.DeltaReadPartition> Partitions { get; }

            public void Dispose()
            {
                ReadStream.Dispose();
                QueryStream.Dispose();
                ChangeDataReadStream.Dispose();
                ChangeDataQueryStream.Dispose();
                PartitionStream.Dispose();
            }

            public IReadOnlyList<Client.Models.DeltaReadPartition> GetReadPartitions(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                GetPartitionsCalls++;
                LastStatementOptions = statementOptions.Clone();
                return Partitions;
            }

            public Task<IReadOnlyList<Client.Models.DeltaReadPartition>> GetReadPartitionsAsync(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                GetPartitionsCalls++;
                LastStatementOptions = statementOptions.Clone();
                return Task.FromResult(Partitions);
            }

            public Schema GetSchema(DeltaAdbcStatementOptions? statementOptions, CancellationToken cancellationToken)
            {
                return StreamSchema;
            }

            public Task<Schema> GetSchemaAsync(DeltaAdbcStatementOptions? statementOptions, CancellationToken cancellationToken)
            {
                return Task.FromResult(StreamSchema);
            }

            public IArrowArrayStream OpenQueryStream(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                QueryCalls++;
                LastSql = sql;
                LastStatementOptions = statementOptions.Clone();
                return QueryStream;
            }

            public IArrowArrayStream OpenChangeDataStream(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                ChangeDataReadCalls++;
                LastStartingVersion = statementOptions.CdfStartingVersion!.Value;
                LastEndingVersion = statementOptions.CdfEndingVersion;
                LastStatementOptions = statementOptions.Clone();
                return ChangeDataReadStream;
            }

            public Task<IArrowArrayStream> OpenChangeDataStreamAsync(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                ChangeDataReadCalls++;
                LastStartingVersion = statementOptions.CdfStartingVersion!.Value;
                LastEndingVersion = statementOptions.CdfEndingVersion;
                LastStatementOptions = statementOptions.Clone();
                return Task.FromResult(ChangeDataReadStream);
            }

            public IArrowArrayStream OpenChangeDataQueryStream(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                ChangeDataQueryCalls++;
                LastChangeDataSql = sql;
                LastStartingVersion = statementOptions.CdfStartingVersion!.Value;
                LastEndingVersion = statementOptions.CdfEndingVersion;
                LastStatementOptions = statementOptions.Clone();
                return ChangeDataQueryStream;
            }

            public Task<IArrowArrayStream> OpenChangeDataQueryStreamAsync(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                ChangeDataQueryCalls++;
                LastChangeDataSql = sql;
                LastStartingVersion = statementOptions.CdfStartingVersion!.Value;
                LastEndingVersion = statementOptions.CdfEndingVersion;
                LastStatementOptions = statementOptions.Clone();
                return Task.FromResult(ChangeDataQueryStream);
            }

            public Task<IArrowArrayStream> OpenQueryStreamAsync(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                QueryCalls++;
                LastSql = sql;
                LastStatementOptions = statementOptions.Clone();
                return Task.FromResult(QueryStream);
            }

            public IArrowArrayStream OpenReadTableStream(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                ReadCalls++;
                LastStatementOptions = statementOptions.Clone();
                return ReadStream;
            }

            public IArrowArrayStream OpenReadPartitionStream(string partitionToken, int? batchSize, CancellationToken cancellationToken)
            {
                ReadPartitionCalls++;
                LastPartitionToken = partitionToken;
                LastPartitionBatchSize = batchSize;
                return PartitionStream;
            }

            public Task<IArrowArrayStream> OpenReadTableStreamAsync(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
            {
                ReadCalls++;
                LastStatementOptions = statementOptions.Clone();
                return Task.FromResult(ReadStream);
            }

            public Task<IArrowArrayStream> OpenReadPartitionStreamAsync(string partitionToken, int? batchSize, CancellationToken cancellationToken)
            {
                ReadPartitionCalls++;
                LastPartitionToken = partitionToken;
                LastPartitionBatchSize = batchSize;
                return Task.FromResult(PartitionStream);
            }

            private static IArrowArrayStream CreateStream(string value)
            {
                var batch = new RecordBatch(
                    StreamSchema,
                    new IArrowArray[]
                    {
                        new StringArray.Builder().Append(value).Build(),
                    },
                    1);

                return new TestArrowArrayStream(StreamSchema, batch);
            }
        }

        private sealed class TestArrowArrayStream : IArrowArrayStream
        {
            private readonly RecordBatch _batch;
            private bool _consumed;

            public TestArrowArrayStream(Schema schema, RecordBatch batch)
            {
                Schema = schema;
                _batch = batch;
            }

            public Schema Schema { get; }

            public void Dispose()
            {
                _batch.Dispose();
            }

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (_consumed)
                {
                    return new ValueTask<RecordBatch?>((RecordBatch?)null);
                }

                _consumed = true;
                return new ValueTask<RecordBatch?>(_batch);
            }
        }
    }
}
