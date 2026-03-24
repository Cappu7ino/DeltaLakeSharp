// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Microsoft.DI.DeltaTableService.Client;
using Microsoft.DI.DeltaTableService.Client.Models;

namespace Microsoft.DI.DeltaTableService.Adbc.Internal
{
    internal sealed class DeltaAdbcClientAdapter : IDeltaAdbcClientAdapter
    {
        private readonly DeltaTableServiceClient _client;

        public DeltaAdbcClientAdapter(DeltaAdbcConnectOptions options)
        {
            Options = options;
            _client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
        }

        public DeltaAdbcConnectOptions Options { get; }

        public Task<IArrowArrayStream> OpenReadTableStreamAsync(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
        {
            return _client.ReadTableAsArrowStreamAsync(
                Options.TableUri,
                genericStorageOptions: Options.StorageOptions ?? null,
                numRows: statementOptions.MaxRows,
                batchSize: statementOptions.BatchSize,
                version: statementOptions.Version,
                cancellationToken: cancellationToken);
        }

        public IArrowArrayStream OpenReadTableStream(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
        {
            return RunSynchronously(OpenReadTableStreamAsync(statementOptions, cancellationToken));
        }

        public Task<IArrowArrayStream> OpenQueryStreamAsync(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
        {
            return _client.ExecuteQueryAsArrowStreamAsync(
                sql,
                tablePath: Options.TableUri,
                tableName: DeltaAdbcConnectOptions.LogicalTableName,
                genericStorageOptions: Options.StorageOptions ?? null,
                batchSize: statementOptions.BatchSize,
                version: statementOptions.Version,
                cancellationToken: cancellationToken);
        }

        public Task<IArrowArrayStream> OpenChangeDataStreamAsync(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
        {
            return _client.ReadChangeDataAsArrowStreamAsync(
                Options.TableUri,
                statementOptions.CdfStartingVersion!.Value,
                statementOptions.CdfEndingVersion,
                genericStorageOptions: Options.StorageOptions,
                cancellationToken: cancellationToken);
        }

        public IArrowArrayStream OpenChangeDataStream(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
        {
            return RunSynchronously(OpenChangeDataStreamAsync(statementOptions, cancellationToken));
        }

        public Task<IArrowArrayStream> OpenChangeDataQueryStreamAsync(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
        {
            return _client.ExecuteChangeDataQueryAsArrowStreamAsync(
                sql,
                Options.TableUri,
                statementOptions.CdfStartingVersion!.Value,
                statementOptions.CdfEndingVersion,
                genericStorageOptions: Options.StorageOptions,
                cancellationToken: cancellationToken);
        }

        public IArrowArrayStream OpenChangeDataQueryStream(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
        {
            return RunSynchronously(OpenChangeDataQueryStreamAsync(sql, statementOptions, cancellationToken));
        }

        public IArrowArrayStream OpenQueryStream(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken)
        {
            return RunSynchronously(OpenQueryStreamAsync(sql, statementOptions, cancellationToken));
        }

        public async Task<Schema> GetSchemaAsync(CancellationToken cancellationToken)
        {
            IArrowArrayStream streamResult = await OpenReadTableStreamAsync(new DeltaAdbcStatementOptions(), cancellationToken).ConfigureAwait(false);
            try
            {
                return streamResult.Schema;
            }
            finally
            {
                streamResult.Dispose();
            }
        }

        public Schema GetSchema(CancellationToken cancellationToken)
        {
            return RunSynchronously(GetSchemaAsync(cancellationToken));
        }

        public void Dispose()
        {
            _client.Dispose();
        }

        private static T RunSynchronously<T>(Task<T> operation)
        {
            // The public .NET ADBC driver surface is synchronous here, so this is the single sync-over-async bridge for the adapter.
            return operation.ConfigureAwait(false).GetAwaiter().GetResult();
        }
    }
}
