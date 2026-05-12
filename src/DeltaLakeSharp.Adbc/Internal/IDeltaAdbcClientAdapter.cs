// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DeltaLakeSharp.Client.Models;

namespace DeltaLakeSharp.Adbc.Internal
{
    internal interface IDeltaAdbcClientAdapter : IDisposable
    {
        Task<IReadOnlyList<DeltaReadPartition>> GetReadPartitionsAsync(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        IReadOnlyList<DeltaReadPartition> GetReadPartitions(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        Task<IArrowArrayStream> OpenReadTableStreamAsync(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        IArrowArrayStream OpenReadTableStream(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        Task<IArrowArrayStream> OpenReadPartitionStreamAsync(string partitionToken, int? batchSize, CancellationToken cancellationToken);

        IArrowArrayStream OpenReadPartitionStream(string partitionToken, int? batchSize, CancellationToken cancellationToken);

        Task<IArrowArrayStream> OpenQueryStreamAsync(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        IArrowArrayStream OpenQueryStream(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        Task<IArrowArrayStream> OpenChangeDataStreamAsync(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        IArrowArrayStream OpenChangeDataStream(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        Task<IArrowArrayStream> OpenChangeDataQueryStreamAsync(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        IArrowArrayStream OpenChangeDataQueryStream(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        Task<Schema> GetSchemaAsync(DeltaAdbcStatementOptions? statementOptions, CancellationToken cancellationToken);

        Schema GetSchema(DeltaAdbcStatementOptions? statementOptions, CancellationToken cancellationToken);
    }
}
