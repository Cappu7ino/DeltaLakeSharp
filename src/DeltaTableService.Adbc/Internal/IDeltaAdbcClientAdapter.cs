// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Threading.Tasks;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace Microsoft.DI.DeltaTableService.Adbc.Internal
{
    internal interface IDeltaAdbcClientAdapter : IDisposable
    {
        Task<IArrowArrayStream> OpenReadTableStreamAsync(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        IArrowArrayStream OpenReadTableStream(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        Task<IArrowArrayStream> OpenQueryStreamAsync(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        IArrowArrayStream OpenQueryStream(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        Task<IArrowArrayStream> OpenChangeDataStreamAsync(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        IArrowArrayStream OpenChangeDataStream(DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        Task<IArrowArrayStream> OpenChangeDataQueryStreamAsync(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        IArrowArrayStream OpenChangeDataQueryStream(string sql, DeltaAdbcStatementOptions statementOptions, CancellationToken cancellationToken);

        Task<Schema> GetSchemaAsync(CancellationToken cancellationToken);

        Schema GetSchema(CancellationToken cancellationToken);
    }
}
