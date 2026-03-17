// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Microsoft.DI.DeltaTableService.Client.Internal;

namespace Microsoft.DI.DeltaTableService.Client
{
    /// <summary>
    /// Extension methods for consuming <see cref="IAsyncEnumerable{RecordBatch}"/>
    /// streams returned by <see cref="DeltaTableServiceClient.ReadTableAsync"/>.
    /// </summary>
    public static class ReadStreamExtensions
    {
        /// <summary>
        /// Buffers all <see cref="RecordBatch"/> items from the asynchronous stream
        /// into a <see cref="List{RecordBatch}"/>.
        /// This is a convenience method for callers who prefer the fully-buffered
        /// pattern over incremental streaming.
        /// </summary>
        /// <param name="source">The asynchronous stream of record batches.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A list containing all record batches from the stream.</returns>
        public static async Task<List<RecordBatch>> ToListAsync(
            this IAsyncEnumerable<RecordBatch> source,
            CancellationToken cancellationToken = default)
        {
            var list = new List<RecordBatch>();
            await foreach (RecordBatch batch in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                list.Add(batch);
            }
            return list;
        }

        /// <summary>
        /// Materialises all <see cref="RecordBatch"/> items from the asynchronous
        /// stream into a single <see cref="DataTable"/>.
        /// This is the replacement for the old <c>ReadTableAsync</c> overload that
        /// returned <c>Task&lt;DataTable&gt;</c>.
        /// </summary>
        /// <param name="source">The asynchronous stream of record batches.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A <see cref="DataTable"/> containing all rows from the stream.</returns>
        public static async Task<DataTable> ToDataTableAsync(
            this IAsyncEnumerable<RecordBatch> source,
            CancellationToken cancellationToken = default)
        {
            List<RecordBatch> batches = await source.ToListAsync(cancellationToken).ConfigureAwait(false);
            return ArrowConverter.ToDataTable(batches);
        }
    }
}
