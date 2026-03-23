// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Flight.Client;

namespace Microsoft.DI.DeltaTableService.Client.Internal
{
    internal sealed class FlightArrowArrayStream : IArrowArrayStream
    {
        private readonly FlightRecordBatchStreamingCall _call;
        private bool _disposed;

        public FlightArrowArrayStream(Schema schema, FlightRecordBatchStreamingCall call)
        {
            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _call = call ?? throw new ArgumentNullException(nameof(call));
        }

        public Schema Schema { get; }

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(FlightArrowArrayStream));
            }

            return await _call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false)
                ? _call.ResponseStream.Current
                : null;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _call.Dispose();
            _disposed = true;
        }
    }
}
