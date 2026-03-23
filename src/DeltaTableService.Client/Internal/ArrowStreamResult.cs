// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace Microsoft.DI.DeltaTableService.Client.Internal
{
    internal sealed class ArrowStreamResult : IDisposable
    {
        public ArrowStreamResult(Schema schema, IArrowArrayStream stream)
        {
            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            Stream = stream ?? throw new ArgumentNullException(nameof(stream));
        }

        public Schema Schema { get; }

        public IArrowArrayStream Stream { get; }

        public void Dispose()
        {
            Stream.Dispose();
        }
    }
}
