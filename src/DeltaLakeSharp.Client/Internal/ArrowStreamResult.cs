using System;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace DeltaLakeSharp.Client.Internal
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
