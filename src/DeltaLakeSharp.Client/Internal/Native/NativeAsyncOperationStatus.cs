namespace DeltaLakeSharp.Client.Internal.Native
{
    /// <summary>
    /// Stable native async operation status values exposed by the Rust V3 ABI.
    /// </summary>
    /// <remarks>
    /// Numeric values must match ASYNC_OPERATION_* constants in src/DeltaLakeSharp.Server/v3/src/interop/native.rs.
    /// </remarks>
    internal enum NativeAsyncOperationStatus
    {
        Pending = 0,
        Succeeded = 1,
        Failed = 2,
        Cancelled = 3,
    }
}
