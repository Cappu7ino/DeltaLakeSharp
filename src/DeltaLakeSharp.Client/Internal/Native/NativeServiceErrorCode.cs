// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace DeltaLakeSharp.Client.Internal.Native
{
    /// <summary>
    /// Stable native service error codes exposed by the Rust V3 ABI.
    /// </summary>
    /// <remarks>
    /// Numeric values must match ServiceErrorCode in src/DeltaLakeSharp.Server/v3/src/error.rs.
    /// </remarks>
    internal enum NativeServiceErrorCode
    {
        Ok = 0,
        InvalidRequest = 1,
        TableNotFound = 2,
        Delta = 3,
        DataFusion = 4,
        Arrow = 5,
        Json = 6,
        Internal = 7,
        Cancelled = 8,
    }
}
