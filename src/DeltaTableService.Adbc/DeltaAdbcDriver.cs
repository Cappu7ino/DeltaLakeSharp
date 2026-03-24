// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System.Collections.Generic;
using Apache.Arrow.Adbc;

namespace Microsoft.DI.DeltaTableService.Adbc
{
    /// <summary>
    /// Read-only ADBC driver that exposes a single Delta table path to .NET consumers.
    /// </summary>
    public sealed class DeltaAdbcDriver : AdbcDriver
    {
        /// <summary>
        /// Opens a path-scoped Delta database using the provided ADBC driver options.
        /// </summary>
        public override AdbcDatabase Open(IReadOnlyDictionary<string, string> parameters)
        {
            DeltaAdbcConnectOptions options = DeltaAdbcConnectOptions.Parse(parameters);
            return new DeltaAdbcDatabase(options);
        }
    }
}
