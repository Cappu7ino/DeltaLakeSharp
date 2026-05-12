// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Apache.Arrow.Adbc;

namespace DeltaLakeSharp.Adbc
{
    /// <summary>
    /// Represents a parsed, path-scoped Delta database configuration for the ADBC driver.
    /// </summary>
    public sealed class DeltaAdbcDatabase : AdbcDatabase
    {
        private readonly DeltaAdbcConnectOptions _options;

        internal DeltaAdbcDatabase(DeltaAdbcConnectOptions options)
        {
            _options = options;
        }

        internal DeltaAdbcConnectOptions Options => _options;

        /// <summary>
        /// Creates a read-only connection over the configured Delta table path.
        /// </summary>
        public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options)
        {
            if (options == null || options.Count == 0)
            {
                return new DeltaAdbcConnection(_options);
            }

            var merged = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (KeyValuePair<string, string> pair in _options.ToParameterDictionary())
            {
                merged[pair.Key] = pair.Value;
            }

            foreach (KeyValuePair<string, string> pair in options)
            {
                merged[pair.Key] = pair.Value;
            }

            return new DeltaAdbcConnection(DeltaAdbcConnectOptions.Parse(merged));
        }
    }
}
