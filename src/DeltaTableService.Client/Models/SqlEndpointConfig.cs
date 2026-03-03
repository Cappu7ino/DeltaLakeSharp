// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;

namespace Microsoft.ADMS.Testing.DeltaTableService.Client.Models
{
    /// <summary>
    /// Connection configuration for a Fabric Lakehouse SQL analytics endpoint.
    /// The SQL analytics endpoint provides read-only T-SQL access to Delta tables
    /// via the standard TDS protocol on port 1433.
    /// </summary>
    /// <remarks>
    /// This class is internal because the SQL analytics endpoint query capability
    /// is intended only for internal benchmarking purposes.
    /// </remarks>
    internal sealed class SqlEndpointConfig
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SqlEndpointConfig"/> class.
        /// </summary>
        /// <param name="server">
        /// The SQL analytics endpoint hostname, e.g.
        /// <c>"&lt;guid&gt;.datawarehouse.fabric.microsoft.com"</c>.
        /// Found in the Fabric portal under Lakehouse → Settings → SQL endpoint.
        /// </param>
        /// <param name="database">
        /// The database name, which is the Lakehouse name (not the GUID).
        /// </param>
        public SqlEndpointConfig(string server, string database)
        {
            if (string.IsNullOrWhiteSpace(server))
            {
                throw new ArgumentException("Server must not be null or empty.", nameof(server));
            }

            if (string.IsNullOrWhiteSpace(database))
            {
                throw new ArgumentException("Database must not be null or empty.", nameof(database));
            }

            Server = server;
            Database = database;
        }

        /// <summary>
        /// Gets the SQL analytics endpoint hostname.
        /// </summary>
        public string Server { get; }

        /// <summary>
        /// Gets the database (Lakehouse) name.
        /// </summary>
        public string Database { get; }
    }
}
