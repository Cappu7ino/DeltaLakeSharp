// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Identity;
using Microsoft.Data.SqlClient;
using Microsoft.DI.DeltaTableService.Client.Models;

namespace Microsoft.DI.DeltaTableService.Client
{
    /// <summary>
    /// Lightweight client for querying Delta tables in a Fabric Lakehouse
    /// through the SQL analytics endpoint (read-only T-SQL over TDS).
    /// </summary>
    /// <remarks>
    /// <para>
    /// This class is <b>internal</b> because it is intended solely for
    /// internal benchmarking — comparing SQL analytics endpoint query
    /// performance against the Arrow Flight path provided by
    /// <see cref="DeltaTableServiceClient"/>.
    /// </para>
    /// <para>
    /// The SQL analytics endpoint is automatically provisioned for every
    /// Fabric Lakehouse and provides read-only T-SQL access to Delta tables
    /// via the standard TDS protocol on port 1433. Authentication is
    /// Microsoft Entra ID only (no SQL logins).
    /// </para>
    /// <para>
    /// Each method opens a fresh <see cref="SqlConnection"/> using token-based
    /// authentication. The <see cref="DefaultAzureCredential"/> is used to
    /// acquire tokens for the <c>https://database.windows.net/.default</c>
    /// scope. The caller must be signed in via <c>az login</c> or have a
    /// managed identity / service principal available in the credential chain.
    /// </para>
    /// </remarks>
    internal sealed class SqlEndpointClient : IDisposable
    {
        /// <summary>
        /// The Azure SQL / Fabric token scope for Entra ID authentication.
        /// </summary>
        private const string DatabaseTokenScope = "https://database.windows.net/.default";

        /// <summary>
        /// Default command timeout in seconds for SQL queries.
        /// Set higher than typical defaults to accommodate Fabric cold-start latency.
        /// </summary>
        private const int DefaultCommandTimeoutSeconds = 120;

        private readonly SqlEndpointConfig _config;
        private readonly TokenCredential _credential;
        private readonly string _connectionString;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlEndpointClient"/> class.
        /// </summary>
        /// <param name="config">
        /// The SQL analytics endpoint connection configuration.
        /// </param>
        /// <param name="credential">
        /// An optional <see cref="TokenCredential"/>. When <c>null</c>,
        /// <see cref="DefaultAzureCredential"/> is used.
        /// </param>
        public SqlEndpointClient(SqlEndpointConfig config, TokenCredential? credential = null)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _credential = credential ?? new DefaultAzureCredential();

            // Build the base connection string without auth — the token is set
            // per-connection via SqlConnection.AccessToken.
            _connectionString = new SqlConnectionStringBuilder
            {
                DataSource = config.Server,
                InitialCatalog = config.Database,
                Encrypt = true,
                TrustServerCertificate = false,
                ConnectTimeout = DefaultCommandTimeoutSeconds,
            }.ConnectionString;
        }

        /// <summary>
        /// Gets the SQL analytics endpoint configuration.
        /// </summary>
        public SqlEndpointConfig Config => _config;

        /// <summary>
        /// Checks connectivity to the SQL analytics endpoint by executing
        /// <c>SELECT 1</c>.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns><c>true</c> if the endpoint is reachable and responsive.</returns>
        public async Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            try
            {
                using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                command.CommandTimeout = DefaultCommandTimeoutSeconds;
                var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return result is int value && value == 1;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Executes a read-only T-SQL query and returns the results as a
        /// <see cref="DataTable"/>.
        /// </summary>
        /// <param name="sql">
        /// The T-SQL query to execute (e.g. <c>"SELECT * FROM dbo.MyTable"</c>).
        /// Only read operations are supported by the SQL analytics endpoint.
        /// </param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>
        /// A <see cref="DataTable"/> containing all result rows and columns.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="sql"/> is null or whitespace.
        /// </exception>
        /// <exception cref="SqlException">
        /// Thrown when the SQL analytics endpoint returns an error.
        /// </exception>
        public async Task<DataTable> ExecuteQueryAsync(string sql, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("SQL query must not be null or empty.", nameof(sql));
            }

            using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.CommandTimeout = DefaultCommandTimeoutSeconds;

            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            var dataTable = new DataTable();
            dataTable.Load(reader);
            return dataTable;
        }

        /// <summary>
        /// Retrieves the column schema of a table by executing
        /// <c>SELECT TOP 0 * FROM [schemaName].[tableName]</c>.
        /// </summary>
        /// <param name="tableName">
        /// The table name (e.g. <c>"MyTable"</c>). Brackets are added
        /// automatically for safe quoting.
        /// </param>
        /// <param name="schemaName">
        /// The SQL schema name. Defaults to <c>"dbo"</c>.
        /// </param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>
        /// A <see cref="DataTable"/> with zero rows but with columns matching
        /// the table schema (including column names and CLR types).
        /// </returns>
        public async Task<DataTable> GetSchemaAsync(
            string tableName,
            string schemaName = "dbo",
            CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentException("Table name must not be null or empty.", nameof(tableName));
            }

            var sql = $"SELECT TOP 0 * FROM [{schemaName}].[{tableName}]";
            return await ExecuteQueryAsync(sql, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes a scalar query and returns the single result value.
        /// Useful for <c>SELECT COUNT(*)</c> or other aggregate queries.
        /// </summary>
        /// <param name="sql">The T-SQL scalar query.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The scalar result, or <c>null</c> if the result set is empty.</returns>
        public async Task<object?> ExecuteScalarAsync(string sql, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("SQL query must not be null or empty.", nameof(sql));
            }

            using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.CommandTimeout = DefaultCommandTimeoutSeconds;
            return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _disposed = true;
        }

        /// <summary>
        /// Opens a new <see cref="SqlConnection"/> with an Entra ID access token.
        /// </summary>
        private async Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
        {
            var tokenRequest = new TokenRequestContext(new[] { DatabaseTokenScope });
            var accessToken = await _credential.GetTokenAsync(tokenRequest, cancellationToken).ConfigureAwait(false);

            var connection = new SqlConnection(_connectionString)
            {
                AccessToken = accessToken.Token
            };

            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return connection;
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(SqlEndpointClient));
            }
        }
    }
}
