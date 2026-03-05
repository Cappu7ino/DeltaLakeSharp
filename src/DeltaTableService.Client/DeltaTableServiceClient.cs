// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Data;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Microsoft.ADMS.Testing.DeltaTableService.Client.Internal;
using Microsoft.ADMS.Testing.DeltaTableService.Client.Models;

namespace Microsoft.ADMS.Testing.DeltaTableService.Client
{
    /// <summary>
    /// Specifies the backend protocol to use for communicating with the Delta Table Service.
    /// </summary>
    public enum ServiceMode
    {
        /// <summary>
        /// V1 backend using PySpark + Delta Lake via Arrow Flight (default).
        /// </summary>
        V1_Spark,

        /// <summary>
        /// V2 backend using DataFusion (Rust engine) + delta-rs via Arrow Flight.
        /// Lightweight alternative to V1 — no JVM or Spark dependency.
        /// Reuses the same Arrow Flight protocol as V1.
        /// </summary>
        V2_DataFusion,

        /// <summary>
        /// V3 backend using a native Rust binary (arrow-flight + DataFusion + delta-rs).
        /// Spawned as a child process instead of Docker container.
        /// Uses the same Arrow Flight protocol as V1/V2.
        /// </summary>
        V3_Rust,
    }

    /// <summary>
    /// High-level client for the Delta Table Service.
    /// Supports V1 (PySpark + Arrow Flight), V2 (DataFusion + Arrow Flight),
    /// and V3 (native Rust + Arrow Flight) backends.
    /// All Arrow and gRPC types are kept internal — callers interact exclusively
    /// with standard .NET types (<see cref="DataTable"/>, dictionaries, etc.)
    /// and the models in <see cref="Models"/>.
    /// </summary>
    public sealed class DeltaTableServiceClient : IDisposable
    {
        private readonly IDeltaTableServiceBackend _backend;

        /// <summary>
        /// Gets the service mode this client is using.
        /// </summary>
        public ServiceMode Mode { get; }

        /// <summary>
        /// Initializes a new client that connects to the server at the given URI
        /// using the V1 PySpark backend (Arrow Flight protocol).
        /// </summary>
        /// <param name="serverUri">
        /// The base URI of the Arrow Flight server (e.g. <c>http://localhost:8815</c>).
        /// </param>
        public DeltaTableServiceClient(Uri serverUri)
            : this(serverUri, ServiceMode.V1_Spark)
        {
        }

        /// <summary>
        /// Initializes a new client that connects to the server at the given URI
        /// using the specified backend protocol.
        /// </summary>
        /// <param name="serverUri">
        /// The base URI of the server.
        /// For V1: Arrow Flight server (e.g. <c>http://localhost:8815</c>).
        /// For V2: Arrow Flight server (e.g. <c>http://localhost:8815</c>).
        /// </param>
        /// <param name="mode">The backend protocol to use.</param>
        public DeltaTableServiceClient(Uri serverUri, ServiceMode mode)
        {
            if (serverUri == null)
            {
                throw new ArgumentNullException(nameof(serverUri));
            }

            Mode = mode;
            _backend = mode switch
            {
                ServiceMode.V1_Spark => new FlightClientWrapper(serverUri),
                ServiceMode.V2_DataFusion => new FlightClientWrapper(serverUri),
                ServiceMode.V3_Rust => new FlightClientWrapper(serverUri),
                _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown service mode"),
            };
        }

        // ------------------------------------------------------------------ //
        //  Health check
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Checks if the server is healthy and responsive.
        /// </summary>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns><c>true</c> if the server reports healthy; otherwise <c>false</c>.</returns>
        public Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default)
        {
            return _backend.HealthCheckAsync(cancellationToken);
        }

        // ------------------------------------------------------------------ //
        //  Read table  ->  IAsyncEnumerable<RecordBatch>
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Reads all rows and columns from a Delta table and streams them as
        /// Arrow <see cref="RecordBatch"/> objects as they arrive from the server.
        /// Use <see cref="ReadStreamExtensions.ToDataTableAsync"/> to materialise
        /// the result as a <see cref="DataTable"/>, or
        /// <see cref="ReadStreamExtensions.ToListAsync"/> to buffer all batches.
        /// </summary>
        /// <param name="path">Path to the Delta table (local or abfss://).</param>
        /// <param name="storageConfig">Optional ABFSS storage credentials.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        public async IAsyncEnumerable<RecordBatch> ReadTableAsync(
            string path,
            StorageConfig storageConfig = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (RecordBatch batch in _backend.ReadTableAsync(path, storageConfig, cancellationToken).ConfigureAwait(false))
            {
                yield return batch;
            }
        }

        // ------------------------------------------------------------------ //
        //  Get schema
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Returns the schema of a Delta table as a <see cref="TableSchema"/>.
        /// </summary>
        /// <param name="path">Path to the Delta table.</param>
        /// <param name="storageConfig">Optional ABFSS storage credentials.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        public Task<TableSchema> GetSchemaAsync(
            string path,
            StorageConfig storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            return _backend.GetSchemaAsync(path, storageConfig, cancellationToken);
        }

        // ------------------------------------------------------------------ //
        //  Execute query (read-oriented SQL: SELECT, SHOW, DESCRIBE, etc.)
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Executes a read-oriented SQL query via GetFlightInfo + DoGet and
        /// streams the result as Arrow <see cref="RecordBatch"/> objects. When
        /// <paramref name="tablePath"/> and <paramref name="tableName"/> are provided,
        /// the server registers the Delta table before executing the query
        /// (required for stateless engines like DataFusion V2). When omitted,
        /// the SQL is executed directly.
        /// </summary>
        /// <param name="sql">The SQL query to execute (SELECT, SHOW, DESCRIBE, etc.).</param>
        /// <param name="tablePath">Optional path to a Delta table to register before executing.</param>
        /// <param name="tableName">Optional logical table name to use in the SQL query.</param>
        /// <param name="storageConfig">Optional ABFSS storage credentials.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        public async IAsyncEnumerable<RecordBatch> ExecuteQueryAsync(
            string sql,
            string tablePath = null,
            string tableName = null,
            StorageConfig storageConfig = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (RecordBatch batch in _backend.ExecuteQueryAsync(sql, tablePath, tableName, storageConfig, cancellationToken).ConfigureAwait(false))
            {
                yield return batch;
            }
        }

        // ------------------------------------------------------------------ //
        //  Create table - overload 1: empty table with schema
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Creates an empty Delta table with the given schema and optional Delta configuration.
        /// </summary>
        /// <remarks>
        /// <para>
        /// When <paramref name="configuration"/> is provided, Delta table configuration properties
        /// are stored in the table metadata. Support varies by backend:
        /// </para>
        /// <list type="bullet">
        /// <item><description>V1 (Spark): Full support for Delta features including column mapping.</description></item>
        /// <item><description>V2 (DataFusion/delta-rs): Configuration is stored in table metadata but advanced
        /// features like column mapping are NOT implemented. The protocol remains at (1, 2) and no column
        /// mapping annotations are added. delta-rs also cannot read column-mapped tables created by Spark.</description></item>
        /// </list>
        /// <para>
        /// For full Delta Lake feature support, use the V1 backend.
        /// </para>
        /// </remarks>
        /// <param name="path">Path where the table will be created.</param>
        /// <param name="schema">The table schema definition.</param>
        /// <param name="configuration">Optional Delta table configuration properties,
        /// e.g. {"delta.columnMapping.mode", "name"}.</param>
        /// <param name="storageConfig">Optional ABFSS storage credentials.</param>
        /// <param name="partitionBy">Optional list of column names to partition the table by. Partition columns must be present in the schema.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        public Task<ExecuteResult> CreateTableAsync(
            string path,
            TableSchema schema,
            Dictionary<string, string> configuration = null,
            StorageConfig storageConfig = null,
            IReadOnlyList<string> partitionBy = null,
            CancellationToken cancellationToken = default)
        {
            return _backend.CreateEmptyTableAsync(path, schema, storageConfig, configuration, partitionBy, cancellationToken);
        }

        // ------------------------------------------------------------------ //
        //  Insert data: IAsyncEnumerable<RecordBatch> (streaming)
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Inserts data from a stream of Arrow <see cref="RecordBatch"/> objects
        /// into a Delta table (creates it if it doesn't exist).
        ///
        /// Each batch is sent individually over the Flight wire, allowing
        /// the server to process them incrementally without materialising the
        /// entire dataset in memory on either side.
        ///
        /// Use <see cref="ArrowConverter"/> to convert common .NET types
        /// (<see cref="DataTable"/>, <c>object[][]</c>, CSV strings) into
        /// <see cref="RecordBatch"/> instances, then wrap them with
        /// <see cref="ArrowConverter.ToAsyncEnumerable"/> before calling
        /// this method.
        /// </summary>
        /// <param name="path">Path to the Delta table.</param>
        /// <param name="schema">The Arrow schema that all batches conform to.</param>
        /// <param name="batches">An async stream of RecordBatches to write.</param>
        /// <param name="mode">Write mode (default <see cref="SaveMode.Overwrite"/>).</param>
        /// <param name="storageConfig">Optional ABFSS storage credentials.</param>
        /// <param name="partitionBy">Optional list of column names to partition the table by. Only applied on the first write (overwrite mode); ignored for appends to existing partitioned tables.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        public Task InsertAsync(
            string path,
            Schema schema,
            IAsyncEnumerable<RecordBatch> batches,
            SaveMode mode = SaveMode.Overwrite,
            StorageConfig storageConfig = null,
            IReadOnlyList<string> partitionBy = null,
            CancellationToken cancellationToken = default)
        {
            string modeString = mode == SaveMode.Append ? "append" : "overwrite";
            return _backend.InsertAsync(path, schema, batches, modeString, storageConfig, partitionBy, cancellationToken);
        }

        // ------------------------------------------------------------------ //
        //  DML: DELETE
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Executes a DELETE statement against a Delta table.
        /// The backend auto-registers the table before executing the SQL.
        /// </summary>
        /// <param name="sql">
        /// The DELETE SQL statement, e.g. <c>DELETE FROM myTable WHERE id = 3</c>.
        /// Must start with "DELETE".
        /// </param>
        /// <param name="tablePath">Path to the Delta table on disk or ABFSS.</param>
        /// <param name="tableName">Logical table name referenced in the SQL statement.</param>
        /// <param name="storageConfig">Optional ABFSS storage credentials.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="sql"/> does not start with "DELETE".
        /// </exception>
        public Task<ExecuteResult> DeleteAsync(
            string sql,
            string tablePath,
            string tableName,
            StorageConfig storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            if (!sql.TrimStart().StartsWith("DELETE", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException("SQL statement must start with DELETE.", nameof(sql));
            }

            return _backend.DeleteAsync(sql, tablePath, tableName, storageConfig, cancellationToken);
        }

        // ------------------------------------------------------------------ //
        //  DML: UPDATE
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Executes an UPDATE statement against a Delta table.
        /// The backend auto-registers the table before executing the SQL.
        /// </summary>
        /// <param name="sql">
        /// The UPDATE SQL statement, e.g. <c>UPDATE myTable SET col = val WHERE ...</c>.
        /// Must start with "UPDATE".
        /// </param>
        /// <param name="tablePath">Path to the Delta table on disk or ABFSS.</param>
        /// <param name="tableName">Logical table name referenced in the SQL statement.</param>
        /// <param name="storageConfig">Optional ABFSS storage credentials.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="sql"/> does not start with "UPDATE".
        /// </exception>
        public Task<ExecuteResult> UpdateAsync(
            string sql,
            string tablePath,
            string tableName,
            StorageConfig storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            if (!sql.TrimStart().StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException("SQL statement must start with UPDATE.", nameof(sql));
            }

            return _backend.UpdateAsync(sql, tablePath, tableName, storageConfig, cancellationToken);
        }

        // ------------------------------------------------------------------ //
        //  DML: MERGE
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Executes a MERGE statement against a Delta table.
        /// The backend auto-registers the table before executing the SQL.
        /// </summary>
        /// <param name="sql">
        /// The MERGE SQL statement. Must start with "MERGE".
        /// </param>
        /// <param name="tablePath">Path to the Delta table on disk or ABFSS.</param>
        /// <param name="tableName">Logical table name referenced in the SQL statement.</param>
        /// <param name="storageConfig">Optional ABFSS storage credentials.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="sql"/> does not start with "MERGE".
        /// </exception>
        public Task<ExecuteResult> MergeAsync(
            string sql,
            string tablePath,
            string tableName,
            StorageConfig storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            if (!sql.TrimStart().StartsWith("MERGE", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException("SQL statement must start with MERGE.", nameof(sql));
            }

            return _backend.MergeAsync(sql, tablePath, tableName, storageConfig, cancellationToken);
        }

        // ------------------------------------------------------------------ //
        //  DML: MERGE with streamed source data
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Streams source data to the server via Arrow Flight DoPut and performs
        /// a Delta MERGE operation on the target table. Unlike
        /// <see cref="MergeAsync(string, string, string, StorageConfig, CancellationToken)"/>
        /// which requires the source data to already exist as a registered table,
        /// this method sends the source data directly from the client.
        ///
        /// Each batch is sent individually over the Flight wire, allowing the
        /// server to process them incrementally without materialising the entire
        /// dataset in memory.
        /// </summary>
        /// <param name="path">Path to the target Delta table.</param>
        /// <param name="schema">The Arrow schema that all source batches conform to.</param>
        /// <param name="batches">An async stream of source RecordBatches.</param>
        /// <param name="mergeOptions">
        /// Merge predicate and clause configuration (update/insert/delete rules).
        /// </param>
        /// <param name="storageConfig">Optional ABFSS storage credentials.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>
        /// An <see cref="ExecuteResult"/> containing success status and merge
        /// metrics (rows inserted, updated, deleted, etc.).
        /// </returns>
        public Task<ExecuteResult> MergeDataAsync(
            string path,
            Schema schema,
            IAsyncEnumerable<RecordBatch> batches,
            MergeOptions mergeOptions,
            StorageConfig storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            if (path == null)
            {
                throw new ArgumentNullException(nameof(path));
            }

            if (schema == null)
            {
                throw new ArgumentNullException(nameof(schema));
            }

            if (batches == null)
            {
                throw new ArgumentNullException(nameof(batches));
            }

            if (mergeOptions == null)
            {
                throw new ArgumentNullException(nameof(mergeOptions));
            }

            return _backend.MergeDataAsync(path, schema, batches, mergeOptions, storageConfig, cancellationToken);
        }

        // ------------------------------------------------------------------ //
        //  Protocol upgrade
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Upgrades the Delta protocol version of an existing table. When
        /// <paramref name="readerFeatures"/> or <paramref name="writerFeatures"/>
        /// are provided, the corresponding table features are enabled (requires
        /// protocol reader v3 / writer v7 or higher).
        ///
        /// Protocol upgrades are <b>irreversible</b> — a table's protocol
        /// version can only be increased, never decreased.
        /// </summary>
        /// <param name="tablePath">Path to the Delta table (local or abfss://).</param>
        /// <param name="readerVersion">Target minimum reader version (1–3).</param>
        /// <param name="writerVersion">Target minimum writer version (1–7).</param>
        /// <param name="readerFeatures">Optional reader features to enable (e.g. "timestampNtz").</param>
        /// <param name="writerFeatures">Optional writer features to enable (e.g. "appendOnly", "timestampNtz").</param>
        /// <param name="storageConfig">Optional ABFSS storage credentials.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        public Task<ExecuteResult> UpgradeTableProtocolAsync(
            string tablePath,
            int readerVersion,
            int writerVersion,
            IReadOnlyList<string> readerFeatures = null,
            IReadOnlyList<string> writerFeatures = null,
            StorageConfig storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            if (tablePath == null)
            {
                throw new ArgumentNullException(nameof(tablePath));
            }

            if (readerVersion < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(readerVersion), readerVersion, "Reader version must be >= 1.");
            }

            if (writerVersion < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(writerVersion), writerVersion, "Writer version must be >= 1.");
            }

            return _backend.UpgradeTableProtocolAsync(tablePath, readerVersion, writerVersion,
                readerFeatures, writerFeatures, storageConfig, cancellationToken);
        }

        // ------------------------------------------------------------------ //
        //  IDisposable
        // ------------------------------------------------------------------ //

        /// <inheritdoc />
        public void Dispose()
        {
            _backend?.Dispose();
        }
    }
}
