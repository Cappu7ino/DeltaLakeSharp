// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Microsoft.ADMS.Testing.DeltaTableService.Client.Models;

namespace Microsoft.ADMS.Testing.DeltaTableService.Client.Internal
{
    /// <summary>
    /// Common interface for Delta Table Service backends.
    /// V1 uses Arrow Flight with PySpark, V2 uses Arrow Flight with DataFusion.
    /// </summary>
    internal interface IDeltaTableServiceBackend : IDisposable
    {
        /// <summary>
        /// Performs a health check against the server.
        /// </summary>
        Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the schema of a Delta table, optionally at a specific version.
        /// </summary>
        Task<TableSchema> GetSchemaAsync(
            string path,
            StorageConfig? storageConfig = null,
            long? version = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Reads rows from a Delta table, streaming Arrow RecordBatches as
        /// they arrive. Optionally limits the number of rows returned and/or
        /// reads a specific historical version.
        /// </summary>
        IAsyncEnumerable<RecordBatch> ReadTableAsync(
            string path,
            StorageConfig? storageConfig = null,
            long? numRows = null,
            long? version = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Reads Change Data Feed rows from a Delta table, streaming Arrow
        /// RecordBatches for the requested version range.
        /// </summary>
        IAsyncEnumerable<RecordBatch> ReadChangeDataAsync(
            string path,
            long startingVersion,
            long? endingVersion = null,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Executes a read-oriented SQL query (SELECT, SHOW, DESCRIBE, etc.)
        /// via GetFlightInfo + DoGet, returning the result as a stream of Arrow
        /// RecordBatches. When <paramref name="tablePath"/> and
        /// <paramref name="tableName"/> are provided, the server registers the
        /// Delta table before executing the query (required for stateless
        /// engines like DataFusion). When omitted, the SQL is executed directly.
        /// Optionally reads a specific historical version of the table.
        /// </summary>
        IAsyncEnumerable<RecordBatch> ExecuteQueryAsync(
            string sql,
            string? tablePath = null,
            string? tableName = null,
            StorageConfig? storageConfig = null,
            long? version = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Creates an empty Delta table with the specified schema.
        /// </summary>
        /// <param name="path">The path to the Delta table.</param>
        /// <param name="schema">The table schema.</param>
        /// <param name="storageConfig">Optional Azure storage configuration.</param>
        /// <param name="configuration">Optional Delta table configuration properties,
        /// e.g. {"delta.columnMapping.mode", "name"}.</param>
        /// <param name="partitionBy">Optional list of column names to partition the table by.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        Task<ExecuteResult> CreateEmptyTableAsync(
            string path,
            TableSchema schema,
            StorageConfig? storageConfig = null,
            Dictionary<string, string>? configuration = null,
            IReadOnlyList<string>? partitionBy = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Writes a stream of Arrow RecordBatches to a Delta table.
        /// Each batch is sent individually over the Flight wire, avoiding
        /// full materialisation of the dataset in the client.
        /// </summary>
        /// <param name="path">Path to the Delta table.</param>
        /// <param name="schema">The Arrow schema for the batches.</param>
        /// <param name="batches">An async stream of RecordBatches to write.</param>
        /// <param name="mode">Write mode ('overwrite' or 'append').</param>
        /// <param name="storageConfig">Optional ABFSS storage credentials.</param>
        /// <param name="partitionBy">Optional list of column names to partition the table by.
        /// Applied when the write creates the table, and otherwise validated
        /// against the existing table metadata.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        Task InsertAsync(
            string path,
            Schema schema,
            IAsyncEnumerable<RecordBatch> batches,
            string mode = "overwrite",
            WriteSchemaMode? schemaMode = null,
            StorageConfig? storageConfig = null,
            IReadOnlyList<string>? partitionBy = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Executes a DELETE DML statement against a Delta table.
        /// The backend auto-registers the table before executing the SQL.
        /// </summary>
        /// <param name="sql">The DELETE SQL statement.</param>
        /// <param name="tablePath">Path to the Delta table.</param>
        /// <param name="tableName">Logical table name used in the SQL statement.</param>
        /// <param name="storageConfig">Optional Azure storage configuration.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        Task<ExecuteResult> DeleteAsync(
            string sql,
            string tablePath,
            string tableName,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Executes an UPDATE DML statement against a Delta table.
        /// The backend auto-registers the table before executing the SQL.
        /// </summary>
        /// <param name="sql">The UPDATE SQL statement.</param>
        /// <param name="tablePath">Path to the Delta table.</param>
        /// <param name="tableName">Logical table name used in the SQL statement.</param>
        /// <param name="storageConfig">Optional Azure storage configuration.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        Task<ExecuteResult> UpdateAsync(
            string sql,
            string tablePath,
            string tableName,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Executes a MERGE DML statement against a Delta table.
        /// The backend auto-registers the table before executing the SQL.
        /// </summary>
        /// <param name="sql">The MERGE SQL statement.</param>
        /// <param name="tablePath">Path to the Delta table.</param>
        /// <param name="tableName">Logical table name used in the SQL statement.</param>
        /// <param name="storageConfig">Optional Azure storage configuration.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        Task<ExecuteResult> MergeAsync(
            string sql,
            string tablePath,
            string tableName,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Streams source data to the server via DoPut and performs a Delta
        /// MERGE operation on the server side. The merge semantics are
        /// controlled by <paramref name="mergeOptions"/>.
        /// </summary>
        /// <param name="path">Path to the target Delta table.</param>
        /// <param name="schema">The Arrow schema for the source batches.</param>
        /// <param name="batches">An async stream of source RecordBatches.</param>
        /// <param name="mergeOptions">Merge predicate and clause configuration.</param>
        /// <param name="storageConfig">Optional ABFSS storage credentials.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>
        /// An <see cref="ExecuteResult"/> containing success status and merge
        /// metrics (rows inserted, updated, deleted, etc.) in the
        /// <see cref="ExecuteResult.Result"/> field.
        /// </returns>
        Task<ExecuteResult> MergeDataAsync(
            string path,
            Schema schema,
            IAsyncEnumerable<RecordBatch> batches,
            MergeOptions mergeOptions,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Upgrades the Delta protocol version of an existing table.
        /// When <paramref name="readerFeatures"/> or <paramref name="writerFeatures"/>
        /// are provided, the corresponding table features are enabled (requires
        /// protocol reader v3 / writer v7 or higher).
        /// </summary>
        /// <param name="path">Path to the Delta table.</param>
        /// <param name="readerVersion">Target minimum reader version (1–3).</param>
        /// <param name="writerVersion">Target minimum writer version (1–7).</param>
        /// <param name="readerFeatures">Optional reader features to enable (e.g. "timestampNtz").</param>
        /// <param name="writerFeatures">Optional writer features to enable (e.g. "appendOnly").</param>
        /// <param name="storageConfig">Optional Azure storage configuration.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        Task<ExecuteResult> UpgradeTableProtocolAsync(
            string path,
            int readerVersion,
            int writerVersion,
            IReadOnlyList<string>? readerFeatures = null,
            IReadOnlyList<string>? writerFeatures = null,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default);
    }
}
