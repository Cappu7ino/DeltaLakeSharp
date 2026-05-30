using System;
using System.Collections.Generic;

namespace DeltaLakeSharp.Client.Models
{
    /// <summary>
    /// Describes a distributed write run that will be staged by one or more workers
    /// and committed by a single coordinator.
    /// </summary>
    /// <remarks>
    /// Distributed writes are a V3 native Rust capability. The caller supplies a
    /// globally unique <see cref="RunId"/> and shares it with every worker that
    /// belongs to the same activity. The SDK validates that the value is present,
    /// but the caller owns global uniqueness.
    /// </remarks>
    public sealed class DeltaDistributedWriteOptions
    {
        /// <summary>
        /// Gets or sets the caller-provided globally unique distributed activity identifier.
        /// The caller is responsible for ensuring uniqueness across the distributed activity.
        /// </summary>
        public Guid RunId { get; set; }

        /// <summary>
        /// Gets or sets how staged files are committed into the target table.
        /// </summary>
        public SaveMode Mode { get; set; } = SaveMode.Append;

        /// <summary>
        /// Gets or sets optional schema evolution behavior for the coordinator commit.
        /// </summary>
        public WriteSchemaMode? SchemaMode { get; set; }

        /// <summary>
        /// Gets or sets whether the target table must already exist or may be created.
        /// </summary>
        public DistributedWriteTableDisposition TableDisposition { get; set; } = DistributedWriteTableDisposition.ExistingTable;

        /// <summary>
        /// Gets or sets overwrite remove-action scope when <see cref="Mode"/> is <see cref="SaveMode.Overwrite"/>.
        /// </summary>
        public DistributedOverwriteScope OverwriteScope { get; set; } = DistributedOverwriteScope.FullTable;

        /// <summary>
        /// Gets or sets the table schema required for create-table or schema-evolution runs.
        /// </summary>
        public TableSchema? TableSchema { get; set; }

        /// <summary>
        /// Gets or sets table configuration properties used when creating or replacing table metadata.
        /// </summary>
        public IReadOnlyDictionary<string, string>? Configuration { get; set; }

        /// <summary>
        /// Gets or sets target Delta partition columns.
        /// </summary>
        public IReadOnlyList<string>? PartitionBy { get; set; }

        /// <summary>
        /// Gets or sets the table-relative staging prefix. Defaults to <c>_staging</c>.
        /// </summary>
        public string? StagingPrefix { get; set; }

        /// <summary>
        /// Gets or sets a maximum writer buffer size before a worker flushes staged Add actions.
        /// </summary>
        public long? MaxBufferedBytes { get; set; }

        /// <summary>
        /// Gets or sets a maximum number of buffered record batches before a worker flushes staged Add actions.
        /// </summary>
        public int? MaxBufferedRecordBatches { get; set; }
    }
}
