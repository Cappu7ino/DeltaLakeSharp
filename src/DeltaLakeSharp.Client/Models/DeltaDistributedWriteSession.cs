using System;
using System.Collections.Generic;

namespace DeltaLakeSharp.Client.Models
{
    /// <summary>
    /// Descriptor for a distributed write run shared by all workers in one activity.
    /// </summary>
    /// <remarks>
    /// The session is returned by <c>BeginDistributedWriteAsync</c> and passed to
    /// worker-side staging and coordinator-side commit APIs. The run ID is an
    /// opaque activity identifier from the SDK perspective; callers are
    /// responsible for generating a globally unique value.
    /// </remarks>
    public sealed class DeltaDistributedWriteSession
    {
        /// <summary>
        /// Initializes a new distributed write session descriptor.
        /// </summary>
        /// <param name="runId">Caller-provided globally unique activity ID.</param>
        /// <param name="tablePath">Target Delta table path.</param>
        /// <param name="mode">Write mode used by the coordinator commit.</param>
        /// <param name="schemaMode">Optional schema evolution mode.</param>
        /// <param name="tableDisposition">Expected target table disposition.</param>
        /// <param name="overwriteScope">Overwrite remove-action scope.</param>
        /// <param name="stagingPrefix">Table-relative staging prefix.</param>
        /// <param name="partitionBy">Optional target Delta partition columns.</param>
        public DeltaDistributedWriteSession(
            Guid runId,
            string tablePath,
            SaveMode mode,
            WriteSchemaMode? schemaMode,
            DistributedWriteTableDisposition tableDisposition,
            DistributedOverwriteScope overwriteScope,
            string stagingPrefix,
            IReadOnlyList<string>? partitionBy = null)
        {
            if (runId == Guid.Empty)
            {
                throw new ArgumentException("Distributed write run ID must be provided.", nameof(runId));
            }

            if (string.IsNullOrWhiteSpace(tablePath))
            {
                throw new ArgumentException("Table path must be provided.", nameof(tablePath));
            }

            if (string.IsNullOrWhiteSpace(stagingPrefix))
            {
                throw new ArgumentException("Staging prefix must be provided.", nameof(stagingPrefix));
            }

            RunId = runId;
            TablePath = tablePath;
            Mode = mode;
            SchemaMode = schemaMode;
            TableDisposition = tableDisposition;
            OverwriteScope = overwriteScope;
            StagingPrefix = stagingPrefix;
            PartitionBy = partitionBy ?? Array.Empty<string>();
        }

        /// <summary>
        /// Gets the caller-provided distributed activity ID shared by all workers.
        /// </summary>
        public Guid RunId { get; }

        /// <summary>
        /// Gets the target Delta table path.
        /// </summary>
        public string TablePath { get; }

        /// <summary>
        /// Gets the write mode used by the coordinator commit.
        /// </summary>
        public SaveMode Mode { get; }

        /// <summary>
        /// Gets optional schema evolution behavior for the coordinator commit.
        /// </summary>
        public WriteSchemaMode? SchemaMode { get; }

        /// <summary>
        /// Gets whether the target table must already exist or may be created.
        /// </summary>
        public DistributedWriteTableDisposition TableDisposition { get; }

        /// <summary>
        /// Gets the overwrite remove-action scope.
        /// </summary>
        public DistributedOverwriteScope OverwriteScope { get; }

        /// <summary>
        /// Gets the table-relative staging prefix used for Add-action artifacts.
        /// </summary>
        public string StagingPrefix { get; }

        /// <summary>
        /// Gets the target Delta partition columns for this run.
        /// </summary>
        public IReadOnlyList<string> PartitionBy { get; }
    }
}
