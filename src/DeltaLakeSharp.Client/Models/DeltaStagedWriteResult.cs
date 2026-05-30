using System;

namespace DeltaLakeSharp.Client.Models
{
    /// <summary>
    /// Summary of one worker staging operation within a distributed write run.
    /// </summary>
    /// <remarks>
    /// The result reports safe aggregate counts only. It does not expose raw
    /// Delta Add actions or staged artifact payloads to keep the distributed
    /// write contract backend-owned.
    /// </remarks>
    public sealed class DeltaStagedWriteResult
    {
        /// <summary>
        /// Initializes a new staged write result.
        /// </summary>
        /// <param name="runId">Distributed activity ID for the staging operation.</param>
        /// <param name="stagingPrefix">Table-relative staging prefix used by the run.</param>
        /// <param name="artifactCount">Number of staged Add-action artifacts produced.</param>
        /// <param name="addedFileCount">Number of Delta data files staged by the worker.</param>
        /// <param name="totalDataFileBytes">Total size of staged Delta data files, in bytes.</param>
        public DeltaStagedWriteResult(
            Guid runId,
            string stagingPrefix,
            int artifactCount,
            long addedFileCount,
            long totalDataFileBytes)
        {
            if (runId == Guid.Empty)
            {
                throw new ArgumentException("Distributed write run ID must be provided.", nameof(runId));
            }

            if (string.IsNullOrWhiteSpace(stagingPrefix))
            {
                throw new ArgumentException("Staging prefix must be provided.", nameof(stagingPrefix));
            }

            if (artifactCount < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(artifactCount), artifactCount, "Artifact count must be non-negative.");
            }

            if (addedFileCount < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(addedFileCount), addedFileCount, "Added file count must be non-negative.");
            }

            if (totalDataFileBytes < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(totalDataFileBytes), totalDataFileBytes, "Total data file bytes must be non-negative.");
            }

            RunId = runId;
            StagingPrefix = stagingPrefix;
            ArtifactCount = artifactCount;
            AddedFileCount = addedFileCount;
            TotalDataFileBytes = totalDataFileBytes;
        }

        /// <summary>
        /// Gets the distributed activity ID for the staging operation.
        /// </summary>
        public Guid RunId { get; }

        /// <summary>
        /// Gets the table-relative staging prefix used by the run.
        /// </summary>
        public string StagingPrefix { get; }

        /// <summary>
        /// Gets the number of staged Add-action artifacts produced by this worker.
        /// </summary>
        public int ArtifactCount { get; }

        /// <summary>
        /// Gets the number of Delta data files staged by this worker.
        /// </summary>
        public long AddedFileCount { get; }

        /// <summary>
        /// Gets the total size of staged Delta data files, in bytes.
        /// </summary>
        public long TotalDataFileBytes { get; }
    }
}
