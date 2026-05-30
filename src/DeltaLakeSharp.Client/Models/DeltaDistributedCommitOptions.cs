namespace DeltaLakeSharp.Client.Models
{
    /// <summary>
    /// Coordinator options for committing a distributed write run.
    /// </summary>
    /// <remarks>
    /// These options are used only by the coordinator. Worker staging is
    /// controlled by <see cref="DeltaDistributedWriteOptions"/> and the
    /// distributed write session returned by the begin step.
    /// </remarks>
    public sealed class DeltaDistributedCommitOptions
    {
        /// <summary>
        /// Gets or sets an expected table version used as an optimistic guard before commit.
        /// </summary>
        public long? ExpectedVersion { get; set; }

        /// <summary>
        /// Gets or sets whether committed staging artifacts should be deleted after a successful commit.
        /// </summary>
        public bool CleanupStagingArtifacts { get; set; } = true;
    }
}
