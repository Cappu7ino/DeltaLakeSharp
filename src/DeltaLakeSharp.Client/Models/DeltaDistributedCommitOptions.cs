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
        /// Gets or sets whether committed staging artifacts should be deleted after a successful commit.
        /// </summary>
        public bool CleanupStagingArtifacts { get; set; } = true;

        /// <summary>
        /// Gets or sets whether the coordinator should re-stat each staged data file before commit.
        /// </summary>
        /// <remarks>
        /// Defaults to <c>false</c> so coordinator commits can stay metadata-bound.
        /// Worker staging still verifies newly written files before publishing Add artifacts.
        /// </remarks>
        public bool ValidateStagedDataFiles { get; set; } = false;
    }
}
