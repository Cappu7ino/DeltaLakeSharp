namespace DeltaLakeSharp.Client.Models
{
    /// <summary>
    /// Describes the remove-action scope used by a distributed overwrite commit.
    /// </summary>
    /// <remarks>
    /// Append commits ignore this value. Overwrite commits use it to determine
    /// which active files should receive Delta remove actions before the staged
    /// Add actions are committed.
    /// </remarks>
    public enum DistributedOverwriteScope
    {
        /// <summary>
        /// Replace all active files in the table.
        /// </summary>
        FullTable,

        /// <summary>
        /// Replace only partitions touched by the staged Add actions.
        /// </summary>
        TouchedPartitions,
    }
}
