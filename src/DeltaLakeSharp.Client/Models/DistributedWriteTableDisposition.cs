namespace DeltaLakeSharp.Client.Models
{
    /// <summary>
    /// Describes how a distributed write run expects the target Delta table to exist.
    /// </summary>
    /// <remarks>
    /// The disposition is evaluated by the coordinator when the staged Add
    /// artifacts are committed to the Delta log.
    /// </remarks>
    public enum DistributedWriteTableDisposition
    {
        /// <summary>
        /// The target Delta table must already exist.
        /// </summary>
        ExistingTable,

        /// <summary>
        /// The target Delta table may be created if it does not already exist.
        /// </summary>
        CreateIfMissing,

        /// <summary>
        /// The target Delta table may be created or replaced atomically by the coordinator.
        /// </summary>
        CreateOrReplace,
    }
}
