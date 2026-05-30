namespace DeltaLakeSharp.Client.Models
{
    /// <summary>
    /// Specifies how write operations handle schema differences.
    /// </summary>
    public enum WriteSchemaMode
    {
        /// <summary>
        /// Merges the incoming schema into the existing table schema during a write.
        /// </summary>
        Merge,

        /// <summary>
        /// Replaces the existing table schema during an overwrite write.
        /// </summary>
        Overwrite,
    }
}
