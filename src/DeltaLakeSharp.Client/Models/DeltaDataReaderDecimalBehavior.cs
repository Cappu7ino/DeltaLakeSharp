namespace DeltaLakeSharp.Client.Models
{
    /// <summary>
    /// Controls how Arrow decimal values are exposed through the row-based
    /// <see cref="System.Data.Common.DbDataReader"/> APIs.
    /// </summary>
    public enum DeltaDataReaderDecimalBehavior
    {
        /// <summary>
        /// Returns decimal values as <see cref="System.Data.SqlTypes.SqlDecimal"/>.
        /// This is the default because Delta decimals support precision up to 38.
        /// </summary>
        UseSqlDecimal,

        /// <summary>
        /// Returns decimal values as <see cref="decimal"/> when possible.
        /// Values that do not fit in <see cref="decimal"/> follow the configured
        /// overflow handling behavior.
        /// </summary>
        UseDecimal,

        /// <summary>
        /// Returns decimal values as <see cref="decimal"/> when possible and as
        /// strings when they overflow <see cref="decimal"/>.
        /// </summary>
        OverflowDecimalAsString,

        /// <summary>
        /// Throws when a decimal value cannot be represented as
        /// <see cref="decimal"/>.
        /// </summary>
        ThrowOnOverflow,
    }
}
