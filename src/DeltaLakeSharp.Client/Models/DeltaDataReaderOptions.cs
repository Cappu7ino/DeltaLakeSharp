// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace DeltaLakeSharp.Client.Models
{
    /// <summary>
    /// Configures row-oriented <see cref="System.Data.Common.DbDataReader"/>
    /// behavior for Delta table reads.
    /// </summary>
    public sealed class DeltaDataReaderOptions
    {
        /// <summary>
        /// Gets or sets how decimal columns are surfaced to callers.
        /// Defaults to <see cref="DeltaDataReaderDecimalBehavior.UseSqlDecimal"/>.
        /// </summary>
        public DeltaDataReaderDecimalBehavior DecimalBehavior { get; set; } = DeltaDataReaderDecimalBehavior.UseSqlDecimal;
    }
}
