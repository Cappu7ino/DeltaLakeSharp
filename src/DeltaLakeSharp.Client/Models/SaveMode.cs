// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace DeltaLakeSharp.Client.Models
{
    /// <summary>
    /// Specifies how data is written to a Delta table.
    /// </summary>
    public enum SaveMode
    {
        /// <summary>
        /// Replaces the existing table data with the new data.
        /// </summary>
        Overwrite,

        /// <summary>
        /// Adds the new data to the existing table without removing existing rows.
        /// </summary>
        Append,
    }
}
