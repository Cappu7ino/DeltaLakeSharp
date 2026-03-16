// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace Microsoft.ADMS.Testing.DeltaTableService.Client.Models
{
    /// <summary>
    /// Specifies how write operations handle schema differences.
    /// </summary>
    public enum WriteSchemaMode
    {
        /// <summary>
        /// Replaces the existing table schema during an overwrite write.
        /// </summary>
        Overwrite,
    }
}
