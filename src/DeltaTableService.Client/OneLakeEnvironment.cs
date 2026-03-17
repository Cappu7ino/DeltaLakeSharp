// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace Microsoft.DI.DeltaTableService.Client
{
    /// <summary>
    /// Identifies the OneLake DFS environment to use when acquiring SAS tokens.
    /// </summary>
    public enum OneLakeEnvironment
    {
        /// <summary>
        /// Production OneLake (onelake.dfs.fabric.microsoft.com, account "onelake").
        /// </summary>
        Production,

        /// <summary>
        /// Microsoft-internal test environment (msit-onelake.dfs.fabric.microsoft.com, account "msit-onelake").
        /// </summary>
        Msit
    }
}
