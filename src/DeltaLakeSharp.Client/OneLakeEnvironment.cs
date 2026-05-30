namespace DeltaLakeSharp.Client
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
