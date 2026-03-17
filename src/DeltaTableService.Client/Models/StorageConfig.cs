// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace Microsoft.DI.DeltaTableService.Client.Models
{
    /// <summary>
    /// Contains the storage account name and SAS token used to authenticate
    /// against Azure Blob File System (ABFSS) / OneLake endpoints.
    /// Passed per-request so that different tables in different storage
    /// accounts can be accessed from the same service instance.
    /// </summary>
    public sealed class StorageConfig
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StorageConfig"/> class.
        /// </summary>
        /// <param name="storageAccount">The Azure storage account name (e.g. "onelake").</param>
        /// <param name="sasToken">The SAS token used for authentication.</param>
        /// <param name="evictFileSystemCache">
        /// Whether the V1 (PySpark) backend should evict Hadoop's cached
        /// <c>FileSystem</c> instance before configuring storage.  Defaults to
        /// <c>true</c> (current behaviour — required when SAS tokens change
        /// between requests).  Set to <c>false</c> for benchmark scenarios
        /// where the same table is read repeatedly and stale cache is not a
        /// concern.  This setting has no effect on the V2 (DataFusion) backend.
        /// </param>
        public StorageConfig(string storageAccount, string sasToken, bool evictFileSystemCache = true)
        {
            StorageAccount = storageAccount ?? throw new System.ArgumentNullException(nameof(storageAccount));
            SasToken = sasToken ?? throw new System.ArgumentNullException(nameof(sasToken));
            EvictFileSystemCache = evictFileSystemCache;
        }

        /// <summary>
        /// Gets the Azure storage account name.
        /// </summary>
        public string StorageAccount { get; }

        /// <summary>
        /// Gets the SAS token for ABFSS authentication.
        /// </summary>
        public string SasToken { get; }

        /// <summary>
        /// Gets a value indicating whether the V1 (PySpark) backend should
        /// evict Hadoop's cached <c>FileSystem</c> instance before configuring
        /// storage.  Default is <c>true</c>.
        /// </summary>
        public bool EvictFileSystemCache { get; }
    }
}
