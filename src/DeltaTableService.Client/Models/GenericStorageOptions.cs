// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;

namespace Microsoft.DI.DeltaTableService.Client.Models
{
    /// <summary>
    /// Generic storage options passed through to delta-rs-backed object stores.
    /// </summary>
    public sealed class GenericStorageOptions
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="GenericStorageOptions"/> class.
        /// </summary>
        public GenericStorageOptions()
            : this(new Dictionary<string, string>(StringComparer.Ordinal))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="GenericStorageOptions"/> class.
        /// </summary>
        /// <param name="options">Storage options to pass to delta-rs.</param>
        public GenericStorageOptions(IReadOnlyDictionary<string, string> options)
        {
            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            var normalizedOptions = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (KeyValuePair<string, string> option in options)
            {
                normalizedOptions[option.Key] = option.Value;
            }

            Options = normalizedOptions;
        }

        /// <summary>
        /// Gets the normalized storage options.
        /// </summary>
        public IReadOnlyDictionary<string, string> Options { get; }

        /// <summary>
        /// Creates generic storage options from the legacy Azure-specific <see cref="StorageConfig"/>.
        /// </summary>
        public static GenericStorageOptions FromStorageConfig(StorageConfig storageConfig)
        {
            if (storageConfig == null)
            {
                throw new ArgumentNullException(nameof(storageConfig));
            }

            var options = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["account_name"] = storageConfig.StorageAccount,
                ["sas_token"] = storageConfig.SasToken,
            };

            if (string.Equals(storageConfig.StorageAccount, "onelake", StringComparison.OrdinalIgnoreCase))
            {
                options["use_fabric_endpoint"] = "true";
            }

            return new GenericStorageOptions(options);
        }
    }
}
