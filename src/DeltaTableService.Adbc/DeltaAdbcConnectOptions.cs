// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Microsoft.DI.DeltaTableService.Client.Models;

namespace Microsoft.DI.DeltaTableService.Adbc
{
    internal sealed class DeltaAdbcConnectOptions
    {
        public const string TableUriKey = "delta.table_uri";
        public const string StorageOptionPrefix = "delta.storage.option.";
        public const string AzureStorageAccountKey = "delta.azure.storage_account";
        public const string AzureSasTokenKey = "delta.azure.sas_token";
        public const string BearerTokenStorageOptionKey = "bearer_token";

        public const string LogicalTableName = "delta_table";

        public string TableUri { get; private set; } = string.Empty;

        public GenericStorageOptions? StorageOptions { get; private set; }

        public IReadOnlyDictionary<string, string> ToParameterDictionary()
        {
            var parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [TableUriKey] = TableUri,
            };

            if (StorageOptions != null)
            {
                foreach (KeyValuePair<string, string> pair in StorageOptions.Options)
                {
                    parameters[StorageOptionPrefix + pair.Key] = pair.Value;
                }
            }

            return parameters;
        }

        public static DeltaAdbcConnectOptions Parse(IReadOnlyDictionary<string, string> parameters)
        {
            if (parameters == null)
            {
                throw new ArgumentNullException(nameof(parameters));
            }

            var storageOptions = new Dictionary<string, string>(StringComparer.Ordinal);
            string? azureStorageAccount = null;
            string? azureSasToken = null;

            var options = new DeltaAdbcConnectOptions();

            foreach (KeyValuePair<string, string> pair in parameters)
            {
                if (string.Equals(pair.Key, TableUriKey, StringComparison.OrdinalIgnoreCase))
                {
                    options.TableUri = pair.Value;
                }
                else if (string.Equals(pair.Key, AzureStorageAccountKey, StringComparison.OrdinalIgnoreCase))
                {
                    azureStorageAccount = pair.Value;
                }
                else if (string.Equals(pair.Key, AzureSasTokenKey, StringComparison.OrdinalIgnoreCase))
                {
                    azureSasToken = pair.Value;
                }
                else if (pair.Key.StartsWith(StorageOptionPrefix, StringComparison.OrdinalIgnoreCase))
                {
                    string key = pair.Key.Substring(StorageOptionPrefix.Length);
                    if (!string.IsNullOrWhiteSpace(key))
                    {
                        storageOptions[key] = pair.Value;
                    }
                }
            }

            if (string.IsNullOrWhiteSpace(options.TableUri))
            {
                throw new ArgumentException($"Required option '{TableUriKey}' was not provided.", nameof(parameters));
            }

            if (!string.IsNullOrWhiteSpace(azureStorageAccount))
            {
                storageOptions["account_name"] = azureStorageAccount!;
                if (string.Equals(azureStorageAccount, "onelake", StringComparison.OrdinalIgnoreCase))
                {
                    storageOptions["use_fabric_endpoint"] = "true";
                }
            }

            if (!string.IsNullOrWhiteSpace(azureSasToken))
            {
                storageOptions["sas_token"] = azureSasToken!;
            }

            if (storageOptions.Count > 0)
            {
                options.StorageOptions = new GenericStorageOptions(storageOptions);
            }
            return options;
        }

    }
}
