// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Apache.Arrow.Adbc;

namespace DeltaLakeSharp.Adbc.Internal
{
    internal sealed class DeltaAdbcStatementOptions
    {
        public const string VersionOptionKey = "delta.version";
        public const string MaxRowsOptionKey = "delta.max_rows";
        public const string BatchSizeOptionKey = "delta.batch_size";
        public const string CdfStartingVersionOptionKey = "delta.cdf.starting_version";
        public const string CdfEndingVersionOptionKey = "delta.cdf.ending_version";

        public long? Version { get; private set; }

        public long? MaxRows { get; private set; }

        public int? BatchSize { get; private set; }

        public long? CdfStartingVersion { get; private set; }

        public long? CdfEndingVersion { get; private set; }

        public DeltaAdbcStatementOptions Clone()
        {
            return new DeltaAdbcStatementOptions
            {
                Version = Version,
                MaxRows = MaxRows,
                BatchSize = BatchSize,
                CdfStartingVersion = CdfStartingVersion,
                CdfEndingVersion = CdfEndingVersion,
            };
        }

        public DeltaAdbcStatementOptions WithDefaults(long? version, long? maxRows, int? batchSize)
        {
            Version = version;
            MaxRows = maxRows;
            BatchSize = batchSize;
            return this;
        }

        public bool IsChangeDataFeedRead => CdfStartingVersion.HasValue;

        public IReadOnlyDictionary<string, string> ToParameterDictionary()
        {
            var parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            if (Version.HasValue)
            {
                parameters[VersionOptionKey] = Version.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }

            if (MaxRows.HasValue)
            {
                parameters[MaxRowsOptionKey] = MaxRows.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }

            if (BatchSize.HasValue)
            {
                parameters[BatchSizeOptionKey] = BatchSize.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }

            if (CdfStartingVersion.HasValue)
            {
                parameters[CdfStartingVersionOptionKey] = CdfStartingVersion.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }

            if (CdfEndingVersion.HasValue)
            {
                parameters[CdfEndingVersionOptionKey] = CdfEndingVersion.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }

            return parameters;
        }

        public void SetOption(string key, string value)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new AdbcException("Statement option key must be provided.", AdbcStatusCode.InvalidArgument);
            }

            if (value == null)
            {
                throw new AdbcException($"Statement option '{key}' requires a value.", AdbcStatusCode.InvalidArgument);
            }

            try
            {
                if (string.Equals(key, VersionOptionKey, StringComparison.OrdinalIgnoreCase))
                {
                    long version = long.Parse(value, System.Globalization.CultureInfo.InvariantCulture);
                    if (version < 0)
                    {
                        throw new AdbcException($"Statement option '{VersionOptionKey}' must be greater than or equal to zero.", AdbcStatusCode.InvalidArgument);
                    }

                    Version = version;
                    return;
                }

                if (string.Equals(key, MaxRowsOptionKey, StringComparison.OrdinalIgnoreCase))
                {
                    long maxRows = long.Parse(value, System.Globalization.CultureInfo.InvariantCulture);
                    if (maxRows < 0)
                    {
                        throw new AdbcException($"Statement option '{MaxRowsOptionKey}' must be greater than or equal to zero.", AdbcStatusCode.InvalidArgument);
                    }

                    MaxRows = maxRows;
                    return;
                }

                if (string.Equals(key, BatchSizeOptionKey, StringComparison.OrdinalIgnoreCase))
                {
                    int batchSize = int.Parse(value, System.Globalization.CultureInfo.InvariantCulture);
                    if (batchSize <= 0)
                    {
                        throw new AdbcException($"Statement option '{BatchSizeOptionKey}' must be greater than zero.", AdbcStatusCode.InvalidArgument);
                    }

                    BatchSize = batchSize;
                    return;
                }

                if (string.Equals(key, CdfStartingVersionOptionKey, StringComparison.OrdinalIgnoreCase))
                {
                    long startingVersion = long.Parse(value, System.Globalization.CultureInfo.InvariantCulture);
                    if (startingVersion < 0)
                    {
                        throw new AdbcException($"Statement option '{CdfStartingVersionOptionKey}' must be greater than or equal to zero.", AdbcStatusCode.InvalidArgument);
                    }

                    if (CdfEndingVersion.HasValue && CdfEndingVersion.Value < startingVersion)
                    {
                        throw new AdbcException($"Statement option '{CdfEndingVersionOptionKey}' must be greater than or equal to '{CdfStartingVersionOptionKey}'.", AdbcStatusCode.InvalidArgument);
                    }

                    CdfStartingVersion = startingVersion;
                    return;
                }

                if (string.Equals(key, CdfEndingVersionOptionKey, StringComparison.OrdinalIgnoreCase))
                {
                    long endingVersion = long.Parse(value, System.Globalization.CultureInfo.InvariantCulture);
                    if (endingVersion < 0)
                    {
                        throw new AdbcException($"Statement option '{CdfEndingVersionOptionKey}' must be greater than or equal to zero.", AdbcStatusCode.InvalidArgument);
                    }

                    if (CdfStartingVersion.HasValue && endingVersion < CdfStartingVersion.Value)
                    {
                        throw new AdbcException($"Statement option '{CdfEndingVersionOptionKey}' must be greater than or equal to '{CdfStartingVersionOptionKey}'.", AdbcStatusCode.InvalidArgument);
                    }

                    CdfEndingVersion = endingVersion;
                    return;
                }
            }
            catch (FormatException)
            {
                throw new AdbcException($"Statement option '{key}' has an invalid value '{value}'.", AdbcStatusCode.InvalidArgument);
            }
            catch (OverflowException)
            {
                throw new AdbcException($"Statement option '{key}' has an out-of-range value '{value}'.", AdbcStatusCode.InvalidArgument);
            }

            throw new AdbcException($"Statement option '{key}' is not supported by the Delta ADBC driver.", AdbcStatusCode.InvalidArgument);
        }
    }
}
