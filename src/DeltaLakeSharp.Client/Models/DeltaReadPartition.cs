// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;

namespace DeltaLakeSharp.Client.Models
{
    /// <summary>
    /// Opaque descriptor for a partitioned Delta table read planned against a pinned table snapshot.
    /// </summary>
    public sealed class DeltaReadPartition
    {
        public DeltaReadPartition(string token, long version, int ordinal, int totalPartitions, int fileCount)
        {
            if (string.IsNullOrWhiteSpace(token))
            {
                throw new ArgumentException("Partition token must be provided.", nameof(token));
            }

            if (ordinal < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(ordinal), ordinal, "Partition ordinal must be non-negative.");
            }

            if (totalPartitions <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(totalPartitions), totalPartitions, "Total partition count must be greater than zero.");
            }

            if (fileCount < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(fileCount), fileCount, "File count must be non-negative.");
            }

            Token = token;
            Version = version;
            Ordinal = ordinal;
            TotalPartitions = totalPartitions;
            FileCount = fileCount;
        }

        public string Token { get; }

        public long Version { get; }

        public int Ordinal { get; }

        public int TotalPartitions { get; }

        public int FileCount { get; }
    }
}
