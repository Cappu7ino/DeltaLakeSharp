// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Apache.Arrow.Adbc;
using DeltaLakeSharp.Adbc.Internal;
using DeltaLakeSharp.Client.Models;

namespace DeltaLakeSharp.Adbc
{
    /// <summary>
    /// Read-only ADBC statement that either streams the configured Delta table directly or runs SQL against <c>delta_table</c>.
    /// </summary>
    public sealed class DeltaAdbcStatement : AdbcStatement
    {
        private readonly IDeltaAdbcClientAdapter _adapter;
        private readonly DeltaAdbcStatementOptions _options;

        internal DeltaAdbcStatement(IDeltaAdbcClientAdapter adapter, DeltaAdbcStatementOptions? defaultOptions = null)
        {
            _adapter = adapter;
            _options = defaultOptions?.Clone() ?? new DeltaAdbcStatementOptions();
        }

        /// <summary>
        /// Executes the current statement as an Arrow-native query.
        /// </summary>
        public override QueryResult ExecuteQuery()
        {
            ValidateCdfRange();

            if (string.IsNullOrWhiteSpace(SqlQuery))
            {
                // No SQL means "scan the configured table path directly" for the MVP surface.
                if (_options.IsChangeDataFeedRead)
                {
                    return new QueryResult(-1, _adapter.OpenChangeDataStream(_options, default));
                }

                return new QueryResult(-1, _adapter.OpenReadTableStream(_options, default));
            }

            if (_options.MaxRows.HasValue)
            {
                throw new AdbcException($"Statement option '{DeltaAdbcStatementOptions.MaxRowsOptionKey}' is supported only for direct table reads.", AdbcStatusCode.InvalidArgument);
            }

            if (_options.IsChangeDataFeedRead)
            {
                string trimmedSql = SqlQuery!.TrimStart();
                bool isDirectCdfSelect = trimmedSql.StartsWith("SELECT * FROM _cdf", StringComparison.OrdinalIgnoreCase);
                bool referencesCdf = trimmedSql.IndexOf("_cdf", StringComparison.OrdinalIgnoreCase) >= 0;
                if (!isDirectCdfSelect && !referencesCdf)
                {
                    throw new AdbcException(
                        $"SQL queries executed with '{DeltaAdbcStatementOptions.CdfStartingVersionOptionKey}' must reference '_cdf'.",
                        AdbcStatusCode.InvalidArgument);
                }

                return new QueryResult(
                    -1,
                    _adapter.OpenChangeDataQueryStream(SqlQuery!, _options, default));
            }

            return new QueryResult(-1, _adapter.OpenQueryStream(SqlQuery!, _options, default));
        }

        public override PartitionedResult ExecutePartitioned()
        {
            ValidateCdfRange();

            if (!string.IsNullOrWhiteSpace(SqlQuery))
            {
                throw new AdbcException("Partitioned execution is supported only for direct Delta table reads.", AdbcStatusCode.InvalidArgument);
            }

            if (_options.IsChangeDataFeedRead)
            {
                throw new AdbcException("Partitioned execution is not supported for Change Data Feed reads.", AdbcStatusCode.InvalidArgument);
            }

            if (_options.MaxRows.HasValue)
            {
                throw new AdbcException($"Statement option '{DeltaAdbcStatementOptions.MaxRowsOptionKey}' is not supported for partitioned reads.", AdbcStatusCode.InvalidArgument);
            }

            IReadOnlyList<DeltaReadPartition> partitions = _adapter.GetReadPartitions(_options, default);
            var descriptors = new List<PartitionDescriptor>(partitions.Count);
            foreach (DeltaReadPartition partition in partitions)
            {
                descriptors.Add(new PartitionDescriptor(AdbcPartitionDescriptorCodec.Encode(partition.Token, _options.BatchSize)));
            }

            return new PartitionedResult(_adapter.GetSchema(_options, default), -1, descriptors);
        }

        public override UpdateResult ExecuteUpdate()
        {
            throw AdbcException.NotImplemented("Delta ADBC driver currently supports read semantics only.");
        }

        public override void Prepare()
        {
            throw AdbcException.NotImplemented("Prepared statements are not implemented by the Delta ADBC driver.");
        }

        public override void SetOption(string key, string value)
        {
            _options.SetOption(key, value);
        }

        private void ValidateCdfRange()
        {
            if (_options.CdfEndingVersion.HasValue && !_options.CdfStartingVersion.HasValue)
            {
                throw new AdbcException(
                    $"Statement option '{DeltaAdbcStatementOptions.CdfEndingVersionOptionKey}' requires '{DeltaAdbcStatementOptions.CdfStartingVersionOptionKey}'.",
                    AdbcStatusCode.InvalidArgument);
            }
        }
    }
}
