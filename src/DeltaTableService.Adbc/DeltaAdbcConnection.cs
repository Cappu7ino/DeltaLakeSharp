// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Microsoft.DI.DeltaTableService.Adbc.Internal;

namespace Microsoft.DI.DeltaTableService.Adbc
{
    /// <summary>
    /// Read-only ADBC connection that exposes one synthetic logical table for a Delta path.
    /// </summary>
    public sealed class DeltaAdbcConnection : AdbcConnection
    {
        private readonly IDeltaAdbcClientAdapter _adapter;
        private readonly DeltaAdbcConnectOptions? _options;

        internal DeltaAdbcConnection(DeltaAdbcConnectOptions options)
            : this(new DeltaAdbcClientAdapter(options))
        {
            _options = options;
        }

        internal DeltaAdbcConnection(DeltaAdbcConnectOptions options, IDeltaAdbcClientAdapter adapter)
        {
            _options = options;
            _adapter = adapter;
        }

        internal DeltaAdbcConnection(IDeltaAdbcClientAdapter adapter)
        {
            _adapter = adapter;
        }

        internal DeltaAdbcConnectOptions? Options => _options;

        /// <summary>
        /// Creates a statement that can either read the underlying Delta table directly or execute SQL against the logical table alias.
        /// </summary>
        public override AdbcStatement CreateStatement()
        {
            return new DeltaAdbcStatement(_adapter, CreateDefaultStatementOptions());
        }

        /// <summary>
        /// Returns the Arrow schema for the single supported logical table, <c>delta_table</c>.
        /// </summary>
        public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
        {
            ValidateLogicalTable(catalog, dbSchema, tableName);
            return _adapter.GetSchema(CreateDefaultStatementOptions(), default);
        }

        /// <summary>
        /// Returns the single supported table type for the path-scoped driver surface.
        /// </summary>
        public override IArrowArrayStream GetTableTypes()
        {
            return DeltaAdbcMetadataBuilder.CreateTableTypesStream();
        }

        /// <summary>
        /// Returns driver and vendor metadata for the Delta ADBC implementation.
        /// </summary>
        public override IArrowArrayStream GetInfo(IReadOnlyList<AdbcInfoCode> codes)
        {
            return DeltaAdbcMetadataBuilder.CreateGetInfoStream(codes);
        }

        /// <summary>
        /// Returns a synthetic catalog/schema/table hierarchy rooted at the single logical table exposed by this connection.
        /// </summary>
        public override IArrowArrayStream GetObjects(
            GetObjectsDepth depth,
            string? catalogPattern,
            string? dbSchemaPattern,
            string? tableNamePattern,
            IReadOnlyList<string>? tableTypes,
            string? columnNamePattern)
        {
            return DeltaAdbcMetadataBuilder.CreateGetObjectsStream(
                _adapter.GetSchema(CreateDefaultStatementOptions(), default),
                depth,
                catalogPattern,
                dbSchemaPattern,
                tableNamePattern,
                tableTypes,
                columnNamePattern);
        }

        public override IArrowArrayStream ReadPartition(PartitionDescriptor partition)
        {
            AdbcPartitionDescriptorCodec.DecodedPartitionDescriptor decoded = AdbcPartitionDescriptorCodec.Decode(partition);
            return _adapter.OpenReadPartitionStream(decoded.Token, decoded.BatchSize, default);
        }

        public override bool ReadOnly
        {
            get => true;
            set => throw new AdbcException("Delta ADBC connection is read-only.", AdbcStatusCode.InvalidState);
        }

        public override void Dispose()
        {
            _adapter.Dispose();
            base.Dispose();
        }

        private DeltaAdbcStatementOptions CreateDefaultStatementOptions()
        {
            return new DeltaAdbcStatementOptions().WithDefaults(
                version: _options?.Version,
                maxRows: null,
                batchSize: null);
        }

        private static void ValidateLogicalTable(string? catalog, string? dbSchema, string tableName)
        {
            // The MVP driver is intentionally path-scoped, so catalog/schema inputs are rejected and the caller must reference the fixed logical table alias.
            if (!string.IsNullOrEmpty(catalog))
            {
                throw new AdbcException("Catalog is not supported for path-based Delta access.", AdbcStatusCode.InvalidArgument);
            }

            if (!string.IsNullOrEmpty(dbSchema))
            {
                throw new AdbcException("Schema is not supported for path-based Delta access.", AdbcStatusCode.InvalidArgument);
            }

            if (!string.Equals(tableName, DeltaAdbcConnectOptions.LogicalTableName, StringComparison.Ordinal))
            {
                throw new AdbcException($"Only logical table '{DeltaAdbcConnectOptions.LogicalTableName}' is supported.", AdbcStatusCode.NotFound);
            }
        }
    }
}
