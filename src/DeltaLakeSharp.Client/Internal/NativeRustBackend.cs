// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using DeltaLakeSharp.Client.Internal.Native;
using DeltaLakeSharp.Client.Models;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text;

namespace DeltaLakeSharp.Client.Internal
{
    /// <summary>
    /// Native in-process backend for the V3 architecture.
    ///
    /// The backend bridges the existing C# client surface to the Rust V3 core
    /// through the Arrow C Data / C Stream interfaces and JSON command payloads.
    /// </summary>
    internal sealed class NativeRustBackend : IDeltaLakeBackend
    {
        private readonly NativeEngineHandle _engine;

        internal NativeRustBackend()
        {
            _engine = NativeEngineHandle.Create();
        }

        public Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bool healthy = NativeMethods.HealthCheck(_engine.DangerousGetHandle()) == 1;
            return Task.FromResult(healthy);
        }

        public Task<TableSchema> GetSchemaAsync(
            string path,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            long? version = null,
            CancellationToken cancellationToken = default)
        {
            Schema arrowSchema = GetArrowSchema(
                path,
                storageConfig,
                genericStorageOptions,
                version,
                cancellationToken);

            return Task.FromResult(ArrowConverter.ToTableSchema(arrowSchema));
        }

        public Task<Schema> GetArrowSchemaAsync(
            string path,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            long? version = null,
            CancellationToken cancellationToken = default)
        {
            return Task.FromResult(GetArrowSchema(path, storageConfig, genericStorageOptions, version, cancellationToken));
        }

        private Schema GetArrowSchema(
            string path,
            StorageConfig? storageConfig,
            GenericStorageOptions? genericStorageOptions,
            long? version,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var command = new Dictionary<string, object>
            {
                ["path"] = path,
            };

            AddStorageConfig(command, storageConfig, genericStorageOptions);
            if (version.HasValue)
            {
                command["version"] = version.Value;
            }

            string commandJson = JsonSerializer.Serialize(command);

            unsafe
            {
                CArrowSchema* schemaPtr = CArrowSchema.Create();
                try
                {
                    int result = NativeMethods.GetSchema(
                        _engine.DangerousGetHandle(),
                        commandJson,
                        schemaPtr);

                    if (result != 1)
                    {
                        throw CreateNativeOperationFailedException(nameof(GetSchemaAsync));
                    }

                    Schema schema = CArrowSchemaImporter.ImportSchema(schemaPtr);
                    return schema;
                }
                finally
                {
                    CArrowSchema.Free(schemaPtr);
                }
            }
        }

        public async IAsyncEnumerable<RecordBatch> ReadTableAsync(
            string path,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            long? numRows = null,
            int? batchSize = null,
            long? version = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            using ArrowStreamResult streamResult = await OpenReadTableStreamAsync(
                path,
                storageConfig,
                genericStorageOptions,
                numRows,
                batchSize,
                version,
                cancellationToken).ConfigureAwait(false);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                RecordBatch? batch = await streamResult.Stream.ReadNextRecordBatchAsync(cancellationToken)
                    .ConfigureAwait(false);
                if (batch == null)
                {
                    yield break;
                }

                yield return batch;
            }
        }

        public Task<ArrowStreamResult> OpenReadTableStreamAsync(
            string path,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            long? numRows = null,
            int? batchSize = null,
            long? version = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string commandJson = BuildReadTableCommandJson(path, storageConfig, genericStorageOptions, numRows, batchSize, version);
            IArrowArrayStream stream = OpenReadTableStream(commandJson);
            return Task.FromResult(new ArrowStreamResult(stream.Schema, stream));
        }

        public Task<IReadOnlyList<DeltaReadPartition>> GetReadPartitionsAsync(
            string path,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            long? version = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var command = new Dictionary<string, object>
            {
                ["path"] = path,
            };

            AddStorageConfig(command, storageConfig, genericStorageOptions);
            if (version.HasValue)
            {
                command["version"] = version.Value;
            }

            string commandJson = JsonSerializer.Serialize(command);
            IntPtr resultPtr = NativeMethods.PlanReadPartitions(_engine.DangerousGetHandle(), commandJson);
            if (resultPtr == IntPtr.Zero)
            {
                throw CreateNativeOperationFailedException(nameof(GetReadPartitionsAsync));
            }

            try
            {
                string resultJson = NativeMethods.PtrToStringUtf8(resultPtr)
                    ?? throw new InvalidOperationException("Native plan_read_partitions returned null JSON.");
                return Task.FromResult(ParseReadPartitions(resultJson));
            }
            finally
            {
                NativeMethods.FreeString(resultPtr);
            }
        }

        public async IAsyncEnumerable<RecordBatch> ReadTablePartitionAsync(
            string path,
            DeltaReadPartition partition,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            int? batchSize = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            using ArrowStreamResult streamResult = await OpenReadTablePartitionStreamAsync(
                path,
                partition,
                storageConfig,
                genericStorageOptions,
                batchSize,
                cancellationToken).ConfigureAwait(false);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                RecordBatch? batch = await streamResult.Stream.ReadNextRecordBatchAsync(cancellationToken)
                    .ConfigureAwait(false);
                if (batch == null)
                {
                    yield break;
                }

                yield return batch;
            }
        }

        public async IAsyncEnumerable<RecordBatch> ReadTablePartitionByTokenAsync(
            string path,
            string partitionToken,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            int? batchSize = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            using ArrowStreamResult streamResult = await OpenReadTablePartitionStreamByTokenAsync(
                path,
                partitionToken,
                storageConfig,
                genericStorageOptions,
                batchSize,
                cancellationToken).ConfigureAwait(false);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                RecordBatch? batch = await streamResult.Stream.ReadNextRecordBatchAsync(cancellationToken)
                    .ConfigureAwait(false);
                if (batch == null)
                {
                    yield break;
                }

                yield return batch;
            }
        }

        public Task<ArrowStreamResult> OpenReadTablePartitionStreamAsync(
            string path,
            DeltaReadPartition partition,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            int? batchSize = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string commandJson = BuildReadTablePartitionCommandJson(path, partition, storageConfig, genericStorageOptions, batchSize);
            IArrowArrayStream stream = OpenReadTablePartitionStream(commandJson);
            return Task.FromResult(new ArrowStreamResult(stream.Schema, stream));
        }

        public Task<ArrowStreamResult> OpenReadTablePartitionStreamByTokenAsync(
            string path,
            string partitionToken,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            int? batchSize = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string commandJson = BuildReadTablePartitionCommandJson(path, partitionToken, storageConfig, genericStorageOptions, batchSize);
            IArrowArrayStream stream = OpenReadTablePartitionStream(commandJson);
            return Task.FromResult(new ArrowStreamResult(stream.Schema, stream));
        }

        public async IAsyncEnumerable<RecordBatch> ReadChangeDataAsync(
            string path,
            long startingVersion,
            long? endingVersion = null,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (RecordBatch batch in ExecuteChangeDataCoreAsync(
                path,
                startingVersion,
                endingVersion,
                storageConfig,
                genericStorageOptions,
                sql: null,
                cancellationToken).ConfigureAwait(false))
            {
                yield return batch;
            }
        }

        public Task<ArrowStreamResult> OpenReadChangeDataStreamAsync(
            string path,
            long startingVersion,
            long? endingVersion = null,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            CancellationToken cancellationToken = default)
        {
            return OpenChangeDataCoreStreamAsync(path, startingVersion, endingVersion, storageConfig, genericStorageOptions, sql: null, cancellationToken);
        }

        public async IAsyncEnumerable<RecordBatch> ExecuteChangeDataQueryAsync(
            string sql,
            string path,
            long startingVersion,
            long? endingVersion = null,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (RecordBatch batch in ExecuteChangeDataCoreAsync(
                path,
                startingVersion,
                endingVersion,
                storageConfig,
                genericStorageOptions,
                sql,
                cancellationToken).ConfigureAwait(false))
            {
                yield return batch;
            }
        }

        public Task<ArrowStreamResult> OpenExecuteChangeDataQueryStreamAsync(
            string sql,
            string path,
            long startingVersion,
            long? endingVersion = null,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            CancellationToken cancellationToken = default)
        {
            return OpenChangeDataCoreStreamAsync(path, startingVersion, endingVersion, storageConfig, genericStorageOptions, sql, cancellationToken);
        }

        private async IAsyncEnumerable<RecordBatch> ExecuteChangeDataCoreAsync(
            string path,
            long startingVersion,
            long? endingVersion,
            StorageConfig? storageConfig,
            GenericStorageOptions? genericStorageOptions,
            string? sql,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            using ArrowStreamResult streamResult = await OpenChangeDataCoreStreamAsync(
                path,
                startingVersion,
                endingVersion,
                storageConfig,
                genericStorageOptions,
                sql,
                cancellationToken).ConfigureAwait(false);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                RecordBatch? batch = await streamResult.Stream.ReadNextRecordBatchAsync(cancellationToken)
                    .ConfigureAwait(false);
                if (batch == null)
                {
                    yield break;
                }

                yield return batch;
            }
        }

        public async IAsyncEnumerable<RecordBatch> ExecuteQueryAsync(
            string sql,
            string? tablePath = null,
            string? tableName = null,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            int? batchSize = null,
            long? version = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            using ArrowStreamResult streamResult = await OpenExecuteQueryStreamAsync(
                sql,
                tablePath,
                tableName,
                storageConfig,
                genericStorageOptions,
                batchSize,
                version,
                cancellationToken).ConfigureAwait(false);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                RecordBatch? batch = await streamResult.Stream.ReadNextRecordBatchAsync(cancellationToken)
                    .ConfigureAwait(false);
                if (batch == null)
                {
                    yield break;
                }

                yield return batch;
            }
        }

        public Task<ArrowStreamResult> OpenExecuteQueryStreamAsync(
            string sql,
            string? tablePath = null,
            string? tableName = null,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            int? batchSize = null,
            long? version = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string commandJson = BuildExecuteQueryCommandJson(sql, tablePath, tableName, storageConfig, genericStorageOptions, batchSize, version);
            IArrowArrayStream stream = OpenExecuteQueryStream(commandJson);
            return Task.FromResult(new ArrowStreamResult(stream.Schema, stream));
        }

        public Task<ExecuteResult> CreateEmptyTableAsync(
            string path,
            TableSchema schema,
            StorageConfig? storageConfig = null,
            Dictionary<string, string>? configuration = null,
            IReadOnlyList<string>? partitionBy = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var command = new Dictionary<string, object>
            {
                ["path"] = path,
                ["schema"] = BuildSchemaPayload(schema),
            };

            AddStorageConfig(command, storageConfig, null);
            if (configuration != null && configuration.Count > 0)
            {
                command["configuration"] = configuration;
            }
            if (partitionBy != null && partitionBy.Count > 0)
            {
                command["partition_by"] = partitionBy;
            }

            string commandJson = JsonSerializer.Serialize(command);
            IntPtr resultPtr = NativeMethods.CreateTable(_engine.DangerousGetHandle(), commandJson);
            if (resultPtr == IntPtr.Zero)
            {
                throw CreateNativeOperationFailedException(nameof(CreateEmptyTableAsync));
            }

            try
            {
                string resultJson = NativeMethods.PtrToStringUtf8(resultPtr)
                    ?? throw new InvalidOperationException("Native create_table returned null JSON.");
                return Task.FromResult(ParseExecuteResult(resultJson));
            }
            finally
            {
                NativeMethods.FreeString(resultPtr);
            }
        }

        public Task InsertAsync(
            string path,
            Schema schema,
            IAsyncEnumerable<RecordBatch> batches,
            string mode = "overwrite",
            WriteSchemaMode? schemaMode = null,
            StorageConfig? storageConfig = null,
            IReadOnlyList<string>? partitionBy = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var command = new Dictionary<string, object>
            {
                ["path"] = path,
                ["mode"] = mode,
            };

            if (schemaMode.HasValue)
            {
                command["schema_mode"] = schemaMode.Value.ToString().ToLowerInvariant();
            }

            AddStorageConfig(command, storageConfig, null);
            if (partitionBy != null && partitionBy.Count > 0)
            {
                command["partition_by"] = partitionBy;
            }

            string commandJson = JsonSerializer.Serialize(command);
            return InsertCoreAsync(schema, batches, commandJson, cancellationToken);
        }

        public Task<ExecuteResult> DeleteAsync(
            string sql,
            string tablePath,
            string tableName,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            return ExecuteDmlAsync(sql, tablePath, tableName, storageConfig, cancellationToken);
        }

        public Task<ExecuteResult> UpdateAsync(
            string sql,
            string tablePath,
            string tableName,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            return ExecuteDmlAsync(sql, tablePath, tableName, storageConfig, cancellationToken);
        }

        public Task<ExecuteResult> MergeAsync(
            string sql,
            string tablePath,
            string tableName,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotSupportedException(
                "Native V3 backend does not support SQL MergeAsync. Use MergeDataAsync for streaming merge source data.");
        }

        public Task<ExecuteResult> MergeDataAsync(
            string path,
            Schema schema,
            IAsyncEnumerable<RecordBatch> batches,
            MergeOptions mergeOptions,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var command = mergeOptions.ToDictionary();
            command["operation"] = "merge";
            command["path"] = path;
            AddStorageConfig(command, storageConfig, null);

            string commandJson = JsonSerializer.Serialize(command);
            return MergeCoreAsync(schema, batches, commandJson, cancellationToken);
        }

        public Task<ExecuteResult> UpgradeTableProtocolAsync(
            string path,
            int readerVersion,
            int writerVersion,
            IReadOnlyList<string>? readerFeatures = null,
            IReadOnlyList<string>? writerFeatures = null,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var command = new Dictionary<string, object>
            {
                ["path"] = path,
                ["reader_version"] = readerVersion,
                ["writer_version"] = writerVersion,
            };

            AddStorageConfig(command, storageConfig, null);
            if (readerFeatures != null && readerFeatures.Count > 0)
            {
                command["reader_features"] = readerFeatures;
            }
            if (writerFeatures != null && writerFeatures.Count > 0)
            {
                command["writer_features"] = writerFeatures;
            }

            string commandJson = JsonSerializer.Serialize(command);
            IntPtr resultPtr = NativeMethods.UpgradeProtocol(_engine.DangerousGetHandle(), commandJson);
            if (resultPtr == IntPtr.Zero)
            {
                throw CreateNativeOperationFailedException(nameof(UpgradeTableProtocolAsync));
            }

            try
            {
                string resultJson = NativeMethods.PtrToStringUtf8(resultPtr)
                    ?? throw new InvalidOperationException("Native upgrade_protocol returned null JSON.");
                return Task.FromResult(ParseExecuteResult(resultJson));
            }
            finally
            {
                NativeMethods.FreeString(resultPtr);
            }
        }

        public void Dispose()
        {
            _engine.Dispose();
        }

        private InvalidOperationException CreateNativeOperationFailedException(string operation)
        {
            string? lastError = GetLastErrorMessage();
            string message = $"Native V3 backend operation '{operation}' failed.";

            if (!string.IsNullOrWhiteSpace(lastError))
            {
                message += $" Native error: {lastError}";
            }

            return new InvalidOperationException(message);
        }

        private string? GetLastErrorMessage()
        {
            return NativeMethods.PtrToStringUtf8(
                NativeMethods.GetLastError(_engine.DangerousGetHandle()));
        }

        /// <summary>
        /// Performs the unsafe Arrow C Stream import and returns a managed Arrow
        /// stream object that owns the native release callback lifecycle.
        /// </summary>
        private unsafe IArrowArrayStream OpenReadTableStream(string commandJson)
        {
            CArrowArrayStream* streamPtr = CArrowArrayStream.Create();
            try
            {
                int result = NativeMethods.ReadTable(
                    _engine.DangerousGetHandle(),
                    commandJson,
                    streamPtr);

                if (result != 1)
                {
                    throw CreateNativeOperationFailedException(nameof(ReadTableAsync));
                }

                IArrowArrayStream stream = CArrowArrayStreamImporter.ImportArrayStream(streamPtr);
                CArrowArrayStream.Free(streamPtr);
                return stream;
            }
            catch
            {
                CArrowArrayStream.Free(streamPtr);
                throw;
            }
        }

        private unsafe IArrowArrayStream OpenReadTablePartitionStream(string commandJson)
        {
            CArrowArrayStream* streamPtr = CArrowArrayStream.Create();
            try
            {
                int result = NativeMethods.ReadTablePartition(
                    _engine.DangerousGetHandle(),
                    commandJson,
                    streamPtr);

                if (result != 1)
                {
                    throw CreateNativeOperationFailedException(nameof(ReadTablePartitionAsync));
                }

                IArrowArrayStream stream = CArrowArrayStreamImporter.ImportArrayStream(streamPtr);
                CArrowArrayStream.Free(streamPtr);
                return stream;
            }
            catch
            {
                CArrowArrayStream.Free(streamPtr);
                throw;
            }
        }

        /// <summary>
        /// Imports the result stream for a SQL/read query from the native backend.
        /// The logic mirrors <see cref="OpenReadTableStream"/> so both table reads
        /// and query execution share the same Arrow C Stream transport pattern.
        /// </summary>
        private unsafe IArrowArrayStream OpenExecuteQueryStream(string commandJson)
        {
            CArrowArrayStream* streamPtr = CArrowArrayStream.Create();
            try
            {
                int result = NativeMethods.ExecuteQuery(
                    _engine.DangerousGetHandle(),
                    commandJson,
                    streamPtr);

                if (result != 1)
                {
                    throw CreateNativeOperationFailedException(nameof(ExecuteQueryAsync));
                }

                IArrowArrayStream stream = CArrowArrayStreamImporter.ImportArrayStream(streamPtr);
                CArrowArrayStream.Free(streamPtr);
                return stream;
            }
            catch
            {
                CArrowArrayStream.Free(streamPtr);
                throw;
            }
        }

        private unsafe IArrowArrayStream OpenChangeDataStream(string commandJson)
        {
            CArrowArrayStream* streamPtr = CArrowArrayStream.Create();
            try
            {
                int result = NativeMethods.ReadChangeData(
                    _engine.DangerousGetHandle(),
                    commandJson,
                    streamPtr);

                if (result != 1)
                {
                    throw CreateNativeOperationFailedException(nameof(ReadChangeDataAsync));
                }

                IArrowArrayStream stream = CArrowArrayStreamImporter.ImportArrayStream(streamPtr);
                CArrowArrayStream.Free(streamPtr);
                return stream;
            }
            catch
            {
                CArrowArrayStream.Free(streamPtr);
                throw;
            }
        }

        /// <summary>
        /// Exports a managed Arrow stream to the native Rust backend using the
        /// Arrow C Stream interface.
        /// </summary>
        private unsafe Task InsertCoreAsync(
            Schema schema,
            IAsyncEnumerable<RecordBatch> batches,
            string commandJson,
            CancellationToken cancellationToken)
        {
            IArrowArrayStream stream = new AsyncEnumerableArrowArrayStream(schema, batches, cancellationToken);
            CArrowArrayStream* streamPtr = CArrowArrayStream.Create();
            try
            {
                CArrowArrayStreamExporter.ExportArrayStream(stream, streamPtr);
                int result = NativeMethods.Insert(
                    _engine.DangerousGetHandle(),
                    commandJson,
                    streamPtr);

                if (result != 1)
                {
                    throw CreateNativeOperationFailedException(nameof(InsertAsync));
                }

                return Task.CompletedTask;
            }
            finally
            {
                stream.Dispose();
                CArrowArrayStream.Free(streamPtr);
            }
        }

        /// <summary>
        /// Exports a managed Arrow stream to the native Rust merge entrypoint.
        /// This mirrors the insert path but returns the standard action result
        /// envelope containing merge metrics.
        /// </summary>
        private unsafe Task<ExecuteResult> MergeCoreAsync(
            Schema schema,
            IAsyncEnumerable<RecordBatch> batches,
            string commandJson,
            CancellationToken cancellationToken)
        {
            IArrowArrayStream stream = new AsyncEnumerableArrowArrayStream(schema, batches, cancellationToken);
            CArrowArrayStream* streamPtr = CArrowArrayStream.Create();
            try
            {
                CArrowArrayStreamExporter.ExportArrayStream(stream, streamPtr);
                IntPtr resultPtr = NativeMethods.MergeStream(
                    _engine.DangerousGetHandle(),
                    commandJson,
                    streamPtr);

                if (resultPtr == IntPtr.Zero)
                {
                    throw CreateNativeOperationFailedException(nameof(MergeDataAsync));
                }

                try
                {
                    string resultJson = NativeMethods.PtrToStringUtf8(resultPtr)
                        ?? throw new InvalidOperationException("Native merge_stream returned null JSON.");
                    return Task.FromResult(ParseExecuteResult(resultJson));
                }
                finally
                {
                    NativeMethods.FreeString(resultPtr);
                }
            }
            finally
            {
                stream.Dispose();
                CArrowArrayStream.Free(streamPtr);
            }
        }

        /// <summary>
        /// Parses the common V3 action result envelope into the public client model.
        /// Keeping the envelope identical to Flight simplifies migration and lets
        /// existing tests assert the same server behavior across transports.
        /// </summary>
        private static ExecuteResult ParseExecuteResult(string json)
        {
            JsonNode root = JsonNode.Parse(json)
                ?? throw new InvalidOperationException("Native backend returned invalid JSON.");

            bool success = root["success"]?.GetValue<bool>() ?? false;
            string message = root["message"]?.GetValue<string>() ?? string.Empty;
            var rows = new List<Dictionary<string, object?>>();

            if (root["result"] is JsonArray resultArray)
            {
                foreach (JsonNode? rowNode in resultArray)
                {
                    if (rowNode is not JsonObject obj)
                    {
                        continue;
                    }

                    var row = new Dictionary<string, object?>();
                    foreach (var kvp in obj)
                    {
                        row[kvp.Key] = ConvertJsonNodeToClrValue(kvp.Value);
                    }

                    rows.Add(row);
                }
            }

            return new ExecuteResult(success, message, rows);
        }

        private static object? ConvertJsonNodeToClrValue(JsonNode? node)
        {
            if (node == null)
            {
                return string.Empty;
            }

            if (node is JsonValue value)
            {
                if (value.TryGetValue(out string? stringValue))
                {
                    return stringValue ?? string.Empty;
                }

                if (value.TryGetValue(out bool boolValue))
                {
                    return boolValue;
                }

                if (value.TryGetValue(out long longValue))
                {
                    return longValue;
                }

                if (value.TryGetValue(out int intValue))
                {
                    return intValue;
                }

                if (value.TryGetValue(out double doubleValue))
                {
                    return doubleValue;
                }
            }

            return node.ToJsonString();
        }

        /// <summary>
        /// Executes a SQL DML action through the native backend using the same
        /// JSON contract as the existing Flight `execute_dml` action.
        /// </summary>
        private Task<ExecuteResult> ExecuteDmlAsync(
            string sql,
            string tablePath,
            string tableName,
            StorageConfig? storageConfig,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var command = new Dictionary<string, object>
            {
                ["sql"] = sql,
                ["table_path"] = tablePath,
                ["table_name"] = tableName,
            };

            AddStorageConfig(command, storageConfig, null);
            string commandJson = JsonSerializer.Serialize(command);

            IntPtr resultPtr = NativeMethods.ExecuteDml(_engine.DangerousGetHandle(), commandJson);
            if (resultPtr == IntPtr.Zero)
            {
                throw CreateNativeOperationFailedException(nameof(DeleteAsync));
            }

            try
            {
                string resultJson = NativeMethods.PtrToStringUtf8(resultPtr)
                    ?? throw new InvalidOperationException("Native execute_dml returned null JSON.");
                return Task.FromResult(ParseExecuteResult(resultJson));
            }
            finally
            {
                NativeMethods.FreeString(resultPtr);
            }
        }

        private static List<Dictionary<string, object>> BuildSchemaPayload(TableSchema schema)
        {
            var columns = new List<Dictionary<string, object>>(schema.Columns.Count);
            foreach (ColumnDefinition column in schema.Columns)
            {
                columns.Add(new Dictionary<string, object>
                {
                    ["name"] = column.Name,
                    ["type"] = column.DataType,
                    ["nullable"] = column.Nullable,
                });
            }

            return columns;
        }

        private static string BuildReadTableCommandJson(
            string path,
            StorageConfig? storageConfig,
            GenericStorageOptions? genericStorageOptions,
            long? numRows,
            int? batchSize,
            long? version)
        {
            var command = new Dictionary<string, object>
            {
                ["path"] = path,
            };

            AddStorageConfig(command, storageConfig, genericStorageOptions);
            if (numRows.HasValue)
            {
                command["num_rows"] = numRows.Value;
            }

            if (batchSize.HasValue)
            {
                command["batch_size"] = batchSize.Value;
            }
            if (version.HasValue)
            {
                command["version"] = version.Value;
            }

            return JsonSerializer.Serialize(command);
        }

        private static string BuildReadTablePartitionCommandJson(
            string path,
            DeltaReadPartition partition,
            StorageConfig? storageConfig,
            GenericStorageOptions? genericStorageOptions,
            int? batchSize)
        {
            return BuildReadTablePartitionCommandJson(path, partition.Token, storageConfig, genericStorageOptions, batchSize);
        }

        private static string BuildReadTablePartitionCommandJson(
            string path,
            string partitionToken,
            StorageConfig? storageConfig,
            GenericStorageOptions? genericStorageOptions,
            int? batchSize)
        {
            var command = new Dictionary<string, object>
            {
                ["path"] = path,
                ["partition_token"] = partitionToken,
            };

            AddStorageConfig(command, storageConfig, genericStorageOptions);
            if (batchSize.HasValue)
            {
                command["batch_size"] = batchSize.Value;
            }

            return JsonSerializer.Serialize(command);
        }

        private static IReadOnlyList<DeltaReadPartition> ParseReadPartitions(string json)
        {
            JsonNode root = JsonNode.Parse(json)
                ?? throw new InvalidOperationException("Native backend returned invalid JSON for partition planning.");

            if (!(root["success"]?.GetValue<bool>() ?? false))
            {
                string message = root["message"]?.GetValue<string>() ?? "Partition planning failed.";
                throw new InvalidOperationException(message);
            }

            var partitions = new List<DeltaReadPartition>();
            if (root["result"] is JsonArray resultArray)
            {
                foreach (JsonNode? item in resultArray)
                {
                    if (item is not JsonObject obj)
                    {
                        continue;
                    }

                    string token = obj["token"]?.GetValue<string>()
                        ?? throw new InvalidOperationException("Partition result is missing token.");
                    long version = obj["version"]?.GetValue<long>()
                        ?? throw new InvalidOperationException("Partition result is missing version.");
                    int ordinal = obj["ordinal"]?.GetValue<int>()
                        ?? throw new InvalidOperationException("Partition result is missing ordinal.");
                    int totalPartitions = obj["totalPartitions"]?.GetValue<int>()
                        ?? throw new InvalidOperationException("Partition result is missing totalPartitions.");
                    int fileCount = obj["fileCount"]?.GetValue<int>()
                        ?? throw new InvalidOperationException("Partition result is missing fileCount.");

                    partitions.Add(new DeltaReadPartition(token, version, ordinal, totalPartitions, fileCount));
                }
            }

            return partitions;
        }

        private Task<ArrowStreamResult> OpenChangeDataCoreStreamAsync(
            string path,
            long startingVersion,
            long? endingVersion,
            StorageConfig? storageConfig,
            GenericStorageOptions? genericStorageOptions,
            string? sql,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var command = new Dictionary<string, object>
            {
                ["path"] = path,
                ["starting_version"] = startingVersion,
            };

            AddStorageConfig(command, storageConfig, genericStorageOptions);
            if (endingVersion.HasValue)
            {
                command["ending_version"] = endingVersion.Value;
            }

            if (!string.IsNullOrWhiteSpace(sql))
            {
                command["sql"] = sql!;
            }

            string commandJson = JsonSerializer.Serialize(command);
            IArrowArrayStream stream = OpenChangeDataStream(commandJson);
            return Task.FromResult(new ArrowStreamResult(stream.Schema, stream));
        }

        private static string BuildExecuteQueryCommandJson(
            string sql,
            string? tablePath,
            string? tableName,
            StorageConfig? storageConfig,
            GenericStorageOptions? genericStorageOptions,
            int? batchSize,
            long? version)
        {
            var command = new Dictionary<string, object>
            {
                ["sql"] = sql,
            };

            if (!string.IsNullOrWhiteSpace(tablePath))
            {
                command["table_path"] = tablePath!;
            }

            if (!string.IsNullOrWhiteSpace(tableName))
            {
                command["table_name"] = tableName!;
            }

            AddStorageConfig(command, storageConfig, genericStorageOptions);
            if (batchSize.HasValue)
            {
                command["batch_size"] = batchSize.Value;
            }
            if (version.HasValue)
            {
                command["version"] = version.Value;
            }

            return JsonSerializer.Serialize(command);
        }

        /// <summary>
        /// Adds per-request storage settings using the same wire contract as the
        /// existing Flight-based backend. Keeping the JSON shape identical makes
        /// it easier to migrate transports without changing service semantics.
        /// </summary>
        private static void AddStorageConfig(
            IDictionary<string, object> command,
            StorageConfig? storageConfig,
            GenericStorageOptions? genericStorageOptions)
        {
            if (storageConfig == null && genericStorageOptions == null)
            {
                return;
            }

            if (storageConfig != null)
            {
                command["storage_account"] = storageConfig.StorageAccount;
                command["sas_token"] = storageConfig.SasToken;
            }

            if (genericStorageOptions != null && genericStorageOptions.Options.Count > 0)
            {
                var storageOptions = new Dictionary<string, string>(StringComparer.Ordinal);
                foreach (KeyValuePair<string, string> option in genericStorageOptions.Options)
                {
                    storageOptions[option.Key] = option.Value;
                }

                command["storage_options"] = storageOptions;
            }
        }

        /// <summary>
        /// Minimal <see cref="IArrowArrayStream"/> adapter over
        /// <see cref="IAsyncEnumerable{RecordBatch}"/>.
        ///
        /// The native insert ABI is pull-based because the Arrow C Stream
        /// interface is pull-based. This adapter lets the existing client-facing
        /// async batch source participate in that model without changing the
        /// public API surface.
        /// </summary>
        private sealed class AsyncEnumerableArrowArrayStream : IArrowArrayStream
        {
            private readonly IAsyncEnumerator<RecordBatch> _enumerator;

            public AsyncEnumerableArrowArrayStream(
                Schema schema,
                IAsyncEnumerable<RecordBatch> batches,
                CancellationToken cancellationToken)
            {
                Schema = schema;
                _enumerator = batches.GetAsyncEnumerator(cancellationToken);
            }

            public Schema Schema { get; }

            public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(
                CancellationToken cancellationToken = default)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return await _enumerator.MoveNextAsync().ConfigureAwait(false)
                    ? _enumerator.Current
                    : null;
            }

            public void Dispose()
            {
                _enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
        }
    }
}
