// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Ipc;
using Google.Protobuf;
using Grpc.Net.Client;
using DeltaLakeSharp.Client.Models;

namespace DeltaLakeSharp.Client.Internal
{
    /// <summary>
    /// Thin wrapper around <see cref="FlightClient"/> that encapsulates all
    /// gRPC / Arrow Flight protocol details. Implements <see cref="IDeltaLakeBackend"/>
    /// to provide the V1 backend for the Delta Table Service.
    /// </summary>
    internal sealed class FlightClientWrapper : IDeltaLakeBackend
    {
        private readonly GrpcChannel _channel;
        private readonly FlightClient _client;

        /// <summary>
        /// Creates a new wrapper connected to the given Flight server endpoint.
        /// </summary>
        /// <param name="serverUri">
        /// The base URI of the Arrow Flight server, e.g. <c>http://localhost:8815</c>.
        /// </param>
        internal FlightClientWrapper(Uri serverUri)
        {
            _channel = GrpcChannel.ForAddress(serverUri, new GrpcChannelOptions
            {
                MaxReceiveMessageSize = null, // unlimited
                MaxSendMessageSize = null,    // unlimited
            });
            _client = new FlightClient(_channel);
        }

        // ------------------------------------------------------------------ //
        //  IDeltaLakeBackend Implementation
        // ------------------------------------------------------------------ //

        /// <inheritdoc />
        public async Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                byte[] resultBytes = await DoActionAsync("health", null, cancellationToken)
                    .ConfigureAwait(false);
                using JsonDocument doc = JsonDocument.Parse(resultBytes);
                return doc.RootElement.TryGetProperty("status", out JsonElement statusEl)
                    && statusEl.GetString() == "healthy";
            }
            catch
            {
                return false;
            }
        }

        /// <inheritdoc />
        public async Task<TableSchema> GetSchemaAsync(
            string path,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            long? version = null,
            CancellationToken cancellationToken = default)
        {
            var cmd = new Dictionary<string, object> { ["path"] = path };
            AddStorageConfig(cmd, storageConfig, genericStorageOptions);
            if (version.HasValue)
            {
                cmd["version"] = version.Value;
            }
            byte[] commandJson = JsonSerializer.SerializeToUtf8Bytes(cmd);
            Schema arrowSchema = await GetArrowSchemaAsync(commandJson).ConfigureAwait(false);
            return ArrowConverter.ToTableSchema(arrowSchema);
        }

        public Task<Schema> GetArrowSchemaAsync(
            string path,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            long? version = null,
            CancellationToken cancellationToken = default)
        {
            var cmd = new Dictionary<string, object> { ["path"] = path };
            AddStorageConfig(cmd, storageConfig, genericStorageOptions);
            if (version.HasValue)
            {
                cmd["version"] = version.Value;
            }

            byte[] commandJson = JsonSerializer.SerializeToUtf8Bytes(cmd);
            return GetArrowSchemaAsync(commandJson);
        }

        /// <inheritdoc />
        public IAsyncEnumerable<RecordBatch> ReadTableAsync(
            string path,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            long? numRows = null,
            int? batchSize = null,
            long? version = null,
            CancellationToken cancellationToken = default)
        {
            byte[] commandJson = BuildReadCommand(path, storageConfig, genericStorageOptions, numRows, batchSize, version);
            return GetRecordBatchesStreamingAsync(commandJson, cancellationToken);
        }

        public async Task<ArrowStreamResult> OpenReadTableStreamAsync(
            string path,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            long? numRows = null,
            int? batchSize = null,
            long? version = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] commandJson = BuildReadCommand(path, storageConfig, genericStorageOptions, numRows, batchSize, version);
            return await OpenArrowArrayStreamAsync(commandJson, cancellationToken).ConfigureAwait(false);
        }

        public Task<IReadOnlyList<DeltaReadPartition>> GetReadPartitionsAsync(
            string path,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            long? version = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotSupportedException(
                "Partitioned reads are supported only by the V3 native Rust backend.");
        }

        public async IAsyncEnumerable<RecordBatch> ReadTablePartitionAsync(
            string path,
            DeltaReadPartition partition,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            int? batchSize = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotSupportedException(
                "Partitioned reads are supported only by the V3 native Rust backend.");
#pragma warning disable CS0162
            yield break;
#pragma warning restore CS0162
        }

        public async IAsyncEnumerable<RecordBatch> ReadTablePartitionByTokenAsync(
            string path,
            string partitionToken,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            int? batchSize = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotSupportedException(
                "Partitioned reads are supported only by the V3 native Rust backend.");
#pragma warning disable CS0162
            yield break;
#pragma warning restore CS0162
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
            throw new NotSupportedException(
                "Partitioned reads are supported only by the V3 native Rust backend.");
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
            throw new NotSupportedException(
                "Partitioned reads are supported only by the V3 native Rust backend.");
        }

        /// <inheritdoc />
        public async IAsyncEnumerable<RecordBatch> ReadChangeDataAsync(
            string path,
            long startingVersion,
            long? endingVersion = null,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotSupportedException(
                "ReadChangeDataAsync is supported only by the V3 native Rust backend.");
#pragma warning disable CS0162
            yield break;
#pragma warning restore CS0162
        }

        public Task<ArrowStreamResult> OpenReadChangeDataStreamAsync(
            string path,
            long startingVersion,
            long? endingVersion = null,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotSupportedException(
                "ReadChangeDataAsync is supported only by the V3 native Rust backend.");
        }

        /// <inheritdoc />
        public async IAsyncEnumerable<RecordBatch> ExecuteChangeDataQueryAsync(
            string sql,
            string path,
            long startingVersion,
            long? endingVersion = null,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotSupportedException(
                "ExecuteChangeDataQueryAsync is supported only by the V3 native Rust backend.");
#pragma warning disable CS0162
            yield break;
#pragma warning restore CS0162
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
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotSupportedException(
                "ExecuteChangeDataQueryAsync is supported only by the V3 native Rust backend.");
        }

        /// <inheritdoc />
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
            // Always use GetFlightInfo + DoGet streaming path, regardless of
            // whether tablePath/tableName are provided.  The server handles
            // both cases (plain SQL and SQL-with-table-registration).
            var cmd = new Dictionary<string, object> { ["sql"] = sql };
            if (tablePath != null)
            {
                cmd["table_path"] = tablePath;
            }
            if (tableName != null)
            {
                cmd["table_name"] = tableName;
            }
            AddStorageConfig(cmd, storageConfig, genericStorageOptions);
            if (batchSize.HasValue)
            {
                cmd["batch_size"] = batchSize.Value;
            }
            if (version.HasValue)
            {
                cmd["version"] = version.Value;
            }
            byte[] commandJson = JsonSerializer.SerializeToUtf8Bytes(cmd);
            await foreach (RecordBatch batch in GetRecordBatchesStreamingAsync(commandJson, cancellationToken).ConfigureAwait(false))
            {
                yield return batch;
            }
        }

        public async Task<ArrowStreamResult> OpenExecuteQueryStreamAsync(
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

            var cmd = new Dictionary<string, object> { ["sql"] = sql };
            if (tablePath != null)
            {
                cmd["table_path"] = tablePath;
            }
            if (tableName != null)
            {
                cmd["table_name"] = tableName;
            }
            AddStorageConfig(cmd, storageConfig, genericStorageOptions);
            if (batchSize.HasValue)
            {
                cmd["batch_size"] = batchSize.Value;
            }
            if (version.HasValue)
            {
                cmd["version"] = version.Value;
            }

            byte[] commandJson = JsonSerializer.SerializeToUtf8Bytes(cmd);
            return await OpenArrowArrayStreamAsync(commandJson, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<ExecuteResult> CreateEmptyTableAsync(
            string path,
            TableSchema schema,
            StorageConfig? storageConfig = null,
            Dictionary<string, string>? configuration = null,
            IReadOnlyList<string>? partitionBy = null,
            CancellationToken cancellationToken = default)
        {
            var schemaList = new List<Dictionary<string, string>>();
            foreach (ColumnDefinition col in schema.Columns)
            {
                schemaList.Add(new Dictionary<string, string>
                {
                    ["name"] = col.Name,
                    ["type"] = col.DataType,
                });
            }

            var cmd = new Dictionary<string, object>
            {
                ["path"] = path,
                ["schema"] = schemaList,
            };
            AddStorageConfig(cmd, storageConfig, null);
            if (configuration != null && configuration.Count > 0)
            {
                cmd["configuration"] = configuration;
            }
            if (partitionBy != null && partitionBy.Count > 0)
            {
                cmd["partition_by"] = partitionBy;
            }
            byte[] body = JsonSerializer.SerializeToUtf8Bytes(cmd);
            byte[] resultBytes = await DoActionAsync("create_table", body, cancellationToken)
                .ConfigureAwait(false);
            return ParseExecuteResult(resultBytes);
        }

        /// <inheritdoc />
        public async Task InsertAsync(
            string path,
            Schema schema,
            IAsyncEnumerable<RecordBatch> batches,
            string mode = "overwrite",
            WriteSchemaMode? schemaMode = null,
            StorageConfig? storageConfig = null,
            IReadOnlyList<string>? partitionBy = null,
            CancellationToken cancellationToken = default)
        {
            if (schemaMode.HasValue)
            {
                throw new NotSupportedException(
                    "Schema-aware overwrite is currently supported only by the native V3 backend.");
            }

            var cmd = new Dictionary<string, object>
            {
                ["path"] = path,
                ["mode"] = mode,
            };
            AddStorageConfig(cmd, storageConfig, null);
            if (partitionBy != null && partitionBy.Count > 0)
            {
                cmd["partition_by"] = partitionBy;
            }
            byte[] commandJson = JsonSerializer.SerializeToUtf8Bytes(cmd);
            await DoPutStreamingAsync(commandJson, schema, batches, cancellationToken).ConfigureAwait(false);
        }

        // ------------------------------------------------------------------ //
        //  DML operations (DELETE, UPDATE, MERGE) via DoAction("execute_dml")
        // ------------------------------------------------------------------ //

        /// <inheritdoc />
        public Task<ExecuteResult> DeleteAsync(
            string sql,
            string tablePath,
            string tableName,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            return ExecuteDmlAsync(sql, tablePath, tableName, storageConfig, cancellationToken);
        }

        /// <inheritdoc />
        public Task<ExecuteResult> UpdateAsync(
            string sql,
            string tablePath,
            string tableName,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            return ExecuteDmlAsync(sql, tablePath, tableName, storageConfig, cancellationToken);
        }

        /// <inheritdoc />
        public Task<ExecuteResult> MergeAsync(
            string sql,
            string tablePath,
            string tableName,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            return ExecuteDmlAsync(sql, tablePath, tableName, storageConfig, cancellationToken);
        }

        /// <inheritdoc />
        public async Task<ExecuteResult> MergeDataAsync(
            string path,
            Schema schema,
            IAsyncEnumerable<RecordBatch> batches,
            MergeOptions mergeOptions,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            var cmd = mergeOptions.ToDictionary();
            cmd["operation"] = "merge";
            cmd["path"] = path;
            AddStorageConfig(cmd, storageConfig, null);

            byte[] commandJson = JsonSerializer.SerializeToUtf8Bytes(cmd);
            byte[]? responseBytes = await DoPutStreamingAsync(commandJson, schema, batches, cancellationToken)
                .ConfigureAwait(false);

            if (responseBytes != null && responseBytes.Length > 0)
            {
                return ParseExecuteResult(responseBytes);
            }

            // Server did not return metadata — treat as success with no metrics.
            return new ExecuteResult(true, "Merge completed (no metrics returned).");
        }

        /// <summary>
        /// Sends a DML statement (DELETE, UPDATE, MERGE) to the server via
        /// <c>DoAction("execute_dml")</c>. The server auto-registers the Delta
        /// table before executing the SQL.
        /// </summary>
        private async Task<ExecuteResult> ExecuteDmlAsync(
            string sql,
            string tablePath,
            string tableName,
            StorageConfig? storageConfig,
            CancellationToken cancellationToken)
        {
            var cmd = new Dictionary<string, object>
            {
                ["sql"] = sql,
                ["table_path"] = tablePath,
                ["table_name"] = tableName,
            };
            AddStorageConfig(cmd, storageConfig, null);
            byte[] body = JsonSerializer.SerializeToUtf8Bytes(cmd);
            byte[] resultBytes = await DoActionAsync("execute_dml", body, cancellationToken)
                .ConfigureAwait(false);
            return ParseExecuteResult(resultBytes);
        }

        // ------------------------------------------------------------------ //
        //  Protocol upgrade
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Upgrades the Delta table protocol by bumping reader/writer versions
        /// and optionally enabling table features via
        /// <c>DoAction("upgrade_protocol")</c>.
        /// </summary>
        public async Task<ExecuteResult> UpgradeTableProtocolAsync(
            string path,
            int readerVersion,
            int writerVersion,
            IReadOnlyList<string>? readerFeatures,
            IReadOnlyList<string>? writerFeatures,
            StorageConfig? storageConfig,
            CancellationToken cancellationToken)
        {
            var cmd = new Dictionary<string, object>
            {
                ["path"] = path,
                ["reader_version"] = readerVersion,
                ["writer_version"] = writerVersion,
            };

            if (readerFeatures is { Count: > 0 })
            {
                cmd["reader_features"] = readerFeatures;
            }

            if (writerFeatures is { Count: > 0 })
            {
                cmd["writer_features"] = writerFeatures;
            }

            AddStorageConfig(cmd, storageConfig, null);
            byte[] body = JsonSerializer.SerializeToUtf8Bytes(cmd);
            byte[] resultBytes = await DoActionAsync("upgrade_protocol", body, cancellationToken)
                .ConfigureAwait(false);
            return ParseExecuteResult(resultBytes);
        }

        // ------------------------------------------------------------------ //
        //  GetFlightInfo + DoGet  ->  streaming read (IAsyncEnumerable)
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Executes a GetFlightInfo + DoGet round-trip and yields each
        /// <see cref="RecordBatch"/> as it arrives from the server, without
        /// buffering the entire result set in memory.
        /// </summary>
        private async IAsyncEnumerable<RecordBatch> GetRecordBatchesStreamingAsync(
            byte[] commandJson,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            using ArrowStreamResult streamResult = await OpenArrowArrayStreamAsync(commandJson, cancellationToken).ConfigureAwait(false);
            while (true)
            {
                RecordBatch? batch = await streamResult.Stream.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
                if (batch == null)
                {
                    yield break;
                }

                yield return batch;
            }
        }

        private async Task<ArrowStreamResult> OpenArrowArrayStreamAsync(
            byte[] commandJson,
            CancellationToken cancellationToken)
        {
            var descriptor = FlightDescriptor.CreateCommandDescriptor(commandJson);
            FlightInfo info = await _client.GetInfo(descriptor).ResponseAsync.ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();

            if (info.Endpoints.Count == 0)
            {
                throw new InvalidOperationException("Flight server returned no endpoints for the requested stream.");
            }

            FlightRecordBatchStreamingCall call = _client.GetStream(info.Endpoints[0].Ticket);
            return new ArrowStreamResult(info.Schema, new FlightArrowArrayStream(info.Schema, call));
        }

        // ------------------------------------------------------------------ //
        //  GetSchema  ->  read table schema
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Calls the Flight GetSchema RPC to retrieve the Arrow schema of a table.
        /// </summary>
        private async Task<Schema> GetArrowSchemaAsync(byte[] commandJson)
        {
            var descriptor = FlightDescriptor.CreateCommandDescriptor(commandJson);
            Schema schema = await _client.GetSchema(descriptor).ResponseAsync.ConfigureAwait(false);
            return schema;
        }

        // ------------------------------------------------------------------ //
        //  DoAction  ->  health, create_table, execute_dml, upgrade_protocol
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Calls the Flight DoAction RPC and returns the first result body as a byte array.
        /// </summary>
        private async Task<byte[]> DoActionAsync(
            string actionType,
            byte[]? body = null,
            CancellationToken cancellationToken = default)
        {
            var action = new FlightAction(actionType, ByteString.CopyFrom(body ?? System.Array.Empty<byte>()));
            var call = _client.DoAction(action);

            if (await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
            {
                return call.ResponseStream.Current.Body.ToByteArray();
            }

            return System.Array.Empty<byte>();
        }

        // ------------------------------------------------------------------ //
        //  DoPut  ->  write table data
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Calls the Flight DoPut RPC to stream <see cref="RecordBatch"/> data
        /// to the server for writing as a Delta table.
        /// </summary>
        private async Task DoPutAsync(
            byte[] commandJson,
            RecordBatch batch,
            CancellationToken cancellationToken = default)
        {
            var descriptor = FlightDescriptor.CreateCommandDescriptor(commandJson);
            var call = _client.StartPut(descriptor);

            await call.RequestStream.WriteAsync(batch).ConfigureAwait(false);
            await call.RequestStream.CompleteAsync().ConfigureAwait(false);
            await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Calls the Flight DoPut RPC to stream multiple <see cref="RecordBatch"/>
        /// objects to the server.  Each batch is sent individually over the
        /// Flight wire, allowing the server to process them incrementally
        /// without materialising the entire dataset in memory.
        /// </summary>
        /// <returns>
        /// The raw metadata bytes returned by the server after the stream
        /// completes, or <c>null</c> if the server did not send metadata.
        /// </returns>
        private async Task<byte[]?> DoPutStreamingAsync(
            byte[] commandJson,
            Schema schema,
            IAsyncEnumerable<RecordBatch> batches,
            CancellationToken cancellationToken = default)
        {
            var descriptor = FlightDescriptor.CreateCommandDescriptor(commandJson);
            var call = _client.StartPut(descriptor);

            await foreach (RecordBatch batch in batches.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                await call.RequestStream.WriteAsync(batch).ConfigureAwait(false);
            }

            await call.RequestStream.CompleteAsync().ConfigureAwait(false);

            if (await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
            {
                FlightPutResult putResult = call.ResponseStream.Current;
                ByteString appMetadata = putResult.ApplicationMetadata;
                if (appMetadata != null && !appMetadata.IsEmpty)
                {
                    return appMetadata.ToByteArray();
                }
            }

            return null;
        }

        // ------------------------------------------------------------------ //
        //  Helper methods
        // ------------------------------------------------------------------ //

        private static byte[] BuildReadCommand(string path, StorageConfig? storageConfig, GenericStorageOptions? genericStorageOptions, long? numRows = null, int? batchSize = null, long? version = null)
        {
            var cmd = new Dictionary<string, object>
            {
                ["path"] = path,
            };
            AddStorageConfig(cmd, storageConfig, genericStorageOptions);
            if (numRows.HasValue)
            {
                cmd["num_rows"] = numRows.Value;
            }
            if (batchSize.HasValue)
            {
                cmd["batch_size"] = batchSize.Value;
            }
            if (version.HasValue)
            {
                cmd["version"] = version.Value;
            }
            return JsonSerializer.SerializeToUtf8Bytes(cmd);
        }

        private static void AddStorageConfig(Dictionary<string, object> cmd, StorageConfig? storageConfig, GenericStorageOptions? genericStorageOptions)
        {
            if (storageConfig != null)
            {
                cmd["storage_account"] = storageConfig.StorageAccount;
                cmd["sas_token"] = storageConfig.SasToken;
                if (!storageConfig.EvictFileSystemCache)
                {
                    cmd["evict_fs_cache"] = false;
                }
            }

            if (genericStorageOptions != null && genericStorageOptions.Options.Count > 0)
            {
                var storageOptions = new Dictionary<string, string>(StringComparer.Ordinal);
                foreach (KeyValuePair<string, string> option in genericStorageOptions.Options)
                {
                    storageOptions[option.Key] = option.Value;
                }

                cmd["storage_options"] = storageOptions;
            }
        }

        private static ExecuteResult ParseExecuteResult(byte[] jsonBytes)
        {
            using JsonDocument doc = JsonDocument.Parse(jsonBytes);
            JsonElement root = doc.RootElement;

            bool success = root.TryGetProperty("success", out JsonElement successEl) && successEl.GetBoolean();
            string message = root.TryGetProperty("message", out JsonElement msgEl) ? msgEl.GetString() ?? "" : "";

            var rows = new List<Dictionary<string, object?>>();
            if (root.TryGetProperty("result", out JsonElement resultEl) && resultEl.ValueKind == JsonValueKind.Array)
            {
                foreach (JsonElement rowEl in resultEl.EnumerateArray())
                {
                    var dict = new Dictionary<string, object?>();
                    foreach (JsonProperty prop in rowEl.EnumerateObject())
                    {
                        dict[prop.Name] = JsonElementToObject(prop.Value);
                    }
                    rows.Add(dict);
                }
            }

            return new ExecuteResult(success, message, rows);
        }

        private static object? JsonElementToObject(JsonElement element)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.String:
                    return element.GetString();
                case JsonValueKind.Number:
                    if (element.TryGetInt64(out long l)) return l;
                    return element.GetDouble();
                case JsonValueKind.True:
                    return true;
                case JsonValueKind.False:
                    return false;
                case JsonValueKind.Null:
                case JsonValueKind.Undefined:
                    return null;
                default:
                    return element.GetRawText();
            }
        }

        // ------------------------------------------------------------------ //
        //  IDisposable
        // ------------------------------------------------------------------ //

        public void Dispose()
        {
            _channel?.Dispose();
        }
    }
}
