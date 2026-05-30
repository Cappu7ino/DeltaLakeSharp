using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
        private static readonly NativeMethods.NativeAsyncOperationCompletedCallback NativeAsyncOperationCompleted = OnNativeAsyncOperationCompleted;

        private delegate IntPtr StartNativeAsyncOperationWithCallback(
            IntPtr engine,
            string commandJson,
            NativeMethods.NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        private delegate IntPtr StartNativeAsyncOperationWithSourceStreamCallback(
            IntPtr engine,
            string commandJson,
            IntPtr sourceStream,
            NativeMethods.NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        private readonly NativeEngineHandle _engine;
        private readonly bool _enableReadPrefetch;

        internal NativeRustBackend(DeltaTableServiceClientOptions? options = null)
        {
            _engine = NativeEngineHandle.Create();
            _enableReadPrefetch = options?.EnableNativeReadPrefetch ?? false;
        }

        public Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bool healthy = NativeMethods.HealthCheck(_engine.DangerousGetHandle()) == 1;
            return Task.FromResult(healthy);
        }

        public async Task<TableSchema> GetSchemaAsync(
            string path,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            long? version = null,
            CancellationToken cancellationToken = default)
        {
            Schema arrowSchema = await GetArrowSchemaAsync(
                path,
                storageConfig,
                genericStorageOptions,
                version,
                cancellationToken).ConfigureAwait(false);

            return ArrowConverter.ToTableSchema(arrowSchema);
        }

        public Task<Schema> GetArrowSchemaAsync(
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
            return ExecuteNativeOperationAsync(
                nameof(GetArrowSchemaAsync),
                "get_schema",
                commandJson,
                NativeMethods.GetSchemaAsyncWithCallback,
                operation => TakeNativeAsyncSchemaResult(operation, "get_schema"),
                cancellationToken);
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
            string commandJson = BuildReadTableCommandJson(path, storageConfig, genericStorageOptions, numRows, batchSize, version, _enableReadPrefetch);
            return await ExecuteNativeStreamOperationAsync(
                nameof(OpenReadTableStreamAsync),
                "read_table",
                commandJson,
                NativeMethods.ReadTableAsyncWithCallback,
                cancellationToken).ConfigureAwait(false);
        }

        public async Task<IReadOnlyList<DeltaReadPartition>> GetReadPartitionsAsync(
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
            using var context = new NativeAsyncOperationContext<IReadOnlyList<DeltaReadPartition>>(
                nameof(GetReadPartitionsAsync),
                "plan_read_partitions",
                cancellationToken,
                static operation => TakeNativeAsyncJsonResult(
                    operation,
                    "plan_read_partitions",
                    ParseReadPartitions));
            IntPtr operationPtr = NativeMethods.PlanReadPartitionsAsyncWithCallback(
                _engine.DangerousGetHandle(),
                commandJson,
                NativeAsyncOperationCompleted,
                context.UserData);
            if (operationPtr == IntPtr.Zero)
            {
                throw CreateNativeOperationFailedException(nameof(GetReadPartitionsAsync));
            }

            context.SetOperation(operationPtr);
            context.RegisterCancellation();
            return await context.Task.ConfigureAwait(false);
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
            string commandJson = BuildReadTablePartitionCommandJson(path, partition, storageConfig, genericStorageOptions, batchSize, _enableReadPrefetch);
            return ExecuteNativeStreamOperationAsync(
                nameof(OpenReadTablePartitionStreamAsync),
                "read_table_partition",
                commandJson,
                NativeMethods.ReadTablePartitionAsyncWithCallback,
                cancellationToken);
        }

        public Task<ArrowStreamResult> OpenReadTablePartitionStreamByTokenAsync(
            string path,
            string partitionToken,
            StorageConfig? storageConfig = null,
            GenericStorageOptions? genericStorageOptions = null,
            int? batchSize = null,
            CancellationToken cancellationToken = default)
        {
            string commandJson = BuildReadTablePartitionCommandJson(path, partitionToken, storageConfig, genericStorageOptions, batchSize, _enableReadPrefetch);
            return ExecuteNativeStreamOperationAsync(
                nameof(OpenReadTablePartitionStreamByTokenAsync),
                "read_table_partition",
                commandJson,
                NativeMethods.ReadTablePartitionAsyncWithCallback,
                cancellationToken);
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
            string commandJson = BuildExecuteQueryCommandJson(sql, tablePath, tableName, storageConfig, genericStorageOptions, batchSize, version, _enableReadPrefetch);
            return await ExecuteNativeStreamOperationAsync(
                nameof(OpenExecuteQueryStreamAsync),
                "execute_query",
                commandJson,
                NativeMethods.ExecuteQueryAsyncWithCallback,
                cancellationToken).ConfigureAwait(false);
        }

        public async Task<ExecuteResult> CreateEmptyTableAsync(
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
            return await ExecuteNativeJsonOperationAsync(
                nameof(CreateEmptyTableAsync),
                "create_table",
                commandJson,
                NativeMethods.CreateTableAsyncWithCallback,
                ParseExecuteResult,
                cancellationToken).ConfigureAwait(false);
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

        public Task<DeltaDistributedWriteSession> BeginDistributedWriteAsync(
            string path,
            DeltaDistributedWriteOptions options,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string commandJson = BuildBeginDistributedWriteCommandJson(path, options, storageConfig);
            return ExecuteNativeJsonOperationAsync(
                nameof(BeginDistributedWriteAsync),
                "begin_distributed_write",
                commandJson,
                NativeMethods.BeginDistributedWriteAsyncWithCallback,
                ParseDistributedWriteSession,
                cancellationToken);
        }

        public Task<DeltaStagedWriteResult> StageDistributedWriteAsync(
            DeltaDistributedWriteSession session,
            Schema schema,
            IAsyncEnumerable<RecordBatch> batches,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string commandJson = BuildDistributedWriteSessionCommandJson(session, storageConfig);
            return StageDistributedWriteCoreAsync(schema, batches, commandJson, cancellationToken);
        }

        public Task<ExecuteResult> CommitDistributedWriteAsync(
            DeltaDistributedWriteSession session,
            DeltaDistributedCommitOptions? options = null,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string commandJson = BuildCommitDistributedWriteCommandJson(session, options, storageConfig);
            return ExecuteNativeJsonOperationAsync(
                nameof(CommitDistributedWriteAsync),
                "commit_distributed_write",
                commandJson,
                NativeMethods.CommitDistributedWriteAsyncWithCallback,
                ParseExecuteResult,
                cancellationToken);
        }

        public Task<ExecuteResult> AbortDistributedWriteAsync(
            DeltaDistributedWriteSession session,
            StorageConfig? storageConfig = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string commandJson = BuildDistributedWriteSessionCommandJson(session, storageConfig);
            return ExecuteNativeJsonOperationAsync(
                nameof(AbortDistributedWriteAsync),
                "abort_distributed_write",
                commandJson,
                NativeMethods.AbortDistributedWriteAsyncWithCallback,
                ParseExecuteResult,
                cancellationToken);
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

        public async Task<ExecuteResult> UpgradeTableProtocolAsync(
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
            return await ExecuteNativeJsonOperationAsync(
                nameof(UpgradeTableProtocolAsync),
                "upgrade_protocol",
                commandJson,
                NativeMethods.UpgradeProtocolAsyncWithCallback,
                ParseExecuteResult,
                cancellationToken).ConfigureAwait(false);
        }

        public void Dispose()
        {
            _engine.Dispose();
        }

        private InvalidOperationException CreateNativeOperationFailedException(string operation)
        {
            NativeErrorInfo error = GetLastError();
            string message = $"Native V3 backend operation '{operation}' failed.";

            if (error.RawCode != (int)NativeServiceErrorCode.Ok)
            {
                message += $" Native error code: {FormatNativeErrorCode(error)}.";
            }

            if (!string.IsNullOrWhiteSpace(error.Message))
            {
                message += $" Native error: {error.Message}";
            }

            return new InvalidOperationException(message);
        }

        private static InvalidOperationException CreateNativeAsyncOperationFailedException(
            IntPtr operation,
            string operationName)
        {
            NativeErrorInfo error = GetAsyncOperationError(operation);
            string message = $"Native V3 backend operation '{operationName}' failed.";

            if (error.RawCode != (int)NativeServiceErrorCode.Ok)
            {
                message += $" Native error code: {FormatNativeErrorCode(error)}.";
            }

            if (!string.IsNullOrWhiteSpace(error.Message))
            {
                message += $" Native error: {error.Message}";
            }

            return new InvalidOperationException(message);
        }

        private NativeErrorInfo GetLastError()
        {
            IntPtr engine = _engine.DangerousGetHandle();
            return new NativeErrorInfo(
                NativeMethods.GetLastErrorCode(engine),
                NativeMethods.PtrToStringUtf8(NativeMethods.GetLastError(engine)));
        }

        private static NativeErrorInfo GetAsyncOperationError(IntPtr operation)
        {
            return new NativeErrorInfo(
                NativeMethods.AsyncOperationGetErrorCode(operation),
                NativeMethods.PtrToStringUtf8(NativeMethods.AsyncOperationGetError(operation)));
        }

        private static string FormatNativeErrorCode(NativeErrorInfo error)
        {
            return error.HasKnownCode
                ? $"{error.RawCode} ({error.Code})"
                : $"{error.RawCode} (Unknown)";
        }

        private static void OnNativeAsyncOperationCompleted(IntPtr operation, IntPtr userData)
        {
            if (userData == IntPtr.Zero)
            {
                return;
            }

            GCHandle handle = GCHandle.FromIntPtr(userData);
            if (handle.Target is INativeAsyncOperationContext context)
            {
                ThreadPool.QueueUserWorkItem(
                    static state =>
                    {
                        var callbackState = (NativeAsyncOperationCallbackState)state!;
                        callbackState.Context.Complete(callbackState.Operation);
                    },
                    new NativeAsyncOperationCallbackState(context, operation));
            }
        }

        private async Task<T> ExecuteNativeJsonOperationAsync<T>(
            string operationName,
            string nativeOperationName,
            string commandJson,
            StartNativeAsyncOperationWithCallback startOperation,
            Func<string, T> parseResult,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var context = new NativeAsyncOperationContext<T>(
                operationName,
                nativeOperationName,
                cancellationToken,
                operation => TakeNativeAsyncJsonResult(operation, nativeOperationName, parseResult));
            IntPtr operationPtr = startOperation(
                _engine.DangerousGetHandle(),
                commandJson,
                NativeAsyncOperationCompleted,
                context.UserData);
            if (operationPtr == IntPtr.Zero)
            {
                throw CreateNativeOperationFailedException(operationName);
            }

            context.SetOperation(operationPtr);
            context.RegisterCancellation();
            return await context.Task.ConfigureAwait(false);
        }

        private static T TakeNativeAsyncJsonResult<T>(
            IntPtr operation,
            string nativeOperationName,
            Func<string, T> parseResult)
        {
            IntPtr resultPtr = NativeMethods.AsyncOperationTakeResult(operation);
            if (resultPtr == IntPtr.Zero)
            {
                throw new InvalidOperationException($"Native async {nativeOperationName} returned null JSON.");
            }

            try
            {
                string resultJson = NativeMethods.PtrToStringUtf8(resultPtr)
                    ?? throw new InvalidOperationException($"Native async {nativeOperationName} returned null JSON.");
                return parseResult(resultJson);
            }
            finally
            {
                NativeMethods.FreeString(resultPtr);
            }
        }

        private async Task<ArrowStreamResult> ExecuteNativeStreamOperationAsync(
            string operationName,
            string nativeOperationName,
            string commandJson,
            StartNativeAsyncOperationWithCallback startOperation,
            CancellationToken cancellationToken)
        {
            IArrowArrayStream stream = await ExecuteNativeOperationAsync(
                operationName,
                nativeOperationName,
                commandJson,
                startOperation,
                operation => TakeNativeAsyncStreamResult(operation, nativeOperationName),
                cancellationToken).ConfigureAwait(false);

            return new ArrowStreamResult(stream.Schema, stream);
        }

        private async Task<T> ExecuteNativeOperationAsync<T>(
            string operationName,
            string nativeOperationName,
            string commandJson,
            StartNativeAsyncOperationWithCallback startOperation,
            Func<IntPtr, T> takeResult,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var context = new NativeAsyncOperationContext<T>(
                operationName,
                nativeOperationName,
                cancellationToken,
                takeResult);
            IntPtr operationPtr = startOperation(
                _engine.DangerousGetHandle(),
                commandJson,
                NativeAsyncOperationCompleted,
                context.UserData);
            if (operationPtr == IntPtr.Zero)
            {
                throw CreateNativeOperationFailedException(operationName);
            }

            context.SetOperation(operationPtr);
            context.RegisterCancellation();
            return await context.Task.ConfigureAwait(false);
        }

        private async Task<T> ExecuteNativeSourceStreamJsonOperationAsync<T>(
            string operationName,
            string nativeOperationName,
            string commandJson,
            IntPtr sourceStream,
            StartNativeAsyncOperationWithSourceStreamCallback startOperation,
            Func<string, T> parseResult,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var context = new NativeAsyncOperationContext<T>(
                operationName,
                nativeOperationName,
                cancellationToken,
                operation => TakeNativeAsyncJsonResult(operation, nativeOperationName, parseResult));
            IntPtr operationPtr = startOperation(
                _engine.DangerousGetHandle(),
                commandJson,
                sourceStream,
                NativeAsyncOperationCompleted,
                context.UserData);
            if (operationPtr == IntPtr.Zero)
            {
                throw CreateNativeOperationFailedException(operationName);
            }

            context.SetOperation(operationPtr);
            context.RegisterCancellation();
            return await context.Task.ConfigureAwait(false);
        }

        private static unsafe IArrowArrayStream TakeNativeAsyncStreamResult(
            IntPtr operation,
            string nativeOperationName)
        {
            CArrowArrayStream* streamPtr = CArrowArrayStream.Create();
            try
            {
                int result = NativeMethods.AsyncOperationTakeStream(operation, streamPtr);
                if (result != 1)
                {
                    throw new InvalidOperationException($"Native async {nativeOperationName} returned null stream.");
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

        private static unsafe Schema TakeNativeAsyncSchemaResult(
            IntPtr operation,
            string nativeOperationName)
        {
            CArrowSchema* schemaPtr = CArrowSchema.Create();
            try
            {
                int result = NativeMethods.AsyncOperationTakeSchema(operation, schemaPtr);
                if (result != 1)
                {
                    throw new InvalidOperationException($"Native async {nativeOperationName} returned null schema.");
                }

                return CArrowSchemaImporter.ImportSchema(schemaPtr);
            }
            finally
            {
                CArrowSchema.Free(schemaPtr);
            }
        }

        /// <summary>
        /// Exports a managed Arrow stream to the native Rust backend using the
        /// Arrow C Stream interface.
        /// </summary>
        private async Task InsertCoreAsync(
            Schema schema,
            IAsyncEnumerable<RecordBatch> batches,
            string commandJson,
            CancellationToken cancellationToken)
        {
            IArrowArrayStream stream = new AsyncEnumerableArrowArrayStream(schema, batches, cancellationToken);
            IntPtr streamPtr = CreateNativeArrowArrayStream();
            try
            {
                ExportArrowArrayStream(stream, streamPtr);
                await ExecuteNativeSourceStreamJsonOperationAsync(
                    nameof(InsertAsync),
                    "insert",
                    commandJson,
                    streamPtr,
                    NativeMethods.InsertAsyncWithCallback,
                    ParseExecuteResult,
                    cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                stream.Dispose();
                FreeNativeArrowArrayStream(streamPtr);
            }
        }

        private static unsafe IntPtr CreateNativeArrowArrayStream()
        {
            return (IntPtr)CArrowArrayStream.Create();
        }

        private static unsafe void ExportArrowArrayStream(IArrowArrayStream stream, IntPtr streamPtr)
        {
            CArrowArrayStreamExporter.ExportArrayStream(stream, (CArrowArrayStream*)streamPtr);
        }

        private static unsafe void FreeNativeArrowArrayStream(IntPtr streamPtr)
        {
            CArrowArrayStream.Free((CArrowArrayStream*)streamPtr);
        }

        /// <summary>
        /// Exports a managed Arrow stream to the native Rust merge entrypoint.
        /// This mirrors the insert path but returns the standard action result
        /// envelope containing merge metrics.
        /// </summary>
        private async Task<ExecuteResult> MergeCoreAsync(
            Schema schema,
            IAsyncEnumerable<RecordBatch> batches,
            string commandJson,
            CancellationToken cancellationToken)
        {
            IArrowArrayStream stream = new AsyncEnumerableArrowArrayStream(schema, batches, cancellationToken);
            IntPtr streamPtr = CreateNativeArrowArrayStream();
            try
            {
                ExportArrowArrayStream(stream, streamPtr);
                return await ExecuteNativeSourceStreamJsonOperationAsync(
                    nameof(MergeDataAsync),
                    "merge_stream",
                    commandJson,
                    streamPtr,
                    NativeMethods.MergeStreamAsyncWithCallback,
                    ParseExecuteResult,
                    cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                stream.Dispose();
                FreeNativeArrowArrayStream(streamPtr);
            }
        }

        private async Task<DeltaStagedWriteResult> StageDistributedWriteCoreAsync(
            Schema schema,
            IAsyncEnumerable<RecordBatch> batches,
            string commandJson,
            CancellationToken cancellationToken)
        {
            IArrowArrayStream stream = new AsyncEnumerableArrowArrayStream(schema, batches, cancellationToken);
            IntPtr streamPtr = CreateNativeArrowArrayStream();
            try
            {
                ExportArrowArrayStream(stream, streamPtr);
                return await ExecuteNativeSourceStreamJsonOperationAsync(
                    nameof(StageDistributedWriteAsync),
                    "stage_distributed_write",
                    commandJson,
                    streamPtr,
                    NativeMethods.StageDistributedWriteAsyncWithCallback,
                    ParseStagedWriteResult,
                    cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                stream.Dispose();
                FreeNativeArrowArrayStream(streamPtr);
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

            return ExecuteNativeJsonOperationAsync(
                nameof(ExecuteDmlAsync),
                "execute_dml",
                commandJson,
                NativeMethods.ExecuteDmlAsyncWithCallback,
                ParseExecuteResult,
                cancellationToken);
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
            long? version,
            bool enableReadPrefetch)
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
            AddReadPrefetch(command, enableReadPrefetch);

            return JsonSerializer.Serialize(command);
        }

        private static string BuildBeginDistributedWriteCommandJson(
            string path,
            DeltaDistributedWriteOptions options,
            StorageConfig? storageConfig)
        {
            if (options.RunId == Guid.Empty)
            {
                throw new ArgumentException("Distributed write options must include a caller-provided run ID.", nameof(options));
            }

            var command = new Dictionary<string, object>
            {
                ["path"] = path,
                ["run_id"] = options.RunId.ToString("D"),
                ["mode"] = options.Mode == SaveMode.Append ? "append" : "overwrite",
                ["table_disposition"] = ToDistributedWriteTableDispositionValue(options.TableDisposition),
                ["overwrite_scope"] = ToDistributedOverwriteScopeValue(options.OverwriteScope),
            };

            if (options.SchemaMode.HasValue)
            {
                command["schema_mode"] = options.SchemaMode.Value.ToString().ToLowerInvariant();
            }
            if (options.TableSchema != null)
            {
                command["schema"] = BuildSchemaPayload(options.TableSchema);
            }
            if (options.Configuration != null && options.Configuration.Count > 0)
            {
                command["configuration"] = options.Configuration;
            }
            if (options.PartitionBy != null && options.PartitionBy.Count > 0)
            {
                command["partition_by"] = options.PartitionBy;
            }
            if (!string.IsNullOrWhiteSpace(options.StagingPrefix))
            {
                command["staging_prefix"] = options.StagingPrefix!;
            }
            if (options.MaxBufferedBytes.HasValue)
            {
                command["max_buffered_bytes"] = options.MaxBufferedBytes.Value;
            }
            if (options.MaxBufferedRecordBatches.HasValue)
            {
                command["max_buffered_record_batches"] = options.MaxBufferedRecordBatches.Value;
            }

            AddStorageConfig(command, storageConfig, null);
            return JsonSerializer.Serialize(command);
        }

        private static string BuildDistributedWriteSessionCommandJson(
            DeltaDistributedWriteSession session,
            StorageConfig? storageConfig)
        {
            var command = new Dictionary<string, object>
            {
                ["path"] = session.TablePath,
                ["run_id"] = session.RunId.ToString("D"),
                ["mode"] = session.Mode == SaveMode.Append ? "append" : "overwrite",
                ["table_disposition"] = ToDistributedWriteTableDispositionValue(session.TableDisposition),
                ["overwrite_scope"] = ToDistributedOverwriteScopeValue(session.OverwriteScope),
                ["staging_prefix"] = session.StagingPrefix,
            };

            if (session.SchemaMode.HasValue)
            {
                command["schema_mode"] = session.SchemaMode.Value.ToString().ToLowerInvariant();
            }
            if (session.PartitionBy.Count > 0)
            {
                command["partition_by"] = session.PartitionBy;
            }
            if (session.MaxBufferedBytes.HasValue)
            {
                command["max_buffered_bytes"] = session.MaxBufferedBytes.Value;
            }
            if (session.MaxBufferedRecordBatches.HasValue)
            {
                command["max_buffered_record_batches"] = session.MaxBufferedRecordBatches.Value;
            }

            AddStorageConfig(command, storageConfig, null);
            return JsonSerializer.Serialize(command);
        }

        private static string BuildCommitDistributedWriteCommandJson(
            DeltaDistributedWriteSession session,
            DeltaDistributedCommitOptions? options,
            StorageConfig? storageConfig)
        {
            var command = new Dictionary<string, object>
            {
                ["path"] = session.TablePath,
                ["run_id"] = session.RunId.ToString("D"),
                ["mode"] = session.Mode == SaveMode.Append ? "append" : "overwrite",
                ["table_disposition"] = ToDistributedWriteTableDispositionValue(session.TableDisposition),
                ["overwrite_scope"] = ToDistributedOverwriteScopeValue(session.OverwriteScope),
                ["staging_prefix"] = session.StagingPrefix,
            };

            if (session.SchemaMode.HasValue)
            {
                command["schema_mode"] = session.SchemaMode.Value.ToString().ToLowerInvariant();
            }
            if (session.PartitionBy.Count > 0)
            {
                command["partition_by"] = session.PartitionBy;
            }
            if (options?.ExpectedVersion != null)
            {
                command["expected_version"] = options.ExpectedVersion.Value;
            }
            if (options != null)
            {
                command["cleanup_staging_artifacts"] = options.CleanupStagingArtifacts;
            }

            AddStorageConfig(command, storageConfig, null);
            return JsonSerializer.Serialize(command);
        }

        private static string ToDistributedWriteTableDispositionValue(DistributedWriteTableDisposition disposition)
        {
            return disposition switch
            {
                DistributedWriteTableDisposition.ExistingTable => "existingTable",
                DistributedWriteTableDisposition.CreateIfMissing => "createIfMissing",
                DistributedWriteTableDisposition.CreateOrReplace => "createOrReplace",
                _ => throw new ArgumentOutOfRangeException(nameof(disposition), disposition, "Unknown distributed write table disposition."),
            };
        }

        private static string ToDistributedOverwriteScopeValue(DistributedOverwriteScope overwriteScope)
        {
            return overwriteScope switch
            {
                DistributedOverwriteScope.FullTable => "fullTable",
                DistributedOverwriteScope.TouchedPartitions => "touchedPartitions",
                _ => throw new ArgumentOutOfRangeException(nameof(overwriteScope), overwriteScope, "Unknown distributed overwrite scope."),
            };
        }

        private static string BuildReadTablePartitionCommandJson(
            string path,
            DeltaReadPartition partition,
            StorageConfig? storageConfig,
            GenericStorageOptions? genericStorageOptions,
            int? batchSize,
            bool enableReadPrefetch)
        {
            return BuildReadTablePartitionCommandJson(path, partition.Token, storageConfig, genericStorageOptions, batchSize, enableReadPrefetch);
        }

        private static string BuildReadTablePartitionCommandJson(
            string path,
            string partitionToken,
            StorageConfig? storageConfig,
            GenericStorageOptions? genericStorageOptions,
            int? batchSize,
            bool enableReadPrefetch)
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
            AddReadPrefetch(command, enableReadPrefetch);

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

        private static DeltaDistributedWriteSession ParseDistributedWriteSession(string json)
        {
            JsonNode root = JsonNode.Parse(json)
                ?? throw new InvalidOperationException("Native backend returned invalid JSON for distributed write begin.");

            if (!(root["success"]?.GetValue<bool>() ?? false))
            {
                string message = root["message"]?.GetValue<string>() ?? "Distributed write begin failed.";
                throw new InvalidOperationException(message);
            }

            if (root["result"] is not JsonArray resultArray || resultArray.Count == 0 || resultArray[0] is not JsonObject obj)
            {
                throw new InvalidOperationException("Distributed write begin result is missing session data.");
            }

            string runId = obj["runId"]?.GetValue<string>()
                ?? throw new InvalidOperationException("Distributed write session is missing runId.");
            if (!Guid.TryParseExact(runId, "D", out Guid parsedRunId))
            {
                throw new InvalidOperationException("Distributed write session returned an invalid runId.");
            }
            string tablePath = obj["tablePath"]?.GetValue<string>()
                ?? throw new InvalidOperationException("Distributed write session is missing tablePath.");
            string mode = obj["mode"]?.GetValue<string>() ?? "append";
            string? schemaMode = obj["schemaMode"]?.GetValue<string>();
            string tableDisposition = obj["tableDisposition"]?.GetValue<string>() ?? "existingTable";
            string overwriteScope = obj["overwriteScope"]?.GetValue<string>() ?? "fullTable";
            string stagingPrefix = obj["stagingPrefix"]?.GetValue<string>() ?? "_staging";
            long? maxBufferedBytes = obj["maxBufferedBytes"]?.GetValue<long>();
            int? maxBufferedRecordBatches = obj["maxBufferedRecordBatches"]?.GetValue<int>();
            var partitionBy = new List<string>();
            if (obj["partitionBy"] is JsonArray partitionArray)
            {
                foreach (JsonNode? item in partitionArray)
                {
                    if (item != null)
                    {
                        partitionBy.Add(item.GetValue<string>());
                    }
                }
            }

            return new DeltaDistributedWriteSession(
                parsedRunId,
                tablePath,
                ParseSaveMode(mode),
                ParseWriteSchemaMode(schemaMode),
                ParseDistributedWriteTableDisposition(tableDisposition),
                ParseDistributedOverwriteScope(overwriteScope),
                stagingPrefix,
                partitionBy,
                maxBufferedBytes,
                maxBufferedRecordBatches);
        }

        private static DeltaStagedWriteResult ParseStagedWriteResult(string json)
        {
            JsonNode root = JsonNode.Parse(json)
                ?? throw new InvalidOperationException("Native backend returned invalid JSON for distributed write staging.");

            if (!(root["success"]?.GetValue<bool>() ?? false))
            {
                string message = root["message"]?.GetValue<string>() ?? "Distributed write staging failed.";
                throw new InvalidOperationException(message);
            }

            if (root["result"] is not JsonArray resultArray || resultArray.Count == 0 || resultArray[0] is not JsonObject obj)
            {
                throw new InvalidOperationException("Distributed write staging result is missing summary data.");
            }

            string runId = obj["runId"]?.GetValue<string>()
                ?? throw new InvalidOperationException("Distributed write staging result is missing runId.");
            if (!Guid.TryParseExact(runId, "D", out Guid parsedRunId))
            {
                throw new InvalidOperationException("Distributed write staging result returned an invalid runId.");
            }

            string stagingPrefix = obj["stagingPrefix"]?.GetValue<string>() ?? "_staging";
            int artifactCount = obj["artifactCount"]?.GetValue<int>() ?? 0;
            long addedFileCount = obj["addedFileCount"]?.GetValue<long>() ?? 0;
            long totalDataFileBytes = obj["totalDataFileBytes"]?.GetValue<long>() ?? 0;

            return new DeltaStagedWriteResult(
                parsedRunId,
                stagingPrefix,
                artifactCount,
                addedFileCount,
                totalDataFileBytes);
        }

        private static SaveMode ParseSaveMode(string value)
        {
            return value switch
            {
                "append" => SaveMode.Append,
                "overwrite" => SaveMode.Overwrite,
                _ => throw new InvalidOperationException($"Distributed write session returned unsupported mode '{value}'."),
            };
        }

        private static WriteSchemaMode? ParseWriteSchemaMode(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return null;
            }

            return value switch
            {
                "merge" => WriteSchemaMode.Merge,
                "overwrite" => WriteSchemaMode.Overwrite,
                _ => throw new InvalidOperationException($"Distributed write session returned unsupported schema mode '{value}'."),
            };
        }

        private static DistributedWriteTableDisposition ParseDistributedWriteTableDisposition(string value)
        {
            return value switch
            {
                "existingTable" => DistributedWriteTableDisposition.ExistingTable,
                "createIfMissing" => DistributedWriteTableDisposition.CreateIfMissing,
                "createOrReplace" => DistributedWriteTableDisposition.CreateOrReplace,
                _ => throw new InvalidOperationException($"Distributed write session returned unsupported table disposition '{value}'."),
            };
        }

        private static DistributedOverwriteScope ParseDistributedOverwriteScope(string value)
        {
            return value switch
            {
                "fullTable" => DistributedOverwriteScope.FullTable,
                "touchedPartitions" => DistributedOverwriteScope.TouchedPartitions,
                _ => throw new InvalidOperationException($"Distributed write session returned unsupported overwrite scope '{value}'."),
            };
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
            AddReadPrefetch(command, _enableReadPrefetch);

            string commandJson = JsonSerializer.Serialize(command);
            return ExecuteNativeStreamOperationAsync(
                nameof(OpenChangeDataCoreStreamAsync),
                "read_change_data",
                commandJson,
                NativeMethods.ReadChangeDataAsyncWithCallback,
                cancellationToken);
        }

        private static string BuildExecuteQueryCommandJson(
            string sql,
            string? tablePath,
            string? tableName,
            StorageConfig? storageConfig,
            GenericStorageOptions? genericStorageOptions,
            int? batchSize,
            long? version,
            bool enableReadPrefetch)
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
            AddReadPrefetch(command, enableReadPrefetch);

            return JsonSerializer.Serialize(command);
        }

        private static void AddReadPrefetch(IDictionary<string, object> command, bool enableReadPrefetch)
        {
            if (enableReadPrefetch)
            {
                command["read_prefetch"] = true;
            }
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

        private interface INativeAsyncOperationContext
        {
            void Complete(IntPtr operation);
        }

        private sealed class NativeAsyncOperationCallbackState
        {
            public NativeAsyncOperationCallbackState(INativeAsyncOperationContext context, IntPtr operation)
            {
                Context = context;
                Operation = operation;
            }

            public INativeAsyncOperationContext Context { get; }

            public IntPtr Operation { get; }
        }

        private sealed class NativeAsyncOperationContext<T> : INativeAsyncOperationContext, IDisposable
        {
            private readonly string _operationName;
            private readonly string _nativeOperationName;
            private readonly CancellationToken _cancellationToken;
            private readonly TaskCompletionSource<T> _completion;
            private readonly GCHandle _selfHandle;
            private readonly Func<IntPtr, T> _takeResult;
            private NativeAsyncOperationHandle? _operation;
            private CancellationTokenRegistration _cancellationRegistration;
            private int _completed;
            private int _disposed;

            public NativeAsyncOperationContext(
                string operationName,
                string nativeOperationName,
                CancellationToken cancellationToken,
                Func<IntPtr, T> takeResult)
            {
                _operationName = operationName;
                _nativeOperationName = nativeOperationName;
                _cancellationToken = cancellationToken;
                _completion = new TaskCompletionSource<T>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                _selfHandle = GCHandle.Alloc(this);
                _takeResult = takeResult;
            }

            public Task<T> Task => _completion.Task;

            public IntPtr UserData => GCHandle.ToIntPtr(_selfHandle);

            public void SetOperation(IntPtr operation)
            {
                _operation = NativeAsyncOperationHandle.FromIntPtr(operation);
            }

            public void RegisterCancellation()
            {
                if (_cancellationToken.CanBeCanceled)
                {
                    _cancellationRegistration = _cancellationToken.Register(
                        static state => ((NativeAsyncOperationContext<T>)state!).Cancel(),
                        this);
                }
            }

            public void Complete(IntPtr operation)
            {
                if (Interlocked.Exchange(ref _completed, 1) != 0)
                {
                    return;
                }

                try
                {
                    int rawStatus = NativeMethods.AsyncOperationStatus(operation);
                    var status = (NativeAsyncOperationStatus)rawStatus;
                    switch (status)
                    {
                        case NativeAsyncOperationStatus.Succeeded:
                            _completion.TrySetResult(_takeResult(operation));
                            break;

                        case NativeAsyncOperationStatus.Failed:
                            if (_cancellationToken.IsCancellationRequested && IsNativeCancellationFailure(operation))
                            {
                                _completion.TrySetCanceled(_cancellationToken);
                                break;
                            }

                            _completion.TrySetException(CreateNativeAsyncOperationFailedException(
                                operation,
                                _operationName));
                            break;

                        case NativeAsyncOperationStatus.Cancelled:
                            if (_cancellationToken.CanBeCanceled)
                            {
                                _completion.TrySetCanceled(_cancellationToken);
                            }
                            else
                            {
                                _completion.TrySetCanceled();
                            }
                            break;

                        case NativeAsyncOperationStatus.Pending:
                            _completion.TrySetException(new InvalidOperationException(
                                $"Native async {_nativeOperationName} completion was signaled before terminal state was available."));
                            break;

                        default:
                            _completion.TrySetException(new InvalidOperationException(
                                $"Native async {_nativeOperationName} returned unknown status {rawStatus}."));
                            break;
                    }
                }
                catch (Exception ex)
                {
                    _completion.TrySetException(ex);
                }
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) != 0)
                {
                    return;
                }

                _cancellationRegistration.Dispose();
                _operation?.Dispose();
                _selfHandle.Free();
            }

            private void Cancel()
            {
                NativeAsyncOperationHandle? operation = _operation;
                if (operation != null && !operation.IsInvalid)
                {
                    NativeMethods.AsyncOperationCancel(operation.DangerousGetHandle());
                }
            }

            private static bool IsNativeCancellationFailure(IntPtr operation)
            {
                NativeErrorInfo errorInfo = GetAsyncOperationError(operation);
                if (errorInfo.Code == NativeServiceErrorCode.Cancelled)
                {
                    return true;
                }

                return errorInfo.Message != null
                    && errorInfo.Message.IndexOf("cancel", StringComparison.OrdinalIgnoreCase) >= 0;
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
