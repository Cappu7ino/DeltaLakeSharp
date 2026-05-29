// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#if NET8_0_OR_GREATER
using System;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using Apache.Arrow.C;

namespace DeltaLakeSharp.Client.Internal.Native
{
    internal static partial class NativeMethods
    {
        static NativeMethods()
        {
            NativeLibrary.SetDllImportResolver(
                typeof(NativeMethods).Assembly,
                ResolveNativeLibrary);
        }

        [LibraryImport(LibraryName, EntryPoint = "dts_create_engine")]
        internal static partial IntPtr CreateEngine();

        [LibraryImport(LibraryName, EntryPoint = "dts_destroy_engine")]
        internal static partial void DestroyEngine(IntPtr engine);

        [LibraryImport(LibraryName, EntryPoint = "dts_health_check")]
        [return: MarshalAs(UnmanagedType.I4)]
        internal static partial int HealthCheck(IntPtr engine);

        [LibraryImport(LibraryName, EntryPoint = "dts_get_last_error")]
        internal static partial IntPtr GetLastError(IntPtr engine);

        [DllImport(LibraryName, EntryPoint = "dts_get_schema_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr GetSchemaAsyncWithCallbackNative(
            IntPtr engine,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [LibraryImport(LibraryName, EntryPoint = "dts_read_table", StringMarshalling = StringMarshalling.Utf8)]
        [return: MarshalAs(UnmanagedType.I4)]
        internal static unsafe partial int ReadTable(IntPtr engine, string commandJson, CArrowArrayStream* stream);

        [DllImport(LibraryName, EntryPoint = "dts_read_table_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr ReadTableAsyncWithCallbackNative(
            IntPtr engine,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_execute_query_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr ExecuteQueryAsyncWithCallbackNative(
            IntPtr engine,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_read_change_data_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr ReadChangeDataAsyncWithCallbackNative(
            IntPtr engine,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_read_table_partition_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr ReadTablePartitionAsyncWithCallbackNative(
            IntPtr engine,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [LibraryImport(LibraryName, EntryPoint = "dts_plan_read_partitions", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial IntPtr PlanReadPartitions(IntPtr engine, string commandJson);

        [DllImport(LibraryName, EntryPoint = "dts_plan_read_partitions_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr PlanReadPartitionsAsyncWithCallbackNative(
            IntPtr engine,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_create_table_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr CreateTableAsyncWithCallbackNative(
            IntPtr engine,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_upgrade_protocol_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr UpgradeProtocolAsyncWithCallbackNative(
            IntPtr engine,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_execute_dml_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr ExecuteDmlAsyncWithCallbackNative(
            IntPtr engine,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_insert_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr InsertAsyncWithCallbackNative(
            IntPtr engine,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string commandJson,
            IntPtr sourceStream,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_merge_stream_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr MergeStreamAsyncWithCallbackNative(
            IntPtr engine,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string commandJson,
            IntPtr sourceStream,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        internal static IntPtr PlanReadPartitionsAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return PlanReadPartitionsAsyncWithCallbackNative(engine, commandJson, callback, userData);
        }

        internal static IntPtr GetSchemaAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return GetSchemaAsyncWithCallbackNative(engine, commandJson, callback, userData);
        }

        internal static IntPtr CreateTableAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return CreateTableAsyncWithCallbackNative(engine, commandJson, callback, userData);
        }

        internal static IntPtr UpgradeProtocolAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return UpgradeProtocolAsyncWithCallbackNative(engine, commandJson, callback, userData);
        }

        internal static IntPtr ExecuteDmlAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return ExecuteDmlAsyncWithCallbackNative(engine, commandJson, callback, userData);
        }

        internal static IntPtr ReadTableAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return ReadTableAsyncWithCallbackNative(engine, commandJson, callback, userData);
        }

        internal static IntPtr ExecuteQueryAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return ExecuteQueryAsyncWithCallbackNative(engine, commandJson, callback, userData);
        }

        internal static IntPtr ReadChangeDataAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return ReadChangeDataAsyncWithCallbackNative(engine, commandJson, callback, userData);
        }

        internal static IntPtr ReadTablePartitionAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return ReadTablePartitionAsyncWithCallbackNative(engine, commandJson, callback, userData);
        }

        internal static IntPtr InsertAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            IntPtr sourceStream,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return InsertAsyncWithCallbackNative(engine, commandJson, sourceStream, callback, userData);
        }

        internal static IntPtr MergeStreamAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            IntPtr sourceStream,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return MergeStreamAsyncWithCallbackNative(engine, commandJson, sourceStream, callback, userData);
        }

        [LibraryImport(LibraryName, EntryPoint = "dts_async_operation_status")]
        [return: MarshalAs(UnmanagedType.I4)]
        internal static partial int AsyncOperationStatus(IntPtr operation);

        [LibraryImport(LibraryName, EntryPoint = "dts_async_operation_take_result")]
        internal static partial IntPtr AsyncOperationTakeResult(IntPtr operation);

        [LibraryImport(LibraryName, EntryPoint = "dts_async_operation_take_stream")]
        [return: MarshalAs(UnmanagedType.I4)]
        internal static unsafe partial int AsyncOperationTakeStream(IntPtr operation, CArrowArrayStream* stream);

        [LibraryImport(LibraryName, EntryPoint = "dts_async_operation_take_schema")]
        [return: MarshalAs(UnmanagedType.I4)]
        internal static unsafe partial int AsyncOperationTakeSchema(IntPtr operation, CArrowSchema* schema);

        [LibraryImport(LibraryName, EntryPoint = "dts_async_operation_get_error")]
        internal static partial IntPtr AsyncOperationGetError(IntPtr operation);

        [LibraryImport(LibraryName, EntryPoint = "dts_async_operation_cancel")]
        internal static partial void AsyncOperationCancel(IntPtr operation);

        [LibraryImport(LibraryName, EntryPoint = "dts_async_operation_destroy")]
        internal static partial void AsyncOperationDestroy(IntPtr operation);

        [LibraryImport(LibraryName, EntryPoint = "dts_read_change_data", StringMarshalling = StringMarshalling.Utf8)]
        [return: MarshalAs(UnmanagedType.I4)]
        internal static unsafe partial int ReadChangeData(IntPtr engine, string commandJson, CArrowArrayStream* stream);

        [LibraryImport(LibraryName, EntryPoint = "dts_execute_query", StringMarshalling = StringMarshalling.Utf8)]
        [return: MarshalAs(UnmanagedType.I4)]
        internal static unsafe partial int ExecuteQuery(IntPtr engine, string commandJson, CArrowArrayStream* stream);

        [LibraryImport(LibraryName, EntryPoint = "dts_create_table", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial IntPtr CreateTable(IntPtr engine, string commandJson);

        [LibraryImport(LibraryName, EntryPoint = "dts_upgrade_protocol", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial IntPtr UpgradeProtocol(IntPtr engine, string commandJson);

        [LibraryImport(LibraryName, EntryPoint = "dts_execute_dml", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial IntPtr ExecuteDml(IntPtr engine, string commandJson);

        [LibraryImport(LibraryName, EntryPoint = "dts_free_string")]
        internal static partial void FreeString(IntPtr value);

        internal static string? PtrToStringUtf8(IntPtr ptr)
        {
            return ptr == IntPtr.Zero ? null : Marshal.PtrToStringUTF8(ptr);
        }

        internal static void EnsureLoaded()
        {
            if (_loadedHandle != IntPtr.Zero)
            {
                return;
            }

            foreach (string candidate in GetCandidateLibraryPaths())
            {
                if (File.Exists(candidate) && NativeLibrary.TryLoad(candidate, out IntPtr handle))
                {
                    _loadedHandle = handle;
                    return;
                }
            }

            throw new DllNotFoundException(
                $"Unable to locate native library '{LibraryName}'. Checked: {string.Join(", ", GetCandidateLibraryPaths())}");
        }

        private static IntPtr ResolveNativeLibrary(
            string libraryName,
            Assembly assembly,
            DllImportSearchPath? searchPath)
        {
            if (!string.Equals(libraryName, LibraryName, StringComparison.Ordinal))
            {
                return IntPtr.Zero;
            }

            if (_loadedHandle != IntPtr.Zero)
            {
                return _loadedHandle;
            }

            foreach (string candidate in GetCandidateLibraryPaths())
            {
                if (File.Exists(candidate) && NativeLibrary.TryLoad(candidate, out IntPtr handle))
                {
                    _loadedHandle = handle;
                    return handle;
                }
            }

            return IntPtr.Zero;
        }
    }
}
#endif
