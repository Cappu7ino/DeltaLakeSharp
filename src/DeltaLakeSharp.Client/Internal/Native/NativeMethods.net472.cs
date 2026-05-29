// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#if NET472 || NETSTANDARD2_0
using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow.C;

namespace DeltaLakeSharp.Client.Internal.Native
{
    internal static partial class NativeMethods
    {
        static NativeMethods()
        {
        }

        [DllImport(LibraryName, EntryPoint = "dts_create_engine", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr CreateEngine();

        [DllImport(LibraryName, EntryPoint = "dts_destroy_engine", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void DestroyEngine(IntPtr engine);

        [DllImport(LibraryName, EntryPoint = "dts_health_check", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int HealthCheck(IntPtr engine);

        [DllImport(LibraryName, EntryPoint = "dts_get_last_error", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr GetLastError(IntPtr engine);

        [DllImport(LibraryName, EntryPoint = "dts_get_schema_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr GetSchemaAsyncWithCallbackNative(
            IntPtr engine,
            IntPtr commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_read_table_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr ReadTableAsyncWithCallbackNative(
            IntPtr engine,
            IntPtr commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_execute_query_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr ExecuteQueryAsyncWithCallbackNative(
            IntPtr engine,
            IntPtr commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_read_change_data_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr ReadChangeDataAsyncWithCallbackNative(
            IntPtr engine,
            IntPtr commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_read_table_partition_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr ReadTablePartitionAsyncWithCallbackNative(
            IntPtr engine,
            IntPtr commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_plan_read_partitions_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr PlanReadPartitionsAsyncWithCallbackNative(
            IntPtr engine,
            IntPtr commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_create_table_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr CreateTableAsyncWithCallbackNative(
            IntPtr engine,
            IntPtr commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_upgrade_protocol_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr UpgradeProtocolAsyncWithCallbackNative(
            IntPtr engine,
            IntPtr commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_execute_dml_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr ExecuteDmlAsyncWithCallbackNative(
            IntPtr engine,
            IntPtr commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_insert_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr InsertAsyncWithCallbackNative(
            IntPtr engine,
            IntPtr commandJson,
            IntPtr sourceStream,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_merge_stream_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr MergeStreamAsyncWithCallbackNative(
            IntPtr engine,
            IntPtr commandJson,
            IntPtr sourceStream,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData);

        [DllImport(LibraryName, EntryPoint = "dts_async_operation_status", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int AsyncOperationStatus(IntPtr operation);

        [DllImport(LibraryName, EntryPoint = "dts_async_operation_take_result", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr AsyncOperationTakeResult(IntPtr operation);

        [DllImport(LibraryName, EntryPoint = "dts_async_operation_take_stream", CallingConvention = CallingConvention.Cdecl)]
        internal static extern unsafe int AsyncOperationTakeStream(IntPtr operation, CArrowArrayStream* stream);

        [DllImport(LibraryName, EntryPoint = "dts_async_operation_take_schema", CallingConvention = CallingConvention.Cdecl)]
        internal static extern unsafe int AsyncOperationTakeSchema(IntPtr operation, CArrowSchema* schema);

        [DllImport(LibraryName, EntryPoint = "dts_async_operation_get_error", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr AsyncOperationGetError(IntPtr operation);

        [DllImport(LibraryName, EntryPoint = "dts_async_operation_cancel", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void AsyncOperationCancel(IntPtr operation);

        [DllImport(LibraryName, EntryPoint = "dts_async_operation_destroy", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void AsyncOperationDestroy(IntPtr operation);

        [DllImport(LibraryName, EntryPoint = "dts_free_string", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void FreeString(IntPtr value);

        internal static IntPtr ReadTableAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return WithUtf8String(commandJson, ptr => ReadTableAsyncWithCallbackNative(engine, ptr, callback, userData));
        }

        internal static IntPtr ExecuteQueryAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return WithUtf8String(commandJson, ptr => ExecuteQueryAsyncWithCallbackNative(engine, ptr, callback, userData));
        }

        internal static IntPtr PlanReadPartitionsAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return WithUtf8String(commandJson, ptr => PlanReadPartitionsAsyncWithCallbackNative(engine, ptr, callback, userData));
        }

        internal static IntPtr GetSchemaAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return WithUtf8String(commandJson, ptr => GetSchemaAsyncWithCallbackNative(engine, ptr, callback, userData));
        }

        internal static IntPtr CreateTableAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return WithUtf8String(commandJson, ptr => CreateTableAsyncWithCallbackNative(engine, ptr, callback, userData));
        }

        internal static IntPtr UpgradeProtocolAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return WithUtf8String(commandJson, ptr => UpgradeProtocolAsyncWithCallbackNative(engine, ptr, callback, userData));
        }

        internal static IntPtr ExecuteDmlAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return WithUtf8String(commandJson, ptr => ExecuteDmlAsyncWithCallbackNative(engine, ptr, callback, userData));
        }

        internal static IntPtr ReadChangeDataAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return WithUtf8String(commandJson, ptr => ReadChangeDataAsyncWithCallbackNative(engine, ptr, callback, userData));
        }

        internal static IntPtr ReadTablePartitionAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return WithUtf8String(commandJson, ptr => ReadTablePartitionAsyncWithCallbackNative(engine, ptr, callback, userData));
        }

        internal static IntPtr InsertAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            IntPtr sourceStream,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return WithUtf8String(commandJson, ptr => InsertAsyncWithCallbackNative(engine, ptr, sourceStream, callback, userData));
        }

        internal static IntPtr MergeStreamAsyncWithCallback(
            IntPtr engine,
            string commandJson,
            IntPtr sourceStream,
            NativeAsyncOperationCompletedCallback callback,
            IntPtr userData)
        {
            return WithUtf8String(commandJson, ptr => MergeStreamAsyncWithCallbackNative(engine, ptr, sourceStream, callback, userData));
        }

        internal static string? PtrToStringUtf8(IntPtr ptr)
        {
            if (ptr == IntPtr.Zero)
            {
                return null;
            }

            int length = 0;
            while (Marshal.ReadByte(ptr, length) != 0)
            {
                length++;
            }

            if (length == 0)
            {
                return string.Empty;
            }

            var bytes = new byte[length];
            Marshal.Copy(ptr, bytes, 0, length);
            return Encoding.UTF8.GetString(bytes, 0, length);
        }

        internal static void EnsureLoaded()
        {
            if (_loadedHandle != IntPtr.Zero)
            {
                return;
            }

            foreach (string candidate in GetCandidateLibraryPaths())
            {
                if (!File.Exists(candidate))
                {
                    continue;
                }

                IntPtr handle = LoadLibrary(candidate);
                if (handle != IntPtr.Zero)
                {
                    _loadedHandle = handle;
                    return;
                }
            }

            throw new DllNotFoundException(
                $"Unable to locate native library '{LibraryName}'. Checked: {string.Join(", ", GetCandidateLibraryPaths())}");
        }

        private static T WithUtf8String<T>(string value, Func<IntPtr, T> action)
        {
            byte[] utf8 = Encoding.UTF8.GetBytes(value + "\0");
            IntPtr unmanaged = Marshal.AllocHGlobal(utf8.Length);
            try
            {
                Marshal.Copy(utf8, 0, unmanaged, utf8.Length);
                return action(unmanaged);
            }
            finally
            {
                Marshal.FreeHGlobal(unmanaged);
            }
        }

        [DllImport("kernel32", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern IntPtr LoadLibrary(string lpFileName);
    }
}
#endif
