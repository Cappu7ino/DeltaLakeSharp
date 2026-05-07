// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#if NET472 || NETSTANDARD2_0
using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow.C;

namespace Microsoft.DI.DeltaTableService.Client.Internal.Native
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

        [DllImport(LibraryName, EntryPoint = "dts_get_schema", CallingConvention = CallingConvention.Cdecl)]
        private static extern unsafe int GetSchemaNative(IntPtr engine, IntPtr commandJson, CArrowSchema* schema);

        [DllImport(LibraryName, EntryPoint = "dts_read_table", CallingConvention = CallingConvention.Cdecl)]
        private static extern unsafe int ReadTableNative(IntPtr engine, IntPtr commandJson, CArrowArrayStream* stream);

        [DllImport(LibraryName, EntryPoint = "dts_plan_read_partitions", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr PlanReadPartitionsNative(IntPtr engine, IntPtr commandJson);

        [DllImport(LibraryName, EntryPoint = "dts_read_table_partition", CallingConvention = CallingConvention.Cdecl)]
        private static extern unsafe int ReadTablePartitionNative(IntPtr engine, IntPtr commandJson, CArrowArrayStream* stream);

        [DllImport(LibraryName, EntryPoint = "dts_read_change_data", CallingConvention = CallingConvention.Cdecl)]
        private static extern unsafe int ReadChangeDataNative(IntPtr engine, IntPtr commandJson, CArrowArrayStream* stream);

        [DllImport(LibraryName, EntryPoint = "dts_execute_query", CallingConvention = CallingConvention.Cdecl)]
        private static extern unsafe int ExecuteQueryNative(IntPtr engine, IntPtr commandJson, CArrowArrayStream* stream);

        [DllImport(LibraryName, EntryPoint = "dts_insert", CallingConvention = CallingConvention.Cdecl)]
        private static extern unsafe int InsertNative(IntPtr engine, IntPtr commandJson, CArrowArrayStream* stream);

        [DllImport(LibraryName, EntryPoint = "dts_merge_stream", CallingConvention = CallingConvention.Cdecl)]
        private static extern unsafe IntPtr MergeStreamNative(IntPtr engine, IntPtr commandJson, CArrowArrayStream* stream);

        [DllImport(LibraryName, EntryPoint = "dts_create_table", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr CreateTableNative(IntPtr engine, IntPtr commandJson);

        [DllImport(LibraryName, EntryPoint = "dts_upgrade_protocol", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr UpgradeProtocolNative(IntPtr engine, IntPtr commandJson);

        [DllImport(LibraryName, EntryPoint = "dts_execute_dml", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr ExecuteDmlNative(IntPtr engine, IntPtr commandJson);

        [DllImport(LibraryName, EntryPoint = "dts_free_string", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void FreeString(IntPtr value);

        internal static unsafe int GetSchema(IntPtr engine, string commandJson, CArrowSchema* schema)
        {
            return WithUtf8String(commandJson, ptr => GetSchemaNative(engine, ptr, schema));
        }

        internal static unsafe int ReadTable(IntPtr engine, string commandJson, CArrowArrayStream* stream)
        {
            return WithUtf8String(commandJson, ptr => ReadTableNative(engine, ptr, stream));
        }

        internal static IntPtr PlanReadPartitions(IntPtr engine, string commandJson)
        {
            return WithUtf8String(commandJson, ptr => PlanReadPartitionsNative(engine, ptr));
        }

        internal static unsafe int ReadTablePartition(IntPtr engine, string commandJson, CArrowArrayStream* stream)
        {
            return WithUtf8String(commandJson, ptr => ReadTablePartitionNative(engine, ptr, stream));
        }

        internal static unsafe int ReadChangeData(IntPtr engine, string commandJson, CArrowArrayStream* stream)
        {
            return WithUtf8String(commandJson, ptr => ReadChangeDataNative(engine, ptr, stream));
        }

        internal static unsafe int ExecuteQuery(IntPtr engine, string commandJson, CArrowArrayStream* stream)
        {
            return WithUtf8String(commandJson, ptr => ExecuteQueryNative(engine, ptr, stream));
        }

        internal static unsafe int Insert(IntPtr engine, string commandJson, CArrowArrayStream* stream)
        {
            return WithUtf8String(commandJson, ptr => InsertNative(engine, ptr, stream));
        }

        internal static unsafe IntPtr MergeStream(IntPtr engine, string commandJson, CArrowArrayStream* stream)
        {
            return WithUtf8String(commandJson, ptr => MergeStreamNative(engine, ptr, stream));
        }

        internal static IntPtr CreateTable(IntPtr engine, string commandJson)
        {
            return WithUtf8String(commandJson, ptr => CreateTableNative(engine, ptr));
        }

        internal static IntPtr UpgradeProtocol(IntPtr engine, string commandJson)
        {
            return WithUtf8String(commandJson, ptr => UpgradeProtocolNative(engine, ptr));
        }

        internal static IntPtr ExecuteDml(IntPtr engine, string commandJson)
        {
            return WithUtf8String(commandJson, ptr => ExecuteDmlNative(engine, ptr));
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
