// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#if !NET472
using System;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using Apache.Arrow.C;

namespace Microsoft.ADMS.Testing.DeltaTableService.Client.Internal.Native
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

        [LibraryImport(LibraryName, EntryPoint = "dts_get_schema", StringMarshalling = StringMarshalling.Utf8)]
        [return: MarshalAs(UnmanagedType.I4)]
        internal static unsafe partial int GetSchema(IntPtr engine, string commandJson, CArrowSchema* schema);

        [LibraryImport(LibraryName, EntryPoint = "dts_read_table", StringMarshalling = StringMarshalling.Utf8)]
        [return: MarshalAs(UnmanagedType.I4)]
        internal static unsafe partial int ReadTable(IntPtr engine, string commandJson, CArrowArrayStream* stream);

        [LibraryImport(LibraryName, EntryPoint = "dts_read_change_data", StringMarshalling = StringMarshalling.Utf8)]
        [return: MarshalAs(UnmanagedType.I4)]
        internal static unsafe partial int ReadChangeData(IntPtr engine, string commandJson, CArrowArrayStream* stream);

        [LibraryImport(LibraryName, EntryPoint = "dts_execute_query", StringMarshalling = StringMarshalling.Utf8)]
        [return: MarshalAs(UnmanagedType.I4)]
        internal static unsafe partial int ExecuteQuery(IntPtr engine, string commandJson, CArrowArrayStream* stream);

        [LibraryImport(LibraryName, EntryPoint = "dts_insert", StringMarshalling = StringMarshalling.Utf8)]
        [return: MarshalAs(UnmanagedType.I4)]
        internal static unsafe partial int Insert(IntPtr engine, string commandJson, CArrowArrayStream* stream);

        [LibraryImport(LibraryName, EntryPoint = "dts_merge_stream", StringMarshalling = StringMarshalling.Utf8)]
        internal static unsafe partial IntPtr MergeStream(IntPtr engine, string commandJson, CArrowArrayStream* stream);

        [LibraryImport(LibraryName, EntryPoint = "dts_create_table", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial IntPtr CreateTable(IntPtr engine, string commandJson);

        [LibraryImport(LibraryName, EntryPoint = "dts_upgrade_protocol", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial IntPtr UpgradeProtocol(IntPtr engine, string commandJson);

        [LibraryImport(LibraryName, EntryPoint = "dts_execute_dml", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial IntPtr ExecuteDml(IntPtr engine, string commandJson);

        [LibraryImport(LibraryName, EntryPoint = "dts_free_string")]
        internal static partial void FreeString(IntPtr value);

        internal static string PtrToStringUtf8(IntPtr ptr)
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
