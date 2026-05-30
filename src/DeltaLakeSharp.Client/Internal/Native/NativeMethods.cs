using System;
using System.IO;
using System.Runtime.InteropServices;

namespace DeltaLakeSharp.Client.Internal.Native
{
    internal static partial class NativeMethods
    {
        internal const string LibraryName = "delta_table_service_native";
        private static IntPtr _loadedHandle;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void NativeAsyncOperationCompletedCallback(IntPtr operation, IntPtr userData);

        private static string[] GetCandidateLibraryPaths()
        {
            string fileName = GetPlatformLibraryFileName();
            string baseDir = AppContext.BaseDirectory;
            string? dir = baseDir;

            string packageLocal = Path.Combine(baseDir, fileName);
            string runtimeLocal = Path.Combine(baseDir, "runtimes", "win-x64", "native", fileName);
            string runtimeCurrent = Path.Combine(baseDir, "native", fileName);

            while (dir != null)
            {
                string solutionFile = Path.Combine(dir, "DeltaLakeSharp.sln");
                if (File.Exists(solutionFile))
                {
                    return new[]
                    {
                        packageLocal,
                        runtimeLocal,
                        runtimeCurrent,
                        Path.Combine(dir, "src", "DeltaLakeSharp.Server", "v3", "target", "debug", fileName),
                        Path.Combine(dir, "src", "DeltaLakeSharp.Server", "v3", "target", "debug", "deps", fileName),
                    };
                }

                dir = Path.GetDirectoryName(dir);
            }

            return new[]
            {
                packageLocal,
                runtimeLocal,
                runtimeCurrent,
            };
        }

        private static string GetPlatformLibraryFileName()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return $"{LibraryName}.dll";
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                return $"lib{LibraryName}.dylib";
            }

            return $"lib{LibraryName}.so";
        }
    }
}
