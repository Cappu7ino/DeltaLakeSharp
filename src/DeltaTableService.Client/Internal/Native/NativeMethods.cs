// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Microsoft.ADMS.Testing.DeltaTableService.Client.Internal.Native
{
    internal static partial class NativeMethods
    {
        internal const string LibraryName = "delta_table_service_v3";
        private static IntPtr _loadedHandle;

        private static string[] GetCandidateLibraryPaths()
        {
            string fileName = GetPlatformLibraryFileName();
            string dir = AppContext.BaseDirectory;

            while (dir != null)
            {
                string solutionFile = Path.Combine(dir, "DeltaTableService.sln");
                if (File.Exists(solutionFile))
                {
                    return new[]
                    {
                        Path.Combine(dir, "src", "DeltaTableService.Server", "v3", "target", "debug", fileName),
                        Path.Combine(dir, "src", "DeltaTableService.Server", "v3", "target", "debug", "deps", fileName),
                    };
                }

                dir = Path.GetDirectoryName(dir);
            }

            return Array.Empty<string>();
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
