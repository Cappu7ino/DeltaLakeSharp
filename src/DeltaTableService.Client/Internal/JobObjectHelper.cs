// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
#if !NET472
using System.Runtime.Versioning;
#endif

namespace Microsoft.ADMS.Testing.DeltaTableService.Client.Internal
{
    /// <summary>
    /// Assigns child processes to a Windows Job Object configured with
    /// <c>JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE</c>. This ensures that if the
    /// parent process (the C# test host) crashes or exits without graceful
    /// shutdown, all child processes (the Rust binary) are terminated by the OS.
    /// </summary>
    /// <remarks>
    /// On non-Windows platforms this class is a no-op. The
    /// <see cref="SupportedOSPlatform"/> attribute documents the Windows-only
    /// nature, but the code gracefully falls through on other platforms by
    /// throwing <see cref="PlatformNotSupportedException"/> from the
    /// constructor.
    /// </remarks>
#if !NET472
    [SupportedOSPlatform("windows")]
#endif
    internal sealed class JobObjectHelper : IDisposable
    {
        private IntPtr _jobHandle;
        private bool _disposed;

        /// <summary>
        /// Creates a new anonymous Job Object with the kill-on-close flag.
        /// </summary>
        /// <exception cref="PlatformNotSupportedException">
        /// Thrown on non-Windows platforms.
        /// </exception>
        /// <exception cref="Win32Exception">
        /// Thrown when the Job Object could not be created or configured.
        /// </exception>
        public JobObjectHelper()
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                throw new PlatformNotSupportedException(
                    "Job Objects are only supported on Windows.");
            }

            _jobHandle = NativeMethods.CreateJobObject(IntPtr.Zero, null);
            if (_jobHandle == IntPtr.Zero)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error(),
                    "Failed to create Job Object.");
            }

            // Configure KILL_ON_JOB_CLOSE so child processes die when the
            // Job Object handle is closed (i.e. when this process exits).
            var info = new NativeMethods.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
            {
                BasicLimitInformation = new NativeMethods.JOBOBJECT_BASIC_LIMIT_INFORMATION
                {
                    LimitFlags = NativeMethods.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
                },
            };

            int infoSize = Marshal.SizeOf(typeof(NativeMethods.JOBOBJECT_EXTENDED_LIMIT_INFORMATION));
            if (!NativeMethods.SetInformationJobObject(
                    _jobHandle,
                    NativeMethods.JobObjectInfoType.ExtendedLimitInformation,
                    ref info,
                    infoSize))
            {
                int error = Marshal.GetLastWin32Error();
                NativeMethods.CloseHandle(_jobHandle);
                _jobHandle = IntPtr.Zero;
                throw new Win32Exception(error,
                    "Failed to set Job Object information (KILL_ON_JOB_CLOSE).");
            }
        }

        /// <summary>
        /// Assigns a running process to this Job Object.
        /// </summary>
        /// <param name="process">The process to assign.</param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="process"/> is <c>null</c>.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the Job Object handle is invalid.
        /// </exception>
        /// <exception cref="Win32Exception">
        /// Thrown when the process could not be assigned to the Job Object.
        /// </exception>
        public void AssignProcess(Process process)
        {
            if (process == null)
            {
                throw new ArgumentNullException(nameof(process));
            }

            if (_jobHandle == IntPtr.Zero)
            {
                throw new InvalidOperationException("Job Object handle is invalid.");
            }

            if (!NativeMethods.AssignProcessToJobObject(_jobHandle, process.Handle))
            {
                throw new Win32Exception(Marshal.GetLastWin32Error(),
                    "Failed to assign process to Job Object.");
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            if (_jobHandle != IntPtr.Zero)
            {
                NativeMethods.CloseHandle(_jobHandle);
                _jobHandle = IntPtr.Zero;
            }
        }

        /// <summary>
        /// P/Invoke declarations for Windows Job Object APIs.
        /// </summary>
        private static class NativeMethods
        {
            internal const uint JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000;

            internal enum JobObjectInfoType
            {
                ExtendedLimitInformation = 9,
            }

            [StructLayout(LayoutKind.Sequential)]
            internal struct JOBOBJECT_BASIC_LIMIT_INFORMATION
            {
                public long PerProcessUserTimeLimit;
                public long PerJobUserTimeLimit;
                public uint LimitFlags;
                public UIntPtr MinimumWorkingSetSize;
                public UIntPtr MaximumWorkingSetSize;
                public uint ActiveProcessLimit;
                public UIntPtr Affinity;
                public uint PriorityClass;
                public uint SchedulingClass;
            }

            [StructLayout(LayoutKind.Sequential)]
            internal struct IO_COUNTERS
            {
                public ulong ReadOperationCount;
                public ulong WriteOperationCount;
                public ulong OtherOperationCount;
                public ulong ReadTransferCount;
                public ulong WriteTransferCount;
                public ulong OtherTransferCount;
            }

            [StructLayout(LayoutKind.Sequential)]
            internal struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION
            {
                public JOBOBJECT_BASIC_LIMIT_INFORMATION BasicLimitInformation;
                public IO_COUNTERS IoInfo;
                public UIntPtr ProcessMemoryLimit;
                public UIntPtr JobMemoryLimit;
                public UIntPtr PeakProcessMemoryUsed;
                public UIntPtr PeakJobMemoryUsed;
            }

            [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
            internal static extern IntPtr CreateJobObject(IntPtr lpJobAttributes, string? lpName);

            [DllImport("kernel32.dll", SetLastError = true)]
            [return: MarshalAs(UnmanagedType.Bool)]
            internal static extern bool SetInformationJobObject(
                IntPtr hJob,
                JobObjectInfoType infoType,
                ref JOBOBJECT_EXTENDED_LIMIT_INFORMATION lpJobObjectInfo,
                int cbJobObjectInfoLength);

            [DllImport("kernel32.dll", SetLastError = true)]
            [return: MarshalAs(UnmanagedType.Bool)]
            internal static extern bool AssignProcessToJobObject(IntPtr hJob, IntPtr hProcess);

            [DllImport("kernel32.dll", SetLastError = true)]
            [return: MarshalAs(UnmanagedType.Bool)]
            internal static extern bool CloseHandle(IntPtr hObject);
        }
    }
}
