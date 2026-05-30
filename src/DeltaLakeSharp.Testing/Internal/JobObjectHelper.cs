using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
#if !NET472
using System.Runtime.Versioning;
#endif

namespace DeltaLakeSharp.Testing.Internal
{
    /// <summary>
    /// Assigns child processes to a Windows Job Object configured with
    /// <c>JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE</c>.
    /// </summary>
#if !NET472
    [SupportedOSPlatform("windows")]
#endif
    internal sealed class JobObjectHelper : IDisposable
    {
        private IntPtr _jobHandle;
        private bool _disposed;

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
            internal static extern IntPtr CreateJobObject(IntPtr lpJobAttributes, string lpName);

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
