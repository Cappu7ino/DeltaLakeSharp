// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Runtime.InteropServices;

namespace DeltaLakeSharp.Client.Internal.Native
{
    internal sealed class NativeAsyncOperationHandle : SafeHandle
    {
        public NativeAsyncOperationHandle()
            : base(IntPtr.Zero, ownsHandle: true)
        {
        }

        public override bool IsInvalid => handle == IntPtr.Zero;

        public static NativeAsyncOperationHandle FromIntPtr(IntPtr ptr)
        {
            if (ptr == IntPtr.Zero)
            {
                throw new InvalidOperationException("Failed to create native async operation.");
            }

            var handle = new NativeAsyncOperationHandle();
            handle.SetHandle(ptr);
            return handle;
        }

        protected override bool ReleaseHandle()
        {
            NativeMethods.AsyncOperationDestroy(handle);
            return true;
        }
    }
}
