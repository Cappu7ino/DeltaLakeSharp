// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Runtime.InteropServices;

namespace DeltaLakeSharp.Client.Internal.Native
{
    internal sealed class NativeEngineHandle : SafeHandle
    {
        public NativeEngineHandle()
            : base(IntPtr.Zero, ownsHandle: true)
        {
        }

        public override bool IsInvalid => handle == IntPtr.Zero;

        public static NativeEngineHandle Create()
        {
            NativeMethods.EnsureLoaded();
            IntPtr ptr = NativeMethods.CreateEngine();
            if (ptr == IntPtr.Zero)
            {
                throw new InvalidOperationException("Failed to create native Delta service engine.");
            }

            var handle = new NativeEngineHandle();
            handle.SetHandle(ptr);
            return handle;
        }

        protected override bool ReleaseHandle()
        {
            NativeMethods.DestroyEngine(handle);
            return true;
        }
    }
}
