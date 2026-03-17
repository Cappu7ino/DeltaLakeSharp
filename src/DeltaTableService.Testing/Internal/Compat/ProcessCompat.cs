// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.DI.DeltaTableService.Testing.Internal.Compat
{
    internal static class ProcessCompat
    {
        internal static Task WaitForExitAsync(Process process, CancellationToken cancellationToken)
        {
#if NET472
            if (process.HasExited)
            {
                return Task.CompletedTask;
            }

            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            EventHandler handler = null;
            handler = (sender, args) =>
            {
                process.Exited -= handler;
                tcs.TrySetResult(null);
            };

            process.EnableRaisingEvents = true;
            process.Exited += handler;

            if (process.HasExited)
            {
                process.Exited -= handler;
                return Task.CompletedTask;
            }

            if (cancellationToken.CanBeCanceled)
            {
                cancellationToken.Register(() =>
                {
                    process.Exited -= handler;
                    tcs.TrySetCanceled(cancellationToken);
                });
            }

            return tcs.Task;
#else
            return process.WaitForExitAsync(cancellationToken);
#endif
        }
    }
}
