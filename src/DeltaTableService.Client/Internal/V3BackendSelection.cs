// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Threading;

namespace Microsoft.ADMS.Testing.DeltaTableService.Client.Internal
{
    /// <summary>
    /// Centralizes the temporary migration switch that decides whether
    /// <see cref="ServiceMode.V3_Rust"/> uses the legacy Flight transport or the
    /// in-process native Rust backend.
    /// </summary>
    internal static class V3BackendSelection
    {
        internal const string AppContextSwitchName = "DeltaTableService.Client.UseNativeV3Backend";
        internal const string EnvironmentVariableName = "DELTA_TABLE_SERVICE_V3_USE_NATIVE";

        private static readonly AsyncLocal<bool?> AsyncOverride = new();

        internal static bool UseNativeBackend =>
            AsyncOverride.Value ?? GetProcessDefault();

        internal static IDisposable PushOverride(bool useNativeBackend)
        {
            bool? previousValue = AsyncOverride.Value;
            AsyncOverride.Value = useNativeBackend;
            return new RestoreOverride(previousValue);
        }

        private static bool GetProcessDefault()
        {
            if (AppContext.TryGetSwitch(AppContextSwitchName, out bool switchEnabled))
            {
                return switchEnabled;
            }

            string? value = Environment.GetEnvironmentVariable(EnvironmentVariableName);
            if (string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            return string.Equals(value, "1", StringComparison.OrdinalIgnoreCase)
                || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase)
                || string.Equals(value, "yes", StringComparison.OrdinalIgnoreCase);
        }

        private sealed class RestoreOverride : IDisposable
        {
            private readonly bool? _previousValue;
            private bool _disposed;

            public RestoreOverride(bool? previousValue)
            {
                _previousValue = previousValue;
            }

            public void Dispose()
            {
                if (_disposed)
                {
                    return;
                }

                AsyncOverride.Value = _previousValue;
                _disposed = true;
            }
        }
    }
}
