// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;

namespace DeltaLakeSharp.Client.Internal.Native
{
    internal readonly struct NativeErrorInfo
    {
        public NativeErrorInfo(int rawCode, string? message)
        {
            RawCode = rawCode;
            Code = Enum.IsDefined(typeof(NativeServiceErrorCode), rawCode)
                ? (NativeServiceErrorCode)rawCode
                : NativeServiceErrorCode.Internal;
            Message = message;
        }

        public int RawCode { get; }

        public NativeServiceErrorCode Code { get; }

        public string? Message { get; }

        public bool HasError => RawCode != (int)NativeServiceErrorCode.Ok || !string.IsNullOrWhiteSpace(Message);

        public bool HasKnownCode => Enum.IsDefined(typeof(NativeServiceErrorCode), RawCode);
    }
}
