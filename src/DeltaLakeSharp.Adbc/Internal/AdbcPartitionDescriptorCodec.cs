// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Text;
using System.Text.Json;
using Apache.Arrow.Adbc;

namespace DeltaLakeSharp.Adbc.Internal
{
    internal static class AdbcPartitionDescriptorCodec
    {
        public static byte[] Encode(string token, int? batchSize)
        {
            if (string.IsNullOrWhiteSpace(token))
            {
                throw new ArgumentException("Partition token must be provided.", nameof(token));
            }

            var payload = new PartitionDescriptorPayload
            {
                Token = token,
                BatchSize = batchSize,
            };

            return Encoding.UTF8.GetBytes(JsonSerializer.Serialize(payload));
        }

        public static DecodedPartitionDescriptor Decode(PartitionDescriptor descriptor)
        {
            string raw = Encoding.UTF8.GetString(descriptor.Descriptor.ToArray());
            if (string.IsNullOrWhiteSpace(raw))
            {
                throw new AdbcException("Partition descriptor payload is empty.", AdbcStatusCode.InvalidArgument);
            }

            try
            {
                PartitionDescriptorPayload? payload = JsonSerializer.Deserialize<PartitionDescriptorPayload>(raw);
                if (payload?.Token is string token && !string.IsNullOrWhiteSpace(token))
                {
                    return new DecodedPartitionDescriptor(token, payload.BatchSize);
                }
            }
            catch (JsonException)
            {
            }

            return new DecodedPartitionDescriptor(raw, batchSize: null);
        }

        internal readonly struct DecodedPartitionDescriptor
        {
            public DecodedPartitionDescriptor(string token, int? batchSize)
            {
                Token = token;
                BatchSize = batchSize;
            }

            public string Token { get; }

            public int? BatchSize { get; }
        }

        private sealed class PartitionDescriptorPayload
        {
            public string? Token { get; set; }

            public int? BatchSize { get; set; }
        }
    }
}
