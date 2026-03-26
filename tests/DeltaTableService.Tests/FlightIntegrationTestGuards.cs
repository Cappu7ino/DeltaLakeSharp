// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.DI.DeltaTableService.Tests
{
    internal static class FlightIntegrationTestGuards
    {
        public static void EnsureArrowFlightSupported()
        {
#if NETFRAMEWORK
            Assert.Inconclusive(
                "Arrow Flight integration tests are not supported on net472 because Grpc.Net.Client cannot connect to the local insecure HTTP/2 Flight endpoint on .NET Framework.");
#endif
        }
    }
}
