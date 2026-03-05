// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.ADMS.Testing.DeltaTableService.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.ADMS.Testing.DeltaTableService.Tests
{
    /// <summary>
    /// Integration tests for the Delta Table Service V3 (native Rust binary).
    /// Exercises the Flight server skeleton via <see cref="DeltaTableServiceClient"/>
    /// using <see cref="DeltaTableProcess"/> to manage the server lifecycle.
    ///
    /// Phase 1 tests cover health, list-actions, and basic client interactions.
    /// Read/write/DML tests will be added in Phases 2-3.
    ///
    /// Run with: dotnet test --filter "TestCategory=Integration&amp;TestCategory=V3Flight"
    /// </summary>
    [TestClass]
    [TestCategory("Integration")]
    [TestCategory("V3")]
    [TestCategory("V3Flight")]
    public class DeltaTableServiceV3IntegrationTests
    {
        private static DeltaTableProcess? _process;
        private static DeltaTableServiceClient? _client;

        /// <summary>
        /// Resolves the path to the Rust binary by walking up from the test output
        /// directory to the repo root.
        /// </summary>
        private static string? FindRustBinary()
        {
            string? dir = AppContext.BaseDirectory;
            while (dir != null)
            {
                string solutionFile = Path.Combine(dir, "DeltaTableService.sln");
                if (File.Exists(solutionFile))
                {
                    string binaryPath = Path.Combine(
                        dir, "src", "DeltaTableService.Server", "v3",
                        "target", "debug", "delta-table-service-v3.exe");
                    return File.Exists(binaryPath) ? binaryPath : null;
                }
                dir = Path.GetDirectoryName(dir);
            }

            return null;
        }

        /// <summary>
        /// Starts the V3 Rust server once for all integration tests.
        /// The Rust binary must be pre-built via <c>cargo build</c>.
        /// </summary>
        [ClassInitialize]
        public static async Task ClassInitialize(TestContext context)
        {
            string? binaryPath = FindRustBinary();
            if (binaryPath == null)
            {
                Assert.Inconclusive(
                    "Rust binary not found. Build it first: " +
                    "cd src/DeltaTableService.Server/v3 && cargo build");
                return;
            }

            _process = new DeltaTableProcess();
            await _process.StartAsync(binaryPath);

            _client = new DeltaTableServiceClient(
                _process.GetFlightUri(), ServiceMode.V3_Rust);

            // Verify health after startup.
            bool healthy = await _client.HealthCheckAsync();
            Assert.IsTrue(healthy, "Delta Table Service V3 did not become healthy.");
        }

        /// <summary>
        /// Stops and disposes the Rust server after all integration tests.
        /// </summary>
        [ClassCleanup]
        public static async Task ClassCleanup()
        {
            _client?.Dispose();
            if (_process != null)
            {
                await _process.DisposeAsync();
            }
        }

        // ================================================================== //
        //  Health check
        // ================================================================== //

        [TestMethod]
        public async Task V3_HealthCheck_ReturnsTrue()
        {
            bool healthy = await _client!.HealthCheckAsync();
            Assert.IsTrue(healthy, "Expected the V3 server to report healthy.");
        }

        // ================================================================== //
        //  Service mode
        // ================================================================== //

        [TestMethod]
        public void V3_Client_ReportsCorrectServiceMode()
        {
            Assert.AreEqual(ServiceMode.V3_Rust, _client!.Mode,
                "Expected client to report V3_Rust service mode.");
        }

        // ================================================================== //
        //  ListActions
        // ================================================================== //

        [TestMethod]
        public async Task V3_ListActions_ReturnsExpectedActions()
        {
            // Use a raw Flight client to call ListActions, since
            // DeltaTableServiceClient doesn't expose it directly.
            var channel = Grpc.Net.Client.GrpcChannel.ForAddress(
                _process!.GetFlightUri());
            try
            {
                var flightClient = new Apache.Arrow.Flight.Client.FlightClient(channel);
                var call = flightClient.ListActions();

                var actions = new System.Collections.Generic.List<string>();
                while (await call.ResponseStream.MoveNext(default))
                {
                    actions.Add(call.ResponseStream.Current.Type);
                }

                // Phase 1 actions: health, shutdown, create_table, execute_dml, upgrade_protocol
                Assert.IsTrue(actions.Contains("health"),
                    $"Missing 'health' action. Got: [{string.Join(", ", actions)}]");
                Assert.IsTrue(actions.Contains("shutdown"),
                    $"Missing 'shutdown' action. Got: [{string.Join(", ", actions)}]");
                Assert.IsTrue(actions.Contains("create_table"),
                    $"Missing 'create_table' action. Got: [{string.Join(", ", actions)}]");
                Assert.IsTrue(actions.Contains("execute_dml"),
                    $"Missing 'execute_dml' action. Got: [{string.Join(", ", actions)}]");
                Assert.IsTrue(actions.Contains("upgrade_protocol"),
                    $"Missing 'upgrade_protocol' action. Got: [{string.Join(", ", actions)}]");
                Assert.AreEqual(5, actions.Count,
                    $"Expected exactly 5 actions, got {actions.Count}: [{string.Join(", ", actions)}]");
            }
            finally
            {
                channel.Dispose();
            }
        }

        // ================================================================== //
        //  Unimplemented stubs return proper errors (Phase 2/3)
        // ================================================================== //

        [TestMethod]
        public async Task V3_GetFlightInfo_ReturnsUnimplemented()
        {
            var channel = Grpc.Net.Client.GrpcChannel.ForAddress(
                _process!.GetFlightUri());
            try
            {
                var flightClient = new Apache.Arrow.Flight.Client.FlightClient(channel);

                var descriptor = Apache.Arrow.Flight.FlightDescriptor.CreateCommandDescriptor(
                    System.Text.Encoding.UTF8.GetBytes("{\"path\":\"/tmp/test\"}"));

                var ex = await Assert.ThrowsExceptionAsync<Grpc.Core.RpcException>(async () =>
                {
                    await flightClient.GetInfo(descriptor);
                });

                Assert.AreEqual(Grpc.Core.StatusCode.Unimplemented, ex.StatusCode,
                    $"Expected Unimplemented, got {ex.StatusCode}: {ex.Status.Detail}");
            }
            finally
            {
                channel.Dispose();
            }
        }

        [TestMethod]
        public async Task V3_DoGet_ReturnsUnimplemented()
        {
            var channel = Grpc.Net.Client.GrpcChannel.ForAddress(
                _process!.GetFlightUri());
            try
            {
                var flightClient = new Apache.Arrow.Flight.Client.FlightClient(channel);

                var ticket = new Apache.Arrow.Flight.FlightTicket(
                    System.Text.Encoding.UTF8.GetBytes("{\"path\":\"/tmp/test\"}"));

                var ex = await Assert.ThrowsExceptionAsync<Grpc.Core.RpcException>(async () =>
                {
                    var stream = flightClient.GetStream(ticket);
                    // Need to attempt reading to trigger the RPC.
                    await stream.ResponseStream.MoveNext(default);
                });

                Assert.AreEqual(Grpc.Core.StatusCode.Unimplemented, ex.StatusCode,
                    $"Expected Unimplemented, got {ex.StatusCode}: {ex.Status.Detail}");
            }
            finally
            {
                channel.Dispose();
            }
        }

        [TestMethod]
        public async Task V3_DoAction_UnknownType_ReturnsInvalidArgument()
        {
            var channel = Grpc.Net.Client.GrpcChannel.ForAddress(
                _process!.GetFlightUri());
            try
            {
                var flightClient = new Apache.Arrow.Flight.Client.FlightClient(channel);
                var action = new Apache.Arrow.Flight.FlightAction(
                    "nonexistent_action",
                    Google.Protobuf.ByteString.Empty);

                var call = flightClient.DoAction(action);

                var ex = await Assert.ThrowsExceptionAsync<Grpc.Core.RpcException>(async () =>
                {
                    while (await call.ResponseStream.MoveNext(default)) { }
                });

                Assert.AreEqual(Grpc.Core.StatusCode.InvalidArgument, ex.StatusCode,
                    $"Expected InvalidArgument, got {ex.StatusCode}: {ex.Status.Detail}");
            }
            finally
            {
                channel.Dispose();
            }
        }
    }
}
