// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ADMS.Testing.DeltaTableService.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.ADMS.Testing.DeltaTableService.Tests
{
    /// <summary>
    /// Tests for <see cref="DeltaTableProcess"/> — the V3 process lifecycle manager.
    ///
    /// These tests exercise the spawn -> sentinel detection -> health -> shutdown
    /// round-trip against the real Rust binary. They require the binary to be built
    /// first via <c>cargo build</c> in <c>src/DeltaTableService.Server/v3/</c>.
    ///
    /// Run with: dotnet test --filter "TestCategory=Integration&amp;TestCategory=V3Process"
    /// </summary>
    [TestClass]
    [TestCategory("Integration")]
    [TestCategory("V3")]
    [TestCategory("V3Process")]
    public class DeltaTableProcessTests
    {
        /// <summary>
        /// Resolves the path to the Rust binary. Walks up from the test output
        /// directory to find the repo root, then navigates to the build output.
        /// </summary>
        private static string? FindRustBinary()
        {
            // Walk up from the test output directory to find the solution root.
            // Test output is typically: tests/DeltaTableService.Tests/bin/Debug/net8.0/
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

        private static string GetRustBinaryOrSkip()
        {
            string? path = FindRustBinary();
            if (path == null)
            {
                Assert.Inconclusive(
                    "Rust binary not found. Build it first: " +
                    "cd src/DeltaTableService.Server/v3 && cargo build");
            }
            return path!;
        }

        // ================================================================== //
        //  Start + Port Detection
        // ================================================================== //

        [TestMethod]
        public async Task StartAsync_DetectsPortFromSentinel()
        {
            string binary = GetRustBinaryOrSkip();
            await using var process = new DeltaTableProcess();

            await process.StartAsync(binary);

            Assert.IsTrue(process.Port > 0, $"Expected port > 0, got {process.Port}");
            Assert.IsTrue(process.Port <= 65535, $"Expected port <= 65535, got {process.Port}");
            Assert.IsTrue(process.IsRunning, "Expected process to be running after start.");
        }

        // ================================================================== //
        //  Health Check
        // ================================================================== //

        [TestMethod]
        public async Task StartAsync_HealthCheckSucceeds()
        {
            string binary = GetRustBinaryOrSkip();
            await using var process = new DeltaTableProcess();
            await process.StartAsync(binary);

            // StartAsync already polls health internally, but let's verify
            // independently via a fresh client.
            using var client = new DeltaTableServiceClient(
                process.GetFlightUri(), ServiceMode.V3_Rust);
            bool healthy = await client.HealthCheckAsync();

            Assert.IsTrue(healthy, "Expected V3 server health check to return true.");
        }

        // ================================================================== //
        //  Graceful Shutdown
        // ================================================================== //

        [TestMethod]
        public async Task StopAsync_GracefullyShutdownsServer()
        {
            string binary = GetRustBinaryOrSkip();
            await using var process = new DeltaTableProcess();
            await process.StartAsync(binary);

            Assert.IsTrue(process.IsRunning, "Server should be running before stop.");

            await process.StopAsync();

            // Give a brief moment for process exit propagation.
            await Task.Delay(500);
            Assert.IsFalse(process.IsRunning, "Server should not be running after stop.");
        }

        // ================================================================== //
        //  Dispose kills process
        // ================================================================== //

        [TestMethod]
        public async Task DisposeAsync_KillsProcessIfStillRunning()
        {
            string binary = GetRustBinaryOrSkip();
            var process = new DeltaTableProcess();
            await process.StartAsync(binary);
            int port = process.Port;

            Assert.IsTrue(process.IsRunning, "Server should be running before dispose.");

            await process.DisposeAsync();

            // After dispose, the process should be gone.
            // Verify by trying to connect — should fail.
            using var client = new DeltaTableServiceClient(
                new Uri($"http://localhost:{port}"), ServiceMode.V3_Rust);
            bool healthy = await client.HealthCheckAsync();
            Assert.IsFalse(healthy,
                "Expected health check to fail after process disposal.");
        }

        // ================================================================== //
        //  Error: Invalid binary path
        // ================================================================== //

        [TestMethod]
        public async Task StartAsync_InvalidPath_ThrowsFileNotFoundException()
        {
            await using var process = new DeltaTableProcess();

            await Assert.ThrowsExceptionAsync<FileNotFoundException>(async () =>
            {
                await process.StartAsync(@"C:\nonexistent\fake-binary.exe");
            });
        }

        // ================================================================== //
        //  Error: Null/empty path
        // ================================================================== //

        [TestMethod]
        public async Task StartAsync_NullPath_ThrowsArgumentNullException()
        {
            await using var process = new DeltaTableProcess();

            await Assert.ThrowsExceptionAsync<ArgumentNullException>(async () =>
            {
                await process.StartAsync(null!);
            });
        }

        // ================================================================== //
        //  Port property before start
        // ================================================================== //

        [TestMethod]
        public async Task Port_BeforeStart_ThrowsInvalidOperationException()
        {
            await using var process = new DeltaTableProcess();

            Assert.ThrowsException<InvalidOperationException>(() =>
            {
                _ = process.Port;
            });

            await Task.CompletedTask; // Satisfy async signature.
        }

        // ================================================================== //
        //  Multiple start/stop cycles
        // ================================================================== //

        [TestMethod]
        public async Task StartAsync_MultipleSequentialCycles()
        {
            string binary = GetRustBinaryOrSkip();

            // Cycle 1
            await using (var process1 = new DeltaTableProcess())
            {
                await process1.StartAsync(binary);
                Assert.IsTrue(process1.Port > 0);
                Assert.IsTrue(process1.IsRunning);
            }
            // process1 disposed here — should kill the server

            // Cycle 2 — fresh process on a (likely) different port
            await using (var process2 = new DeltaTableProcess())
            {
                await process2.StartAsync(binary);
                Assert.IsTrue(process2.Port > 0);
                Assert.IsTrue(process2.IsRunning);
            }
        }
    }
}
