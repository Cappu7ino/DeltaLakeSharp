// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;
using Microsoft.DI.DeltaTableService.Client;
using Microsoft.DI.DeltaTableService.Testing.Internal.Compat;

namespace Microsoft.DI.DeltaTableService.Testing
{
    /// <summary>
    /// Manages the lifecycle of the Delta Table Service Docker container.
    /// Supports V1 (PySpark + Arrow Flight) and V2 (DataFusion + Arrow Flight)
    /// harness scenarios. V3 uses in-process native interop and does not use this helper.
    /// </summary>
    public sealed class DeltaTableContainer : IAsyncDisposable
    {
        public const int FlightPort = 8815;

        private IContainer _container;
        private IFutureDockerImage _builtImage;
        private ServiceMode _mode = ServiceMode.V1_Spark;

        public ServiceMode Mode => _mode;

        public int MappedFlightPort => _container?.GetMappedPublicPort(FlightPort)
            ?? throw new InvalidOperationException("Container has not been started.");

        public int MappedPort => MappedFlightPort;

        public Uri GetFlightUri()
        {
            return new Uri($"http://localhost:{MappedFlightPort}");
        }

        public Uri GetServiceUri()
        {
            return GetFlightUri();
        }

        public async Task<DeltaTableContainer> BuildAndStartAsync(
            string dockerfilePath,
            ServiceMode mode = ServiceMode.V1_Spark,
            string dockerfileName = null,
            string imageName = null,
            bool skipBuildIfExists = false,
            CancellationToken cancellationToken = default)
        {
            _mode = mode;

            string resolvedDockerfile = dockerfileName ?? mode switch
            {
                ServiceMode.V2_DataFusion => "v2/Dockerfile",
                _ => "v1/Dockerfile",
            };
            string resolvedImageName = imageName ?? mode switch
            {
                ServiceMode.V2_DataFusion => "delta-table-service-v2:test",
                _ => "delta-table-service:test",
            };
            int containerPort = FlightPort;

            bool imageExists = skipBuildIfExists && await ImageExistsAsync(resolvedImageName, cancellationToken).ConfigureAwait(false);

            if (!imageExists)
            {
                _builtImage = new ImageFromDockerfileBuilder()
                    .WithDockerfileDirectory(dockerfilePath)
                    .WithDockerfile(resolvedDockerfile)
                    .WithName(resolvedImageName)
                    .WithCleanUp(true)
                    .Build();

                await _builtImage.CreateAsync(cancellationToken).ConfigureAwait(false);
            }

            var containerBuilder = new ContainerBuilder()
                .WithPortBinding(containerPort, true)
                .WithWaitStrategy(
                    Wait.ForUnixContainer()
                        .UntilPortIsAvailable(containerPort))
                .WithCleanUp(true);

            _container = imageExists
                ? containerBuilder.WithImage(resolvedImageName).Build()
                : containerBuilder.WithImage(_builtImage).Build();

            await _container.StartAsync(cancellationToken).ConfigureAwait(false);
            return this;
        }

        private static async Task<bool> ImageExistsAsync(
            string imageName, CancellationToken cancellationToken = default)
        {
            try
            {
                using var process = new Process
                {
                    StartInfo = new ProcessStartInfo
                    {
                        FileName = "docker",
                        Arguments = $"image inspect {imageName}",
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        UseShellExecute = false,
                        CreateNoWindow = true,
                    },
                };

                process.Start();

                _ = process.StandardOutput.ReadToEndAsync();
                _ = process.StandardError.ReadToEndAsync();

                await ProcessCompat.WaitForExitAsync(process, cancellationToken).ConfigureAwait(false);
                return process.ExitCode == 0;
            }
            catch
            {
                return false;
            }
        }

        public async Task<DeltaTableContainer> PullAndStartAsync(
            string imageName,
            ServiceMode mode = ServiceMode.V1_Spark,
            CancellationToken cancellationToken = default)
        {
            _mode = mode;

            int containerPort = FlightPort;

            _container = new ContainerBuilder()
                .WithImage(imageName)
                .WithPortBinding(containerPort, true)
                .WithWaitStrategy(
                    Wait.ForUnixContainer()
                        .UntilPortIsAvailable(containerPort))
                .WithCleanUp(true)
                .Build();

            await _container.StartAsync(cancellationToken).ConfigureAwait(false);
            return this;
        }

        public async Task<(string Stdout, string Stderr)> GetLogsAsync(
            CancellationToken cancellationToken = default)
        {
            if (_container == null)
            {
                return (string.Empty, string.Empty);
            }

            return await _container.GetLogsAsync(ct: cancellationToken).ConfigureAwait(false);
        }

        public async ValueTask DisposeAsync()
        {
            if (_container != null)
            {
                await _container.DisposeAsync().ConfigureAwait(false);
                _container = null;
            }

            if (_builtImage != null)
            {
                await _builtImage.DisposeAsync().ConfigureAwait(false);
                _builtImage = null;
            }
        }
    }
}
