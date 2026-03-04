// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;

namespace Microsoft.ADMS.Testing.DeltaTableService.Client
{
    /// <summary>
    /// Manages the lifecycle of the Delta Table Service Docker container.
    /// Supports V1 (PySpark + Arrow Flight) and V2 (DataFusion + Arrow Flight) modes,
    /// and two startup strategies:
    /// <list type="bullet">
    ///   <item><see cref="BuildAndStartAsync"/> — builds the image from a local Dockerfile.</item>
    ///   <item><see cref="PullAndStartAsync"/> — pulls a pre-built image from a registry.</item>
    /// </list>
    /// <para>
    /// <b>V1:</b> Exposes the Arrow Flight port (8815). Use <see cref="GetFlightUri"/>.
    /// </para>
    /// <para>
    /// <b>V2:</b> Exposes the Arrow Flight port (8815). Use <see cref="GetFlightUri"/>.
    /// Lightweight DataFusion + delta-rs backend — no JVM or Spark dependency.
    /// </para>
    /// </summary>
    public sealed class DeltaTableContainer : IAsyncDisposable
    {
        /// <summary>
        /// The container port where the Arrow Flight server listens.
        /// </summary>
        public const int FlightPort = 8815;

        private IContainer _container;
        private IFutureDockerImage _builtImage;
        private ServiceMode _mode = ServiceMode.V1_Spark;

        /// <summary>
        /// Gets the <see cref="ServiceMode"/> this container was started with.
        /// </summary>
        public ServiceMode Mode => _mode;

        /// <summary>
        /// Gets the mapped host port for the Arrow Flight server.
        /// Only valid after the container has started.
        /// </summary>
        public int MappedFlightPort => _container?.GetMappedPublicPort(FlightPort)
            ?? throw new InvalidOperationException("Container has not been started.");

        /// <summary>
        /// Gets the mapped host port for the active service mode.
        /// </summary>
        public int MappedPort => MappedFlightPort;

        /// <summary>
        /// Gets the URI for connecting to the Arrow Flight server.
        /// </summary>
        public Uri GetFlightUri()
        {
            return new Uri($"http://localhost:{MappedFlightPort}");
        }

        /// <summary>
        /// Gets the URI for the active service mode.
        /// </summary>
        public Uri GetServiceUri()
        {
            return GetFlightUri();
        }

        /// <summary>
        /// Builds a Docker image from the specified Dockerfile directory and
        /// starts the container. When <paramref name="skipBuildIfExists"/> is
        /// <c>true</c> and the resolved image already exists locally, the
        /// build step is skipped and the container is started directly from
        /// the existing image. This avoids Docker Desktop lease errors that
        /// occur when rapidly rebuilding large images (e.g. the ~2 GB Spark
        /// image) across BenchmarkDotNet process boundaries.
        /// </summary>
        /// <param name="dockerfilePath">
        /// The directory containing the Dockerfile (the build context).
        /// </param>
        /// <param name="mode">
        /// The service mode. Defaults to <see cref="ServiceMode.V1_Spark"/>.
        /// This determines the Dockerfile, image name, and wait strategy.
        /// <list type="bullet">
        ///   <item>V1: <c>v1/Dockerfile</c>, image <c>delta-table-service:test</c>, port 8815.</item>
        ///   <item>V2: <c>v2/Dockerfile</c>, image <c>delta-table-service-v2:test</c>, port 8815.</item>
        /// </list>
        /// </param>
        /// <param name="dockerfileName">
        /// The Dockerfile name. When <c>null</c>, defaults based on the service mode.
        /// </param>
        /// <param name="imageName">
        /// Optional image name/tag for the built image. When <c>null</c>, defaults
        /// based on the service mode.
        /// </param>
        /// <param name="skipBuildIfExists">
        /// When <c>true</c>, skips the Docker image build if the image already
        /// exists locally. Defaults to <c>false</c> for backward compatibility.
        /// </param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>This instance (for fluent chaining).</returns>
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

        /// <summary>
        /// Checks whether a Docker image exists locally by running
        /// <c>docker image inspect</c>. Returns <c>true</c> when the image
        /// is present, <c>false</c> otherwise.
        /// </summary>
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

                // Drain stdout/stderr to avoid deadlocks.
                _ = process.StandardOutput.ReadToEndAsync();
                _ = process.StandardError.ReadToEndAsync();

                await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
                return process.ExitCode == 0;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Pulls a pre-built Docker image from a registry and starts the container.
        /// </summary>
        /// <param name="imageName">
        /// The fully qualified image name, e.g. <c>myregistry.azurecr.io/delta-table-service:latest</c>.
        /// </param>
        /// <param name="mode">
        /// The service mode. Defaults to <see cref="ServiceMode.V1_Spark"/>.
        /// </param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>This instance (for fluent chaining).</returns>
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

        /// <summary>
        /// Retrieves the stdout and stderr logs from the container.
        /// Useful for post-test diagnostics. Returns <c>("", "")</c> if the
        /// container has not been started.
        /// </summary>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A tuple of (stdout, stderr) log strings.</returns>
        public async Task<(string Stdout, string Stderr)> GetLogsAsync(
            CancellationToken cancellationToken = default)
        {
            if (_container == null)
                return (string.Empty, string.Empty);

            return await _container.GetLogsAsync(ct: cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
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
