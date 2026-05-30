using System;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Diagnostics.Windows;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using BenchmarkDotNet.Exporters.Csv;
using BenchmarkDotNet.Exporters.Json;
using System.Security.Principal;
using System.Net;
using System.Runtime.InteropServices;

namespace DeltaLakeSharp.Benchmark
{
    /// <summary>
    /// Custom entry point for the DeltaLakeSharp benchmark application.
    /// Supports three configurations: Debug (in-process), Trial (short run), and Full (default).
    /// </summary>
    internal class Program
    {
        /// <summary>
        /// The custom entry point of the benchmark app.
        /// It allows the passing of command line arguments to customize benchmark execution.
        /// Pass <c>-trial</c> to use the short-run trial configuration in Release mode.
        /// </summary>
        /// <param name="args">the command line arguments</param>
        public static void Main(string[] args)
        {
            ConfigureLegacyFrameworkNetworking();
            Logger.Info("Started running DeltaLakeSharp performance benchmarks.");

            try
            {
                if (args.Length > 0 && string.Equals(args[0], "generate-dataset", StringComparison.OrdinalIgnoreCase))
                {
                    int exitCode = BenchmarkDatasetGenerator.RunAsync(args.Skip(1).ToArray())
                        .GetAwaiter()
                        .GetResult();
                    Environment.ExitCode = exitCode;
                    return;
                }

                string[]? bdnArgs = null;

                bool isTrial = false;
                bool decimalOnly = false;
                bool nonDecimalOnly = false;
                bool generateV3Datasets = false;
                bool overwriteV3Datasets = false;
                string? explicitScenarioFilter = null;
                string? v3DatasetFilter = null;
                string? v3PrefetchFilter = null;
                string? v3ConcurrencyFilter = null;
                string? v3DatasetOutputRoot = null;
                if (args != null && args.Length != 0)
                {
                    isTrial = args.Contains("-trial", StringComparer.OrdinalIgnoreCase);
                    decimalOnly = args.Contains("-decimal-only", StringComparer.OrdinalIgnoreCase);
                    nonDecimalOnly = args.Contains("-non-decimal-only", StringComparer.OrdinalIgnoreCase);

                    var forwardedArgs = new System.Collections.Generic.List<string>();
                    for (int i = 0; i < args.Length; i++)
                    {
                        string arg = args[i];
                        if (string.Equals(arg, "-trial", StringComparison.OrdinalIgnoreCase)
                            || string.Equals(arg, "-decimal-only", StringComparison.OrdinalIgnoreCase)
                            || string.Equals(arg, "-non-decimal-only", StringComparison.OrdinalIgnoreCase))
                        {
                            continue;
                        }

                        if (string.Equals(arg, "--generate-datasets", StringComparison.OrdinalIgnoreCase))
                        {
                            generateV3Datasets = true;
                            continue;
                        }

                        if (string.Equals(arg, "--overwrite-datasets", StringComparison.OrdinalIgnoreCase))
                        {
                            overwriteV3Datasets = true;
                            continue;
                        }

                        if (string.Equals(arg, "-scenario", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
                        {
                            explicitScenarioFilter = args[++i];
                            continue;
                        }

                        if (string.Equals(arg, "--dataset", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
                        {
                            v3DatasetFilter = args[++i];
                            continue;
                        }

                        if (string.Equals(arg, "--prefetch", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
                        {
                            v3PrefetchFilter = args[++i];
                            continue;
                        }

                        if (string.Equals(arg, "--concurrency", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
                        {
                            v3ConcurrencyFilter = args[++i];
                            continue;
                        }

                        if (string.Equals(arg, "--output", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
                        {
                            v3DatasetOutputRoot = args[++i];
                            continue;
                        }

                        forwardedArgs.Add(arg);
                    }

                    bdnArgs = forwardedArgs.ToArray();
                }

                string? scenarioFilter = explicitScenarioFilter ?? (decimalOnly ? "decimal" : nonDecimalOnly ? "non-decimal" : null);
                Environment.SetEnvironmentVariable("DTS_BENCHMARK_SCENARIO_FILTER", scenarioFilter);
                Environment.SetEnvironmentVariable("DTS_BENCHMARK_V3_DATASET_FILTER", v3DatasetFilter);
                Environment.SetEnvironmentVariable("DTS_BENCHMARK_V3_PREFETCH", v3PrefetchFilter);
                Environment.SetEnvironmentVariable("DTS_BENCHMARK_V3_CONCURRENCY", v3ConcurrencyFilter);
                if (!string.IsNullOrWhiteSpace(v3DatasetOutputRoot))
                {
                    Environment.SetEnvironmentVariable("DTS_BENCHMARK_V3_DATASET_ROOT", v3DatasetOutputRoot);
                }

                if (generateV3Datasets)
                {
                    V3BenchmarkDatasetManager.GenerateSelectedProfilesAsync(v3DatasetFilter, v3DatasetOutputRoot, overwriteV3Datasets)
                        .GetAwaiter()
                        .GetResult();
                }

                IConfig config =
#if DEBUG
                    new BenchmarkConfigForDebug();
#else
                    isTrial ? new BenchmarkConfigForTrial() : new BenchmarkConfig();
#endif
                BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(bdnArgs, config);
            }
            catch (Exception ex)
            {
                Logger.Error("Error running performance benchmarks. See logs for details.");
                Logger.Error(ex.ToString());
                throw;
            }

            Logger.Info("Completed running DeltaLakeSharp performance benchmarks.");
        }

        private static void ConfigureLegacyFrameworkNetworking()
        {
#if NET472
            AppContext.SetSwitch("Switch.System.Net.DontEnableSchUseStrongCrypto", false);
            AppContext.SetSwitch("Switch.System.Net.DontEnableSystemDefaultTlsVersions", false);
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
#endif
        }

        /// <summary>
        /// Configuration for debugging benchmark code.
        /// Uses <see cref="InProcessEmitToolchain"/> so breakpoints work in Visual Studio.
        /// Runs a single dry iteration with no warmup.
        /// </summary>
        internal class BenchmarkConfigForDebug : ManualConfig
        {
            public BenchmarkConfigForDebug()
            {
                AddJob(Job.Dry
                    .WithToolchain(new InProcessEmitToolchain(TimeSpan.FromHours(1), logOutput: true)));
                AddLogger(ConsoleLogger.Default);
                AddColumnProvider(DefaultColumnProviders.Instance);
                WithOptions(ConfigOptions.StopOnFirstError);
                WithOptions(ConfigOptions.DisableOptimizationsValidator);
            }
        }

        /// <summary>
        /// Configuration for conducting benchmark through a trial run.
        /// Use this config when you want to quickly verify changes to benchmark code and
        /// get a preliminary measurement of performance impact.
        /// Activate by passing <c>-trial</c> on the command line (Release builds only).
        /// </summary>
        internal class BenchmarkConfigForTrial : ManualConfig
        {
            public BenchmarkConfigForTrial()
            {
                AddJob(Job.ShortRun
                    .WithRuntime(Program.GetDefaultBenchmarkRuntime())
                    .WithGcServer(true)
                    .WithEnvironmentVariable("COMPlus_EnableEventLog", "1"));
                WithOptions(ConfigOptions.StopOnFirstError);
                AddLogger(ConsoleLogger.Default);
                AddAnalyser(EnvironmentAnalyser.Default);
                AddDiagnoser(MemoryDiagnoser.Default);
                Program.AddNativeProfilerIfSupported(this);
                AddColumnProvider(DefaultColumnProviders.Instance);
                AddExporter(MarkdownExporter.GitHub);
            }
        }

        /// <summary>
        /// Configuration for running benchmarks to get accurate and rich measurement results for analysis.
        /// Uses <see cref="Job.Default"/> with server GC, memory diagnostics, and CSV/JSON/Markdown exporters.
        /// </summary>
        internal class BenchmarkConfig : ManualConfig
        {
            public BenchmarkConfig()
            {
                AddJob(Job.Default
                    .WithRuntime(Program.GetDefaultBenchmarkRuntime())
                    .WithGcServer(true)
                    .WithEnvironmentVariable("COMPlus_EnableEventLog", "1"));
                WithOptions(ConfigOptions.StopOnFirstError);
                AddLogger(ConsoleLogger.Default);
                AddAnalyser(EnvironmentAnalyser.Default);
                AddDiagnoser(MemoryDiagnoser.Default);
                Program.AddNativeProfilerIfSupported(this);
                AddColumnProvider(DefaultColumnProviders.Instance);
                AddExporter(CsvExporter.Default, JsonExporter.Brief);
                AddExporter(MarkdownExporter.GitHub);
            }
        }

        private static void AddNativeProfilerIfSupported(ManualConfig config)
        {
            if (IsProcessElevated())
            {
                config.AddDiagnoser(new NativeMemoryProfiler());
            }
            else
            {
                Logger.Info("Skipping NativeMemoryProfiler because the process is not running as Administrator.");
            }
        }

        private static Runtime GetDefaultBenchmarkRuntime()
        {
            return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? ClrRuntime.Net472
                : CoreRuntime.Core80;
        }

        private static bool IsProcessElevated()
        {
            try
            {
                using var identity = WindowsIdentity.GetCurrent();
                var principal = new WindowsPrincipal(identity);
                return principal.IsInRole(WindowsBuiltInRole.Administrator);
            }
            catch
            {
                return false;
            }
        }
    }
}
