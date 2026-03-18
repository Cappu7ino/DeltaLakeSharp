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

namespace DeltaTableService.Benchmark
{
    /// <summary>
    /// Custom entry point for the DeltaTableService benchmark application.
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
            Logger.Info("Started running DeltaTableService performance benchmarks.");

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
                if (args != null && args.Length != 0)
                {
                    isTrial = args.Contains("-trial", StringComparer.OrdinalIgnoreCase);
                    decimalOnly = args.Contains("-decimal-only", StringComparer.OrdinalIgnoreCase);
                    nonDecimalOnly = args.Contains("-non-decimal-only", StringComparer.OrdinalIgnoreCase);
                    bdnArgs = args.Where(a => !string.Equals(a, "-trial", StringComparison.OrdinalIgnoreCase)
                                           && !string.Equals(a, "-decimal-only", StringComparison.OrdinalIgnoreCase)
                                           && !string.Equals(a, "-non-decimal-only", StringComparison.OrdinalIgnoreCase)).ToArray();
                }

                string? scenarioFilter = decimalOnly ? "decimal" : nonDecimalOnly ? "non-decimal" : null;
                Environment.SetEnvironmentVariable("DTS_BENCHMARK_SCENARIO_FILTER", scenarioFilter);

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

            Logger.Info("Completed running DeltaTableService performance benchmarks.");
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
                    .WithRuntime(ClrRuntime.Net472)
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
                    .WithRuntime(ClrRuntime.Net472)
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
