using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using Microsoft.DI.Delta.Column;
using Microsoft.DI.Delta.Reader;
using Microsoft.DI.Delta.Store;
using Microsoft.DI.Delta.Table;
using Microsoft.DI.Delta.Writer.DataBlock;
using Microsoft.Data.DeltaLake.Storage;
using Microsoft.DI.DeltaTableService.Client;

namespace DeltaTableService.Benchmark
{
    [MemoryDiagnoser]
    public class DeltaReadPerformanceBenchmark
    {
        private DeltaTableServiceClient _repoClient = null!;
        private DeltaTableOperations _deltaSnapshotTable = null!;
        private DeltaTableOperations _deltaCdfTable = null!;

        [ParamsSource(nameof(ScenarioSources))]
        public DeltaReadBenchmarkScenario Scenario { get; set; } = null!;

        public IEnumerable<DeltaReadBenchmarkScenario> ScenarioSources =>
            FilterScenarios(new[]
            {
                new DeltaReadBenchmarkScenario(
                    label: "1M",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read", "1m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf"),
                    startingVersion: 0,
                    endingVersion: null),
                new DeltaReadBenchmarkScenario(
                    label: "2M",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read", "2m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf"),
                    startingVersion: 0,
                    endingVersion: null),
                new DeltaReadBenchmarkScenario(
                    label: "5M",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read", "5m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf"),
                    startingVersion: 0,
                    endingVersion: null),
                new DeltaReadBenchmarkScenario(
                    label: "10M",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read", "10m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf"),
                    startingVersion: 0,
                    endingVersion: null),
                new DeltaReadBenchmarkScenario(
                    label: "1M Decimal",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read-decimal", "1m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf-decimal"),
                    startingVersion: 0,
                    endingVersion: null),
                new DeltaReadBenchmarkScenario(
                    label: "2M Decimal",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read-decimal", "2m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf-decimal"),
                    startingVersion: 0,
                    endingVersion: null),
                new DeltaReadBenchmarkScenario(
                    label: "5M Decimal",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read-decimal", "5m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf-decimal"),
                    startingVersion: 0,
                    endingVersion: null),
                new DeltaReadBenchmarkScenario(
                    label: "10M Decimal",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read-decimal", "10m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf-decimal"),
                    startingVersion: 0,
                    endingVersion: null),
            });

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            ValidateScenarioPaths(Scenario);

            _repoClient = new DeltaTableServiceClient(ServiceMode.V3_Rust);
            if (!await _repoClient.HealthCheckAsync())
            {
                throw new InvalidOperationException("The repo client V3 native backend is not healthy.");
            }

            _deltaSnapshotTable = CreateDeltaTableOperations(Scenario.SnapshotTablePath);
            _deltaCdfTable = CreateDeltaTableOperations(Scenario.CdfTablePath);

            await RepoClient_FullTableRead();
            await RepoClient_DataReaderFullTableRead();
            await DeltaPackage_FullTableRead();
            await RepoClient_FullChangeDataRead();
            await RepoClient_DataReaderFullChangeDataRead();
            await DeltaPackage_FullChangeDataRead();
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _repoClient?.Dispose();
        }

        [Benchmark(Baseline = true, Description = "Microsoft.DI.Delta full table read")]
        public async Task<ReadIterationResult> DeltaPackage_FullTableRead()
        {
            var options = ReaderOptions.Default;
            DeltaReader reader = await _deltaSnapshotTable.ReadAsync(options, CancellationToken.None);
            return await ConsumeDeltaBlocksAsync(reader.ReadSnapshotDataAsync(CancellationToken.None));
        }

        [Benchmark(Description = "Repo client full table read")]
        public async Task<ReadIterationResult> RepoClient_FullTableRead()
        {
            return await ConsumeArrowBatchesAsync(_repoClient.ReadTableAsync(Scenario.SnapshotTablePath));
        }

        [Benchmark(Description = "Repo client IDataReader full table read")]
        public async Task<ReadIterationResult> RepoClient_DataReaderFullTableRead()
        {
            using DbDataReader reader = await _repoClient.ReadTableAsDataReaderAsync(Scenario.SnapshotTablePath);
            return ConsumeDataReader(reader);
        }

        [Benchmark(Description = "Microsoft.DI.Delta full CDF read")]
        public async Task<ReadIterationResult> DeltaPackage_FullChangeDataRead()
        {
            var options = ReaderOptions.Default;
            options.ChangeDataReadOptions.StartingVersion = checked((int)Scenario.StartingVersion);
            if (Scenario.EndingVersion.HasValue)
            {
                options.ChangeDataReadOptions.EndingVersion = checked((int)Scenario.EndingVersion.Value);
            }

            DeltaReader reader = await _deltaCdfTable.ReadAsync(options, CancellationToken.None);
            return await ConsumeDeltaBlocksAsync(reader.ReadChangeDataAsync(CancellationToken.None));
        }

        [Benchmark(Description = "Repo client full CDF read")]
        public async Task<ReadIterationResult> RepoClient_FullChangeDataRead()
        {
            return await ConsumeArrowBatchesAsync(
                _repoClient.ReadChangeDataAsync(
                    Scenario.CdfTablePath,
                    Scenario.StartingVersion,
                    Scenario.EndingVersion));
        }

        [Benchmark(Description = "Repo client IDataReader full CDF read")]
        public async Task<ReadIterationResult> RepoClient_DataReaderFullChangeDataRead()
        {
            using DbDataReader reader = await _repoClient.ReadChangeDataAsDataReaderAsync(
                Scenario.CdfTablePath,
                Scenario.StartingVersion,
                Scenario.EndingVersion);
            return ConsumeDataReader(reader);
        }

        private static async Task<ReadIterationResult> ConsumeArrowBatchesAsync(IAsyncEnumerable<RecordBatch> batches)
        {
            long rowCount = 0;
            long blockCount = 0;

            await foreach (RecordBatch batch in batches)
            {
                blockCount++;
                rowCount += batch.Length;
            }

            return new ReadIterationResult(rowCount, blockCount);
        }

        private static async Task<ReadIterationResult> ConsumeDeltaBlocksAsync(IAsyncEnumerable<IReadColumnDataBlock> blocks)
        {
            long rowCount = 0;
            long blockCount = 0;

            await foreach (IReadColumnDataBlock block in blocks)
            {
                blockCount++;
                if (block.Data.Length > 0)
                {
                    rowCount += block.Data[0].GetSize();
                }
            }

            return new ReadIterationResult(rowCount, blockCount);
        }

        private static ReadIterationResult ConsumeDataReader(DbDataReader reader)
        {
            long rowCount = 0;
            int fieldCount = reader.FieldCount;

            while (reader.Read())
            {
                rowCount++;
            }

            return new ReadIterationResult(rowCount, fieldCount);
        }

        private static DeltaTableOperations CreateDeltaTableOperations(string tablePath)
        {
            string fullTablePath = Path.GetFullPath(tablePath);
            if (!Directory.Exists(fullTablePath))
            {
                throw new DirectoryNotFoundException($"Benchmark table path '{tablePath}' does not exist.");
            }

            string tableName = Path.GetFileName(fullTablePath);
            IFileSystem fileSystem = new BenchmarkLocalFileSystem(fullTablePath);
            return DeltaTable.ForTable(tableName, fileSystem, fullTablePath);
        }

        private static void ValidateScenarioPaths(DeltaReadBenchmarkScenario scenario)
        {
            if (!Directory.Exists(scenario.SnapshotTablePath))
            {
                throw new DirectoryNotFoundException(
                    $"Snapshot dataset path '{scenario.SnapshotTablePath}' does not exist. Generate it first with the benchmark dataset generator.");
            }

            if (!Directory.Exists(scenario.CdfTablePath))
            {
                throw new DirectoryNotFoundException(
                    $"CDF dataset path '{scenario.CdfTablePath}' does not exist. Generate it first with the benchmark dataset generator.");
            }
        }

        private static IEnumerable<DeltaReadBenchmarkScenario> FilterScenarios(IEnumerable<DeltaReadBenchmarkScenario> scenarios)
        {
            string? filter = Environment.GetEnvironmentVariable("DTS_BENCHMARK_SCENARIO_FILTER");
            if (string.IsNullOrWhiteSpace(filter))
            {
                return scenarios;
            }

            DeltaReadBenchmarkScenario[] filtered = scenarios
                .Where(s => MatchesScenarioFilter(s.Label, filter))
                .ToArray();

            if (filtered.Length == 0)
            {
                throw new InvalidOperationException($"No benchmark scenarios matched filter '{filter}'.");
            }

            return filtered;
        }

        private static bool MatchesScenarioFilter(string label, string filter)
        {
            if (string.Equals(filter, "non-decimal", StringComparison.OrdinalIgnoreCase))
            {
                return label.IndexOf("decimal", StringComparison.OrdinalIgnoreCase) < 0;
            }

            return label.IndexOf(filter, StringComparison.OrdinalIgnoreCase) >= 0;
        }

        public sealed class DeltaReadBenchmarkScenario
        {
            public DeltaReadBenchmarkScenario(string label, string snapshotTablePath, string cdfTablePath, long startingVersion, long? endingVersion)
            {
                Label = label;
                SnapshotTablePath = snapshotTablePath;
                CdfTablePath = cdfTablePath;
                StartingVersion = startingVersion;
                EndingVersion = endingVersion;
            }

            public string Label { get; }

            public string SnapshotTablePath { get; }

            public string CdfTablePath { get; }

            public long StartingVersion { get; }

            public long? EndingVersion { get; }

            public override string ToString()
            {
                return $"{Label}: Snapshot={SnapshotTablePath}, Cdf={CdfTablePath}, CdfStart={StartingVersion}, CdfEnd={(EndingVersion.HasValue ? EndingVersion.Value.ToString() : "latest")}";
            }
        }

        public struct ReadIterationResult
        {
            public ReadIterationResult(long rowCount, long blockCount)
            {
                RowCount = rowCount;
                BlockCount = blockCount;
            }

            public long RowCount { get; }

            public long BlockCount { get; }
        }
    }

    [MemoryDiagnoser]
    public class DeltaReadIDataReaderVsBaselineBenchmark
    {
        private DeltaTableServiceClient _repoClient = null!;
        private DeltaTableOperations _deltaSnapshotTable = null!;
        private DeltaTableOperations _deltaCdfTable = null!;

        [ParamsSource(nameof(ScenarioSources))]
        public DeltaReadPerformanceBenchmark.DeltaReadBenchmarkScenario Scenario { get; set; } = null!;

        public IEnumerable<DeltaReadPerformanceBenchmark.DeltaReadBenchmarkScenario> ScenarioSources =>
            FilterScenarios(new[]
            {
                new DeltaReadPerformanceBenchmark.DeltaReadBenchmarkScenario(
                    label: "10M:",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read", "10m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf"),
                    startingVersion: 0,
                    endingVersion: null),
                new DeltaReadPerformanceBenchmark.DeltaReadBenchmarkScenario(
                    label: "10M Decimal",
                    snapshotTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-read-decimal", "10m"),
                    cdfTablePath: Path.Combine(AppContext.BaseDirectory, "TestData", "delta-full-cdf-decimal"),
                    startingVersion: 0,
                    endingVersion: null),
            });

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            ValidateScenarioPaths(Scenario);

            _repoClient = new DeltaTableServiceClient(ServiceMode.V3_Rust);
            if (!await _repoClient.HealthCheckAsync())
            {
                throw new InvalidOperationException("The repo client V3 native backend is not healthy.");
            }

            _deltaSnapshotTable = CreateDeltaTableOperations(Scenario.SnapshotTablePath);
            _deltaCdfTable = CreateDeltaTableOperations(Scenario.CdfTablePath);

            await DeltaPackage_FullTableRead();
            await RepoClient_DataReaderFullTableRead();
            await DeltaPackage_FullChangeDataRead();
            await RepoClient_DataReaderFullChangeDataRead();
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _repoClient?.Dispose();
        }

        [Benchmark(Baseline = true, Description = "Microsoft.DI.Delta full table read")]
        public async Task<DeltaReadPerformanceBenchmark.ReadIterationResult> DeltaPackage_FullTableRead()
        {
            var options = ReaderOptions.Default;
            DeltaReader reader = await _deltaSnapshotTable.ReadAsync(options, CancellationToken.None);
            return await ConsumeDeltaBlocksAsync(reader.ReadSnapshotDataAsync(CancellationToken.None));
        }

        [Benchmark(Description = "Repo client IDataReader full table read")]
        public async Task<DeltaReadPerformanceBenchmark.ReadIterationResult> RepoClient_DataReaderFullTableRead()
        {
            using DbDataReader reader = await _repoClient.ReadTableAsDataReaderAsync(Scenario.SnapshotTablePath);
            return ConsumeDataReader(reader);
        }

        [Benchmark(Description = "Microsoft.DI.Delta full CDF read")]
        public async Task<DeltaReadPerformanceBenchmark.ReadIterationResult> DeltaPackage_FullChangeDataRead()
        {
            var options = ReaderOptions.Default;
            options.ChangeDataReadOptions.StartingVersion = checked((int)Scenario.StartingVersion);
            if (Scenario.EndingVersion.HasValue)
            {
                options.ChangeDataReadOptions.EndingVersion = checked((int)Scenario.EndingVersion.Value);
            }

            DeltaReader reader = await _deltaCdfTable.ReadAsync(options, CancellationToken.None);
            return await ConsumeDeltaBlocksAsync(reader.ReadChangeDataAsync(CancellationToken.None));
        }

        [Benchmark(Description = "Repo client IDataReader full CDF read")]
        public async Task<DeltaReadPerformanceBenchmark.ReadIterationResult> RepoClient_DataReaderFullChangeDataRead()
        {
            using DbDataReader reader = await _repoClient.ReadChangeDataAsDataReaderAsync(
                Scenario.CdfTablePath,
                Scenario.StartingVersion,
                Scenario.EndingVersion);
            return ConsumeDataReader(reader);
        }

        private static DeltaReadPerformanceBenchmark.ReadIterationResult ConsumeDataReader(DbDataReader reader)
        {
            long rowCount = 0;
            int fieldCount = reader.FieldCount;

            while (reader.Read())
            {
                rowCount++;
            }

            return new DeltaReadPerformanceBenchmark.ReadIterationResult(rowCount, fieldCount);
        }

        private static async Task<DeltaReadPerformanceBenchmark.ReadIterationResult> ConsumeDeltaBlocksAsync(IAsyncEnumerable<IReadColumnDataBlock> blocks)
        {
            long rowCount = 0;
            long blockCount = 0;

            await foreach (IReadColumnDataBlock block in blocks)
            {
                blockCount++;
                if (block.Data.Length > 0)
                {
                    rowCount += block.Data[0].GetSize();
                }
            }

            return new DeltaReadPerformanceBenchmark.ReadIterationResult(rowCount, blockCount);
        }

        private static DeltaTableOperations CreateDeltaTableOperations(string tablePath)
        {
            string fullTablePath = Path.GetFullPath(tablePath);
            if (!Directory.Exists(fullTablePath))
            {
                throw new DirectoryNotFoundException($"Benchmark table path '{tablePath}' does not exist.");
            }

            string tableName = Path.GetFileName(fullTablePath);
            IFileSystem fileSystem = new BenchmarkLocalFileSystem(fullTablePath);
            return DeltaTable.ForTable(tableName, fileSystem, fullTablePath);
        }

        private static void ValidateScenarioPaths(DeltaReadPerformanceBenchmark.DeltaReadBenchmarkScenario scenario)
        {
            if (!Directory.Exists(scenario.SnapshotTablePath))
            {
                throw new DirectoryNotFoundException(
                    $"Snapshot dataset path '{scenario.SnapshotTablePath}' does not exist. Generate it first with the benchmark dataset generator.");
            }

            if (!Directory.Exists(scenario.CdfTablePath))
            {
                throw new DirectoryNotFoundException(
                    $"CDF dataset path '{scenario.CdfTablePath}' does not exist. Generate it first with the benchmark dataset generator.");
            }
        }

        private static IEnumerable<DeltaReadPerformanceBenchmark.DeltaReadBenchmarkScenario> FilterScenarios(IEnumerable<DeltaReadPerformanceBenchmark.DeltaReadBenchmarkScenario> scenarios)
        {
            string? filter = Environment.GetEnvironmentVariable("DTS_BENCHMARK_SCENARIO_FILTER");
            if (string.IsNullOrWhiteSpace(filter))
            {
                return scenarios;
            }

            DeltaReadPerformanceBenchmark.DeltaReadBenchmarkScenario[] filtered = scenarios
                .Where(s => MatchesScenarioFilter(s.Label, filter))
                .ToArray();

            if (filtered.Length == 0)
            {
                throw new InvalidOperationException($"No benchmark scenarios matched filter '{filter}'.");
            }

            return filtered;
        }

        private static bool MatchesScenarioFilter(string label, string filter)
        {
            if (string.Equals(filter, "non-decimal", StringComparison.OrdinalIgnoreCase))
            {
                return label.IndexOf("decimal", StringComparison.OrdinalIgnoreCase) < 0;
            }

            return label.IndexOf(filter, StringComparison.OrdinalIgnoreCase) >= 0;
        }
    }

    internal sealed class BenchmarkLocalFileSystem : IFileSystem
    {
        private readonly string _rootFolderPath;

        public BenchmarkLocalFileSystem(string rootFolderPath)
        {
            _rootFolderPath = Path.GetFullPath(rootFolderPath);
        }

        public string RootFolderPath => _rootFolderPath;

        public Task<IEnumerable<StorageItem>> ListFilesAsync(string folderPath, Predicate<string> filter, bool recursive, CancellationToken cancellationToken)
        {
            string absolutePath = ResolvePath(folderPath);
            if (!Directory.Exists(absolutePath))
            {
                return Task.FromResult<IEnumerable<StorageItem>>(System.Array.Empty<StorageItem>());
            }

            SearchOption searchOption = recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;
            IEnumerable<StorageItem> items = Directory
                .EnumerateFileSystemEntries(absolutePath, "*", searchOption)
                .Where(entry => filter == null || filter(Path.GetFileName(entry)))
                .Select(entry =>
                {
                    bool isDirectory = Directory.Exists(entry);
                    FileSystemInfo info = isDirectory ? new DirectoryInfo(entry) : new FileInfo(entry);
                    string relativePath = GetRelativePath(_rootFolderPath, entry)
                        .Replace(Path.DirectorySeparatorChar, '/')
                        .Replace(Path.AltDirectorySeparatorChar, '/');
                    return new StorageItem(info.Name, relativePath, info.LastWriteTimeUtc);
                });

            return Task.FromResult(items);
        }

        public Task<Stream> OpenReadAsync(string filePath, CancellationToken cancellationToken)
        {
            Stream stream = File.Open(ResolvePath(filePath), FileMode.Open, FileAccess.Read, FileShare.Read);
            return Task.FromResult(stream);
        }

        public async Task<StreamResult> TryOpenReadAsync(string filePath, CancellationToken cancellationToken)
        {
            string absolutePath = ResolvePath(filePath);
            if (!File.Exists(absolutePath))
            {
                return new StreamResult(exists: false, stream: null);
            }

            Stream stream = await OpenReadAsync(filePath, cancellationToken);
            return new StreamResult(exists: true, stream);
        }

        public async Task<StreamResult> TryCreateFileAsync(string filePath, bool allowOverwrite, CancellationToken cancellationToken)
        {
            string absolutePath = ResolvePath(filePath);
            string? directory = Path.GetDirectoryName(absolutePath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            if (!allowOverwrite && File.Exists(absolutePath))
            {
                return new StreamResult(exists: true, stream: null);
            }

            Stream stream = File.Open(absolutePath, allowOverwrite ? FileMode.Create : FileMode.CreateNew, FileAccess.Write, FileShare.None);
            return await Task.FromResult(new StreamResult(exists: false, stream));
        }

        public Task<StreamResult> TryCreateStreamForUploadAsync(string filePath, bool allowOverwrite, CancellationToken cancellationToken)
        {
            return TryCreateFileAsync(filePath, allowOverwrite, cancellationToken);
        }

        public Task<bool> ExistsAsync(string filePath, CancellationToken cancellationToken)
        {
            string absolutePath = ResolvePath(filePath);
            return Task.FromResult(File.Exists(absolutePath) || Directory.Exists(absolutePath));
        }

        public Task CreateFolderIdempotentAsync(string folderPath, CancellationToken cancellationToken)
        {
            Directory.CreateDirectory(ResolvePath(folderPath));
            return Task.CompletedTask;
        }

        public Task<bool> TryDeleteAsync(string filePath, CancellationToken cancellationToken)
        {
            string absolutePath = ResolvePath(filePath);
            if (File.Exists(absolutePath))
            {
                File.Delete(absolutePath);
                return Task.FromResult(true);
            }

            if (Directory.Exists(absolutePath))
            {
                Directory.Delete(absolutePath, recursive: true);
                return Task.FromResult(true);
            }

            return Task.FromResult(false);
        }

        public async Task<bool> TryRenameAsync(string sourceFilePath, string destFilePath, bool allowOverwrite, CancellationToken cancellationToken)
        {
            string source = ResolvePath(sourceFilePath);
            string destination = ResolvePath(destFilePath);

            if (!File.Exists(source) && !Directory.Exists(source))
            {
                return false;
            }

            string? destinationDirectory = Path.GetDirectoryName(destination);
            if (!string.IsNullOrEmpty(destinationDirectory))
            {
                Directory.CreateDirectory(destinationDirectory);
            }

            if (!allowOverwrite && (File.Exists(destination) || Directory.Exists(destination)))
            {
                return false;
            }

            if (File.Exists(destination))
            {
                File.Delete(destination);
            }
            else if (Directory.Exists(destination))
            {
                Directory.Delete(destination, recursive: true);
            }

            if (File.Exists(source))
            {
                File.Move(source, destination);
            }
            else
            {
                Directory.Move(source, destination);
            }

            return await Task.FromResult(true);
        }

        public Task<long> GetFileLengthAsync(string filePath, CancellationToken cancellationToken)
        {
            string absolutePath = ResolvePath(filePath);
            return Task.FromResult(new FileInfo(absolutePath).Length);
        }

        public async Task DownloadAsync(string filePath, Stream destination, CancellationToken cancellation)
        {
            using (Stream source = await OpenReadAsync(filePath, cancellation))
            {
                await source.CopyToAsync(destination, 81920, cancellation);
            }
        }

        public async Task UploadAsync(string filePath, byte[] data, CancellationToken cancellation)
        {
            using (Stream destination = (await TryCreateStreamForUploadAsync(filePath, allowOverwrite: true, cancellation)).Stream
                ?? throw new IOException($"Could not create destination stream for '{filePath}'."))
            {
                await destination.WriteAsync(data, 0, data.Length, cancellation);
            }
        }

        public async Task UploadAsync(string srcFilePath, string destinationFilePath, CancellationToken cancellation)
        {
            using (Stream source = File.OpenRead(srcFilePath))
            using (Stream destination = (await TryCreateStreamForUploadAsync(destinationFilePath, allowOverwrite: true, cancellation)).Stream
                ?? throw new IOException($"Could not create destination stream for '{destinationFilePath}'."))
            {
                await source.CopyToAsync(destination, 81920, cancellation);
            }
        }

        public Task<(bool, StorageItem)> TryGetFileAsync(string filePath, CancellationToken cancellationToken)
        {
            string absolutePath = ResolvePath(filePath);
            if (!File.Exists(absolutePath))
            {
                return Task.FromResult((false, default(StorageItem)));
            }

            var info = new FileInfo(absolutePath);
            string relativePath = GetRelativePath(_rootFolderPath, absolutePath)
                .Replace(Path.DirectorySeparatorChar, '/')
                .Replace(Path.AltDirectorySeparatorChar, '/');
            return Task.FromResult((true, new StorageItem(info.Name, relativePath, info.LastWriteTimeUtc)));
        }

        public string CombinePaths(params string[] paths)
        {
            string combined = paths == null || paths.Length == 0
                ? string.Empty
                : Path.Combine(paths.Where(p => !string.IsNullOrEmpty(p)).ToArray());
            return NormalizePath(combined);
        }

        public string GetDirectoryFromPath(string path)
        {
            string resolved = ResolvePath(path);
            string? directory = Path.GetDirectoryName(resolved);
            return directory == null ? string.Empty : NormalizePath(directory);
        }

        private string ResolvePath(string path)
        {
            string normalized = NormalizePath(path);
            if (Path.IsPathRooted(normalized))
            {
                return normalized;
            }

            return Path.GetFullPath(Path.Combine(_rootFolderPath, normalized));
        }

        private static string NormalizePath(string path)
        {
            return path.Replace('/', Path.DirectorySeparatorChar)
                .Replace('\\', Path.DirectorySeparatorChar);
        }

        private static string GetRelativePath(string rootPath, string fullPath)
        {
            Uri rootUri = new Uri(AppendDirectorySeparator(Path.GetFullPath(rootPath)));
            Uri fullUri = new Uri(Path.GetFullPath(fullPath));
            string relative = Uri.UnescapeDataString(rootUri.MakeRelativeUri(fullUri).ToString());
            return relative.Replace('/', Path.DirectorySeparatorChar);
        }

        private static string AppendDirectorySeparator(string path)
        {
            if (path.EndsWith(Path.DirectorySeparatorChar.ToString(), StringComparison.Ordinal)
                || path.EndsWith(Path.AltDirectorySeparatorChar.ToString(), StringComparison.Ordinal))
            {
                return path;
            }

            return path + Path.DirectorySeparatorChar;
        }
    }
}
