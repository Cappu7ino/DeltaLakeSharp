param(
    [string]$Project = "benchmarks/DeltaLakeSharp.Benchmark/DeltaLakeSharp.Benchmark.csproj",
    [switch]$Overwrite
)

$ErrorActionPreference = 'Stop'

$snapshotDatasets = @(
    @{ Size = '1m'; Rows = 1000000; BatchSize = 250000 },
    @{ Size = '2m'; Rows = 2000000; BatchSize = 500000 },
    @{ Size = '5m'; Rows = 5000000; BatchSize = 1000000 },
    @{ Size = '10m'; Rows = 10000000; BatchSize = 2000000 }
)

foreach ($dataset in $snapshotDatasets) {
    $output = "benchmarks/DeltaLakeSharp.Benchmark/TestData/delta-full-read-decimal/$($dataset.Size)"
    $args = @(
        'run',
        '--project', $Project,
        '--framework', 'net8.0',
        '--',
        'generate-dataset',
        '--kind', 'full-read',
        '--schema', 'decimal',
        '--output', $output,
        '--rows', $dataset.Rows,
        '--batch-size', $dataset.BatchSize
    )

    if ($Overwrite) {
        $args += '--overwrite'
    }

    Write-Host "Generating decimal full-read dataset $($dataset.Size) -> $output"
    dotnet @args
}

$cdfOutput = 'benchmarks/DeltaLakeSharp.Benchmark/TestData/delta-full-cdf-decimal'
$cdfArgs = @(
    'run',
    '--project', $Project,
    '--framework', 'net8.0',
    '--',
    'generate-dataset',
    '--kind', 'full-cdf',
    '--schema', 'decimal',
    '--output', $cdfOutput,
    '--rows', '1000000',
    '--batch-size', '250000',
    '--versions', '20',
    '--rows-per-version', '50000'
)

if ($Overwrite) {
    $cdfArgs += '--overwrite'
}

Write-Host "Generating decimal full-cdf dataset -> $cdfOutput"
dotnet @cdfArgs
