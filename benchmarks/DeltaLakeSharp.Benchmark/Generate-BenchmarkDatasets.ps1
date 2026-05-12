param(
    [string]$Project = "benchmarks/DeltaLakeSharp.Benchmark/DeltaLakeSharp.Benchmark.csproj",
    [switch]$Overwrite
)

$ErrorActionPreference = 'Stop'

$datasets = @(
    @{ Size = '1m'; Rows = 1000000; BatchSize = 250000 },
    @{ Size = '2m'; Rows = 2000000; BatchSize = 500000 },
    @{ Size = '5m'; Rows = 5000000; BatchSize = 1000000 },
    @{ Size = '10m'; Rows = 10000000; BatchSize = 2000000 }
)

foreach ($dataset in $datasets) {
    $output = "benchmarks/DeltaLakeSharp.Benchmark/TestData/delta-full-read/$($dataset.Size)"
    $args = @(
        'run',
        '--project', $Project,
        '--framework', 'net8.0',
        '--',
        'generate-dataset',
        '--kind', 'full-read',
        '--output', $output,
        '--rows', $dataset.Rows,
        '--batch-size', $dataset.BatchSize
    )

    if ($Overwrite) {
        $args += '--overwrite'
    }

    Write-Host "Generating full-read dataset $($dataset.Size) -> $output"
    dotnet @args
}
