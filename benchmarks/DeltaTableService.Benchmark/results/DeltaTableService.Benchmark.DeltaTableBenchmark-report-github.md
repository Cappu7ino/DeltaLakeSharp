# DeltaTableService Benchmark Results

## Summary: V2 (DataFusion) vs V1 (PySpark)

Dataset: 1,000,000 rows, partitioned by region into 5 partitions.

| Scenario          | V1 (PySpark) | V2 (DataFusion) | Speedup | V1 Memory     | V2 Memory     | Memory Reduction |
|-------------------|-------------:|----------------:|--------:|---------------:|--------------:|-----------------:|
| ReadTable         |    3,482 ms  |        386 ms   |   9.0x  | 214,820 KB    |  86,119 KB    |           60%    |
| PartitionPruning  |    1,238 ms  |        142 ms   |   8.7x  |  49,898 KB    |  17,939 KB    |           64%    |
| PredicatePushdown |    1,195 ms  |        231 ms   |   5.2x  |   1,132 KB    |     727 KB    |           36%    |
| AggregateGroupBy  |    1,144 ms  |        102 ms   |  11.2x  |     148 KB    |     102 KB    |           31%    |
| DateRangeFilter   |    1,737 ms  |        146 ms   |  11.9x  |  13,280 KB    |   5,329 KB    |           60%    |

V2 (DataFusion) is **5-12x faster** than V1 (PySpark) across all scenarios, with **30-64% lower memory usage**.

## Full BenchmarkDotNet Report

``` ini

BenchmarkDotNet v0.15.7, Windows 11 (10.0.26200.7840/25H2/2025Update/HudsonValley2) (Hyper-V)
Intel Xeon Platinum 8370C CPU 2.80GHz (Max: 2.79GHz), 1 CPU, 16 logical and 8 physical cores
.NET SDK 10.0.103
  [Host]   : .NET 8.0.24 (8.0.24, 8.0.2426.7010), X64 RyuJIT x86-64-v4
  ShortRun : .NET 8.0.24 (8.0.24, 8.0.2426.7010), X64 RyuJIT x86-64-v4

Job=ShortRun  Server=True  IterationCount=3  
LaunchCount=1  WarmupCount=3  

```
| Method                 | RowCount | Mean       | Error       | StdDev    | Median     | Ratio | RatioSD | Gen0     | Gen1     | Gen2     | Allocated    | Alloc Ratio |
|----------------------- |--------- |-----------:|------------:|----------:|-----------:|------:|--------:|---------:|---------:|---------:|-------------:|------------:|
| 'V1 ReadTable'         | 1000000  | 3,482.4 ms | 5,829.47 ms | 319.53 ms | 3,457.0 ms |  1.01 |    0.11 |        - |        - |        - | 214819.94 KB |       1.000 |
| 'V2 ReadTable'         | 1000000  |   385.9 ms | 1,603.69 ms |  87.90 ms |   361.2 ms |  0.11 |    0.02 |        - |        - |        - |  86119.41 KB |       0.401 |
| 'V1 PartitionPruning'  | 1000000  | 1,238.4 ms | 3,657.64 ms | 200.49 ms | 1,168.3 ms |  0.36 |    0.06 |        - |        - |        - |  49897.85 KB |       0.232 |
| 'V2 PartitionPruning'  | 1000000  |   142.0 ms |    94.21 ms |   5.16 ms |   139.2 ms |  0.04 |    0.00 | 200.0000 | 200.0000 | 200.0000 |  17939.43 KB |       0.084 |
| 'V1 PredicatePushdown' | 1000000  | 1,195.3 ms | 2,071.48 ms | 113.54 ms | 1,179.6 ms |  0.35 |    0.04 |        - |        - |        - |   1132.42 KB |       0.005 |
| 'V2 PredicatePushdown' | 1000000  |   230.5 ms | 3,018.06 ms | 165.43 ms |   138.0 ms |  0.07 |    0.04 |        - |        - |        - |    727.02 KB |       0.003 |
| 'V1 AggregateGroupBy'  | 1000000  | 1,144.1 ms | 1,011.82 ms |  55.46 ms | 1,163.6 ms |  0.33 |    0.03 |        - |        - |        - |    148.41 KB |       0.001 |
| 'V2 AggregateGroupBy'  | 1000000  |   101.6 ms |   265.87 ms |  14.57 ms |   101.2 ms |  0.03 |    0.00 |        - |        - |        - |    101.94 KB |       0.000 |
| 'V1 DateRangeFilter'   | 1000000  | 1,737.4 ms | 6,270.28 ms | 343.69 ms | 1,829.5 ms |  0.50 |    0.09 |        - |        - |        - |  13280.16 KB |       0.062 |
| 'V2 DateRangeFilter'   | 1000000  |   145.6 ms |   189.35 ms |  10.38 ms |   149.5 ms |  0.04 |    0.00 |        - |        - |        - |    5328.7 KB |       0.025 |
