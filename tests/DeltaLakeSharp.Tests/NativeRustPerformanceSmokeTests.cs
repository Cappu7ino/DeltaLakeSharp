using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DeltaLakeSharp.Client;
using DeltaLakeSharp.Client.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DeltaLakeSharp.Tests
{
    [TestClass]
    [TestCategory("V3")]
    public class NativeRustPerformanceSmokeTests
    {
        private static readonly TableSchema SmokeSchema = new(new List<ColumnDefinition>
        {
            new ColumnDefinition("id", "int32"),
            new ColumnDefinition("region", "string"),
            new ColumnDefinition("name", "string"),
        });

        [TestMethod]
        public async Task PartitionPlanPayload_IsBoundedForRepresentativeTable()
        {
            string tablePath = V3TestHelpers.CreateTempTablePath("native_v3_perf_many_files");
            try
            {
                using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
                await CreateTableWithRowsAsync(client, tablePath, rowCount: 32, rowsPerBatch: 1, partitionBy: null);

                IReadOnlyList<DeltaReadPartition> partitions = await client.GetReadPartitionsAsync(tablePath);
                int totalTokenBytes = partitions.Sum(partition => partition.Token.Length);
                int maxTokenBytes = partitions.Count == 0 ? 0 : partitions.Max(partition => partition.Token.Length);

                Assert.IsTrue(partitions.Count > 0, "Expected at least one planned read partition.");
                Assert.IsTrue(totalTokenBytes < 256 * 1024, $"Partition tokens are unexpectedly large. Total bytes: {totalTokenBytes}.");
                Assert.IsTrue(maxTokenBytes < 64 * 1024, $"A single partition token is unexpectedly large. Max bytes: {maxTokenBytes}.");
            }
            finally
            {
                V3TestHelpers.CleanupTablePath(tablePath);
            }
        }

        [TestMethod]
        public Task ReadStream_FirstBatchDoesNotRequireFullDrain()
        {
            return ReadFirstBatchAndDisposeAsync(prefetchEnabled: false);
        }

        [TestMethod]
        public Task PrefetchEnabled_FirstBatchStillProducesRows()
        {
            return ReadFirstBatchAndDisposeAsync(prefetchEnabled: true);
        }

        [TestMethod]
        public async Task ManyClientCreation_DoesNotHangOrCrash()
        {
            var clients = new List<DeltaTableServiceClient>();
            try
            {
                for (int i = 0; i < 16; i++)
                {
                    var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
                    clients.Add(client);
                    Assert.IsTrue(await client.HealthCheckAsync(), $"Client {i} health check failed.");
                }
            }
            finally
            {
                foreach (DeltaTableServiceClient client in clients)
                {
                    client.Dispose();
                }
            }
        }

        [TestMethod]
        public async Task PartitionedConcurrentRead_ReturnsAllRows()
        {
            const int expectedRows = 96;
            string tablePath = V3TestHelpers.CreateTempTablePath("native_v3_perf_partitioned");
            try
            {
                using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);
                await CreateTableWithRowsAsync(client, tablePath, expectedRows, rowsPerBatch: 12, partitionBy: new[] { "region" });

                IReadOnlyList<DeltaReadPartition> partitions = await client.GetReadPartitionsAsync(tablePath);
                Assert.IsTrue(partitions.Count > 0, "Expected partition planning to return at least one partition.");

                using var semaphore = new SemaphoreSlim(2);
                Task<long>[] readTasks = partitions
                    .Select(partition => ReadPartitionRowsAsync(client, tablePath, partition, semaphore))
                    .ToArray();
                long[] rowCounts = await Task.WhenAll(readTasks);

                Assert.AreEqual(expectedRows, rowCounts.Sum(), "Concurrent partition reads should return every row exactly once.");
            }
            finally
            {
                V3TestHelpers.CleanupTablePath(tablePath);
            }
        }

        private static async Task ReadFirstBatchAndDisposeAsync(bool prefetchEnabled)
        {
            string tablePath = V3TestHelpers.CreateTempTablePath("native_v3_perf_first_batch");
            try
            {
                var options = new DeltaTableServiceClientOptions
                {
                    EnableNativeReadPrefetch = prefetchEnabled,
                };
                using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust, options);
                await CreateTableWithRowsAsync(client, tablePath, rowCount: 64, rowsPerBatch: 8, partitionBy: null);

                using IArrowArrayStream stream = await client.ReadTableAsArrowStreamAsync(tablePath, batchSize: 8);
                RecordBatch? firstBatch = await stream.ReadNextRecordBatchAsync();

                Assert.IsNotNull(firstBatch, "Expected a first record batch.");
                Assert.IsTrue(firstBatch!.Length > 0, "Expected the first record batch to contain rows.");
            }
            finally
            {
                V3TestHelpers.CleanupTablePath(tablePath);
            }
        }

        private static async Task CreateTableWithRowsAsync(
            DeltaTableServiceClient client,
            string tablePath,
            int rowCount,
            int rowsPerBatch,
            IReadOnlyList<string>? partitionBy)
        {
            ExecuteResult createResult = await client.CreateTableAsync(tablePath, SmokeSchema, partitionBy: partitionBy);
            Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

            int nextId = 1;
            while (nextId <= rowCount)
            {
                int currentRowCount = Math.Min(rowsPerBatch, rowCount - nextId + 1);
                RecordBatch batch = BuildBatch(nextId, currentRowCount);
                await client.InsertAsync(
                    tablePath,
                    batch.Schema,
                    ArrowConverter.ToAsyncEnumerable(batch),
                    SaveMode.Append,
                    partitionBy: partitionBy);
                nextId += currentRowCount;
            }
        }

        private static RecordBatch BuildBatch(int startingId, int rowCount)
        {
            var rows = new object[rowCount][];
            for (int i = 0; i < rowCount; i++)
            {
                int id = startingId + i;
                rows[i] = new object[]
                {
                    id,
                    id % 2 == 0 ? "US" : "EU",
                    $"name-{id}",
                };
            }

            return ArrowConverter.FromRows(rows, SmokeSchema);
        }

        private static async Task<long> ReadPartitionRowsAsync(
            DeltaTableServiceClient client,
            string tablePath,
            DeltaReadPartition partition,
            SemaphoreSlim semaphore)
        {
            await semaphore.WaitAsync();
            try
            {
                long rows = 0;
                await foreach (RecordBatch batch in client.ReadTablePartitionAsync(tablePath, partition, batchSize: 8))
                {
                    rows += batch.Length;
                }

                return rows;
            }
            finally
            {
                semaphore.Release();
            }
        }
    }
}
