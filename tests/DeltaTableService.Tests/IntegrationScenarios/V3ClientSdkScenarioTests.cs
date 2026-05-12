// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Microsoft.DI.DeltaTableService.Client;
using Microsoft.DI.DeltaTableService.Client.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.DI.DeltaTableService.Tests.IntegrationScenarios
{
    [TestClass]
    [TestCategory("Integration")]
    [TestCategory("V3")]
    [TestCategory("Scenario")]
    public sealed class V3ClientSdkScenarioTests
    {
        private V3TestHelpers.IntegrationContext? _context;

        [TestInitialize]
        public async Task TestInitialize()
        {
            _context = await V3TestHelpers.CreateIntegrationContextAsync();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            _context?.Dispose();
        }

        private DeltaTableServiceClient Client => _context!.Client;

        private string TestTablePath => _context!.TestTablePath;

        private string PartitionedTablePath => _context!.PartitionedTablePath;

        private string NewWriteTestTablePath() => _context!.CreateWriteTestTablePath();

        [TestMethod]
        public async Task ExternalSdkConsumer_ReadsViaStreamingArrowDataReaderAndSql()
        {
            Assert.AreEqual(ServiceMode.V3_Rust, Client.Mode);

            List<(int id, string name)> streamedRows = await ReadIdNameRowsAsync(
                Client.ReadTableAsync(TestTablePath, batchSize: 2));
            CollectionAssert.AreEqual(
                new[] { (1, "a"), (2, "b"), (3, "c") },
                streamedRows,
                "Expected streaming Arrow reads to return the fixture rows.");

            using DbDataReader reader = await Client.ReadTableAsDataReaderAsync(
                TestTablePath,
                batchSize: 2,
                options: new DeltaDataReaderOptions
                {
                    DecimalBehavior = DeltaDataReaderDecimalBehavior.OverflowDecimalAsString,
                });

            var dataReaderRows = new List<(int id, string name)>();
            while (reader.Read())
            {
                dataReaderRows.Add((reader.GetInt32(0), reader.GetString(1)));
            }

            dataReaderRows = dataReaderRows.OrderBy(row => row.id).ToList();
            CollectionAssert.AreEqual(streamedRows, dataReaderRows,
                "Expected DbDataReader rows to match streaming Arrow rows.");

            List<(int id, string name)> sqlRows = await ReadIdNameRowsAsync(
                Client.ExecuteQueryAsync(
                    "SELECT id, name FROM sdk_table WHERE id >= 2 ORDER BY id",
                    TestTablePath,
                    "sdk_table"));

            CollectionAssert.AreEqual(
                new[] { (2, "b"), (3, "c") },
                sqlRows,
                "Expected SQL query to project and filter fixture rows.");
        }

        [TestMethod]
        public async Task ExternalSdkConsumer_UsesPartitionPlanningAndChangeDataFeed()
        {
            IReadOnlyList<DeltaReadPartition> partitions = await Client.GetReadPartitionsAsync(PartitionedTablePath);
            Assert.IsTrue(partitions.Count > 0, "Expected V3 to plan at least one read partition.");
            Assert.IsTrue(partitions.All(partition => !string.IsNullOrWhiteSpace(partition.Token)),
                "Expected all read partitions to expose opaque tokens.");

            var partitionRows = new List<(int id, string region, string name)>();
            foreach (DeltaReadPartition partition in partitions)
            {
                await foreach (RecordBatch batch in Client.ReadTablePartitionAsync(PartitionedTablePath, partition))
                {
                    var idArray = (Int32Array)batch.Column(0);
                    IArrowArray nameArray = batch.Column(1);
                    IArrowArray regionArray = batch.Column(2);

                    for (int rowIndex = 0; rowIndex < batch.Length; rowIndex++)
                    {
                        partitionRows.Add((
                            idArray.GetValue(rowIndex) ?? -1,
                            V3TestHelpers.ReadStringValue(regionArray, rowIndex),
                            V3TestHelpers.ReadStringValue(nameArray, rowIndex)));
                    }
                }
            }

            partitionRows = partitionRows.OrderBy(row => row.id).ToList();
            CollectionAssert.AreEqual(
                new[]
                {
                    (1, "us", "a"),
                    (2, "eu", "b"),
                    (3, "us", "c"),
                    (4, "eu", "d"),
                    (5, "apac", "e"),
                },
                partitionRows,
                "Expected partition reads to cover the full partitioned fixture table.");

            string cdfTablePath = NewWriteTestTablePath();
            var tableSchema = new TableSchema(new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            });

            ExecuteResult createResult = await Client.CreateTableAsync(
                cdfTablePath,
                tableSchema,
                configuration: new Dictionary<string, string>
                {
                    ["delta.enableChangeDataFeed"] = "true",
                });
            Assert.IsTrue(createResult.Success, $"CreateTableAsync failed: {createResult.Message}");

            Schema arrowSchema = V3TestHelpers.BuildIdNameSchema();
            await Client.InsertAsync(
                cdfTablePath,
                arrowSchema,
                V3TestHelpers.ToAsyncEnumerable(V3TestHelpers.BuildIdNameBatch(
                    new[] { 1, 2 },
                    new[] { "a", "b" })),
                SaveMode.Append);

            ExecuteResult updateResult = await Client.UpdateAsync(
                "UPDATE sdk_table SET name = 'b2' WHERE id = 2",
                cdfTablePath,
                "sdk_table");
            Assert.IsTrue(updateResult.Success, $"UpdateAsync failed: {updateResult.Message}");

            ExecuteResult deleteResult = await Client.DeleteAsync(
                "DELETE FROM sdk_table WHERE id = 1",
                cdfTablePath,
                "sdk_table");
            Assert.IsTrue(deleteResult.Success, $"DeleteAsync failed: {deleteResult.Message}");

            List<Dictionary<string, object?>> cdfRows = await V3TestHelpers.ExecuteChangeDataQueryRowsAsync(
                Client,
                "SELECT id, name, _change_type FROM _cdf WHERE _change_type <> 'update_preimage' ORDER BY id, _change_type",
                cdfTablePath,
                startingVersion: 1);

            Assert.IsTrue(cdfRows.Count >= 3, $"Expected CDF rows for insert, update, and delete changes; got {cdfRows.Count}.");
            Assert.IsFalse(cdfRows.Any(row => Equals(row["_change_type"], "update_preimage")),
                "Expected the CDF query filter to remove update preimage rows.");
            Assert.IsTrue(cdfRows.Any(row => Equals(row["id"], 1) && Equals(row["_change_type"], "delete")),
                "Expected delete CDF row for id=1.");
            Assert.IsTrue(cdfRows.Any(row => Equals(row["id"], 2) && Equals(row["name"], "b2") && Equals(row["_change_type"], "update_postimage")),
                "Expected update_postimage CDF row for id=2.");
        }

        private static async Task<List<(int id, string name)>> ReadIdNameRowsAsync(
            IAsyncEnumerable<RecordBatch> batches)
        {
            var rows = new List<(int id, string name)>();
            await foreach (RecordBatch batch in batches)
            {
                var idArray = (Int32Array)batch.Column(0);
                IArrowArray nameArray = batch.Column(1);

                for (int rowIndex = 0; rowIndex < batch.Length; rowIndex++)
                {
                    rows.Add((
                        idArray.GetValue(rowIndex) ?? -1,
                        V3TestHelpers.ReadStringValue(nameArray, rowIndex)));
                }
            }

            return rows.OrderBy(row => row.id).ToList();
        }
    }
}
