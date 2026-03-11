// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System.Threading.Tasks;
using Microsoft.ADMS.Testing.DeltaTableService.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.ADMS.Testing.DeltaTableService.Tests
{
    /// <summary>
    /// OneLake end-to-end integration tests using the V1 (PySpark) backend.
    /// Reads an existing Delta table from a Fabric Lakehouse via SAS authentication.
    ///
    /// <para>
    /// V1 provides full Delta Lake feature support including column mapping
    /// and deletion vectors, making it the safest choice for tables created
    /// by Spark / Fabric.
    /// </para>
    ///
    /// <para>
    /// Each test method specifies its own workspace, lakehouse, and table
    /// via <see cref="OneLakeIntegrationTestBase.GetOneLakeTableConfigAsync"/>.
    /// </para>
    ///
    /// <para>
    /// Requires Docker and Azure CLI login (<c>az login</c>).
    /// </para>
    ///
    /// Run with: <c>dotnet vstest &lt;dll&gt; --TestCaseFilter:"TestCategory=OneLake&amp;TestCategory=V1" /Platform:x64</c>
    /// </summary>
    [TestClass]
    [TestCategory("OneLake")]
    [TestCategory("V1")]
    public class OneLakeV1IntegrationTests : OneLakeIntegrationTestBase
    {
        [ClassInitialize]
        public static async Task ClassInitialize(TestContext context)
        {
            await InitializeAsync(ServiceMode.V1_Spark);
        }

        [ClassCleanup]
        public static async Task ClassCleanup()
        {
            await CleanupAsync();
        }

        // ================================================================== //
        //  Health check
        // ================================================================== //

        [TestMethod]
        public async Task OneLake_V1_HealthCheck_ReturnsTrue()
        {
            bool healthy = await Client.HealthCheckAsync();
            Assert.IsTrue(healthy, "Expected the V1 server to report healthy.");
        }

        // ================================================================== //
        //  Read table
        // ================================================================== //

        [TestMethod]
        public async Task OneLake_V1_ReadTable_ReturnsData()
        {
            var (config, abfssPath, _) = await GetOneLakeTableConfigAsync(
                workspaceId:  "e2829b34-139b-46f3-95da-e6bff2b26ef5",
                lakehouseId:  "9a862620-57e6-4b2a-b942-00b16f44d35b",
                tableName:    "alltypestable");

            await ReadTable_ReturnsData_Core(config, abfssPath);
        }

        // ================================================================== //
        //  Get schema
        // ================================================================== //

        [TestMethod]
        public async Task OneLake_V1_GetSchema_ReturnsColumns()
        {
            var (config, abfssPath, _) = await GetOneLakeTableConfigAsync(
                workspaceId:  "e2829b34-139b-46f3-95da-e6bff2b26ef5",
                lakehouseId:  "9a862620-57e6-4b2a-b942-00b16f44d35b",
                tableName:    "alltypestable");

            await GetSchema_ReturnsColumns_Core(config, abfssPath);
        }

        // ================================================================== //
        //  Execute SQL query
        // ================================================================== //

        [TestMethod]
        public async Task OneLake_V1_ExecuteQuery_SelectAll_ReturnsRows()
        {
            var (config, abfssPath, tableName) = await GetOneLakeTableConfigAsync(
                workspaceId:  "e2829b34-139b-46f3-95da-e6bff2b26ef5",
                lakehouseId:  "9a862620-57e6-4b2a-b942-00b16f44d35b",
                tableName:    "alltypestable");

            await ExecuteQuery_SelectAll_ReturnsRows_Core(config, abfssPath, tableName);
        }

        // ================================================================== //
        //  Write path: Create empty table
        // ================================================================== //

        [TestMethod]
        public async Task OneLake_V1_CreateTable_GetSchema_ReturnsCorrectColumns()
        {
            var (config, abfssPath, _) = await GetWriteTableConfigAsync();

            await CreateTable_GetSchema_ReturnsCorrectColumns_Core(config, abfssPath);
        }

        // ================================================================== //
        //  Write path: Insert + Read round-trip
        // ================================================================== //

        [TestMethod]
        public async Task OneLake_V1_InsertAndRead_DataRoundTrips()
        {
            var (config, abfssPath, _) = await GetWriteTableConfigAsync();

            await InsertAndRead_DataRoundTrips_Core(config, abfssPath);
        }

        // ================================================================== //
        //  Write path: Insert Append
        // ================================================================== //

        [TestMethod]
        public async Task OneLake_V1_InsertAppend_AddsRows()
        {
            var (config, abfssPath, _) = await GetWriteTableConfigAsync();

            await InsertAppend_AddsRows_Core(config, abfssPath);
        }

        // ================================================================== //
        //  Write path: Delete
        // ================================================================== //

        [TestMethod]
        public async Task OneLake_V1_DeleteRows_RemovesMatchingRows()
        {
            var (config, abfssPath, tableName) = await GetWriteTableConfigAsync();

            await DeleteRows_RemovesMatchingRows_Core(config, abfssPath, tableName);
        }

        // ================================================================== //
        //  Write path: Update
        // ================================================================== //

        [TestMethod]
        public async Task OneLake_V1_UpdateRows_ModifiesMatchingRows()
        {
            var (config, abfssPath, tableName) = await GetWriteTableConfigAsync();

            await UpdateRows_ModifiesMatchingRows_Core(config, abfssPath, tableName);
        }

        // ================================================================== //
        //  Write path: MergeData (streaming upsert)
        // ================================================================== //

        [TestMethod]
        public async Task OneLake_V1_MergeData_UpsertAll_UpdatesAndInserts()
        {
            var (config, abfssPath, _) = await GetWriteTableConfigAsync();

            await MergeData_UpsertAll_UpdatesAndInserts_Core(config, abfssPath);
        }

        // ================================================================== //
        //  Write path: CreateTableWithConfig
        // ================================================================== //

        [TestMethod]
        public async Task OneLake_V1_CreateTableWithConfig_RespectsConfiguration()
        {
            var (config, abfssPath, _) = await GetWriteTableConfigAsync();

            await CreateTableWithConfig_RespectsConfiguration_Core(config, abfssPath);
        }

        // ================================================================== //
        //  Write path: CreateTable with Change Data Feed
        // ================================================================== //

        [TestMethod]
        public async Task OneLake_V1_CreateTableWithChangeDataFeed_InsertAndReadBack_Succeeds()
        {
            var (config, abfssPath, _) = await GetWriteTableConfigAsync();

            await CreateTableWithChangeDataFeed_InsertAndReadBack_Core(config, abfssPath);
        }
    }
}
