using System.Threading.Tasks;
using DeltaLakeSharp.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DeltaLakeSharp.Tests
{
    /// <summary>
    /// OneLake end-to-end integration tests using the V2 (DataFusion + delta-rs) backend.
    /// Reads an existing Delta table from a Fabric Lakehouse via SAS authentication.
    ///
    /// <para>
        /// <b>Known limitation (deltalake 1.4.x):</b> The Rust <c>object_store</c> crate's
        /// <c>use_fabric_endpoint</c> flow is standardized around the production
        /// Fabric endpoint. Since the SAS token must be signed with account name
        /// <c>"onelake"</c>, non-production environments (e.g. MSIT with DNS host
        /// <c>msit-onelake.dfs.fabric.microsoft.com</c>) cannot be reached through
        /// this V2 path.
    /// Data-access tests are therefore marked <see cref="Assert.Inconclusive()"/>.
    /// V1 (Spark/Hadoop) does not have this limitation.
    /// </para>
    ///
    /// <para>
    /// V2 starts faster than V1 (no JVM/Spark), but does NOT support column mapping
    /// or deletion vectors. If the target table uses these features (common for
    /// Spark-created tables in Fabric), these tests may fail — that is expected.
    /// Use V1 tests for full Delta Lake feature coverage.
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
    /// Run with: <c>dotnet vstest &lt;dll&gt; --TestCaseFilter:"TestCategory=OneLake&amp;TestCategory=V2" /Platform:x64</c>
    /// </summary>
    [TestClass]
    [TestCategory("Integration")]
    [TestCategory("OneLake")]
    [TestCategory("V2")]
    public class OneLakeV2IntegrationTests : OneLakeIntegrationTestBase
    {
        [ClassInitialize]
        public static async Task ClassInitialize(TestContext context)
        {
            await InitializeAsync(ServiceMode.V2_DataFusion);
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
        public async Task OneLake_V2_HealthCheck_ReturnsTrue()
        {
            bool healthy = await Client.HealthCheckAsync();
            Assert.IsTrue(healthy, "Expected the V2 server to report healthy.");
        }

        // ================================================================== //
        //  Read table
        // ================================================================== //

        [TestMethod]
        public Task OneLake_V2_ReadTable_ReturnsData()
        {
            Assert.Inconclusive(
                "V2 (deltalake 1.4.x / object_store) cannot access non-production OneLake endpoints. " +
                "The standardized use_fabric_endpoint flow routes to production (onelake.dfs.fabric.microsoft.com). " +
                "Use V1 for MSIT OneLake testing.");
            return Task.CompletedTask;
        }

        // ================================================================== //
        //  Get schema
        // ================================================================== //

        [TestMethod]
        public Task OneLake_V2_GetSchema_ReturnsColumns()
        {
            Assert.Inconclusive(
                "V2 (deltalake 1.4.x / object_store) cannot access non-production OneLake endpoints. " +
                "The standardized use_fabric_endpoint flow routes to production (onelake.dfs.fabric.microsoft.com). " +
                "Use V1 for MSIT OneLake testing.");
            return Task.CompletedTask;
        }

        // ================================================================== //
        //  Execute SQL query
        // ================================================================== //

        [TestMethod]
        public Task OneLake_V2_ExecuteQuery_SelectAll_ReturnsRows()
        {
            Assert.Inconclusive(
                "V2 (deltalake 1.4.x / object_store) cannot access non-production OneLake endpoints. " +
                "The standardized use_fabric_endpoint flow routes to production (onelake.dfs.fabric.microsoft.com). " +
                "Use V1 for MSIT OneLake testing.");
            return Task.CompletedTask;
        }

        // ================================================================== //
        //  Write path: Create empty table
        // ================================================================== //

        [TestMethod]
        public Task OneLake_V2_CreateTable_GetSchema_ReturnsCorrectColumns()
        {
            Assert.Inconclusive(
                "V2 (deltalake 1.4.x / object_store) cannot access non-production OneLake endpoints. " +
                "Use V1 for MSIT OneLake write-path testing.");
            return Task.CompletedTask;
        }

        // ================================================================== //
        //  Write path: Insert + Read round-trip
        // ================================================================== //

        [TestMethod]
        public Task OneLake_V2_InsertAndRead_DataRoundTrips()
        {
            Assert.Inconclusive(
                "V2 (deltalake 1.4.x / object_store) cannot access non-production OneLake endpoints. " +
                "Use V1 for MSIT OneLake write-path testing.");
            return Task.CompletedTask;
        }

        // ================================================================== //
        //  Write path: Insert Append
        // ================================================================== //

        [TestMethod]
        public Task OneLake_V2_InsertAppend_AddsRows()
        {
            Assert.Inconclusive(
                "V2 (deltalake 1.4.x / object_store) cannot access non-production OneLake endpoints. " +
                "Use V1 for MSIT OneLake write-path testing.");
            return Task.CompletedTask;
        }

        // ================================================================== //
        //  Write path: Delete
        // ================================================================== //

        [TestMethod]
        public Task OneLake_V2_DeleteRows_RemovesMatchingRows()
        {
            Assert.Inconclusive(
                "V2 (deltalake 1.4.x / object_store) cannot access non-production OneLake endpoints. " +
                "Use V1 for MSIT OneLake write-path testing.");
            return Task.CompletedTask;
        }

        // ================================================================== //
        //  Write path: Update
        // ================================================================== //

        [TestMethod]
        public Task OneLake_V2_UpdateRows_ModifiesMatchingRows()
        {
            Assert.Inconclusive(
                "V2 (deltalake 1.4.x / object_store) cannot access non-production OneLake endpoints. " +
                "Use V1 for MSIT OneLake write-path testing.");
            return Task.CompletedTask;
        }

        // ================================================================== //
        //  Write path: MergeData (streaming upsert)
        // ================================================================== //

        [TestMethod]
        public Task OneLake_V2_MergeData_UpsertAll_UpdatesAndInserts()
        {
            Assert.Inconclusive(
                "V2 (deltalake 1.4.x / object_store) cannot access non-production OneLake endpoints. " +
                "Use V1 for MSIT OneLake write-path testing.");
            return Task.CompletedTask;
        }

        // ================================================================== //
        //  Write path: CreateTableWithConfig
        // ================================================================== //

        [TestMethod]
        public Task OneLake_V2_CreateTableWithConfig_RespectsConfiguration()
        {
            Assert.Inconclusive(
                "V2 (deltalake 1.4.x / object_store) cannot access non-production OneLake endpoints. " +
                "Use V1 for MSIT OneLake write-path testing.");
            return Task.CompletedTask;
        }
    }
}
