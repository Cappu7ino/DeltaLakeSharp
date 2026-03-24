using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Azure.Core;
using Azure.Identity;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.SqlClient;
using Microsoft.DI.DeltaTableService.Adbc;

namespace DeltaTableService.Benchmark
{
    [MemoryDiagnoser]
    public class OneLakeReadPerformanceBenchmark
    {
        private const string StorageTokenScope = "https://storage.azure.com/.default";
        private const string SqlTokenScope = "https://database.windows.net/.default";
        private const string TenantId = "cc535e2f-28ed-426b-aa6f-61426c01db40";
        private const string ClientId = "10850787-f97a-40f3-845f-14098f63c532";
        private const string AccountUpn = "AdminUser01@SoutheastAsia01092026.onmicrosoft.com";
        private const string CertificateName = "bami-tenant-adminuser-adminuser01-southeastasia01092026-20260322";
        private const string CertificateThumbprint = "BD22A334717B5609FFDE8A5A2D4679325BB6043B";

        private const string OneLakeTableUri = "abfss://XingTestWorkspace@onelake.dfs.fabric.microsoft.com/XingLakehouse.Lakehouse/Tables/delta_bench_read_1m";
        private const string SqlEndpointServer = "f5pfhthnfbvufktpmfbgyao3ia-qebpamf64pqebpeph7huhzgidy.datawarehouse.fabric.microsoft.com";
        private const string SqlEndpointDatabase = "XingLakehouse";
        private const string SqlBenchmarkTableName = "delta_bench_read_1m";
        private const string ProjectionSql = "SELECT id, event_ts, amount, region FROM delta_table";
        private const string SqlProjectionQuery = "SELECT [id], [event_ts], [amount], [region] FROM [delta_bench_read_1m]";
        private const string FilteredSql = "SELECT id, tenant_id, amount, region FROM delta_table WHERE tenant_id = 42 AND is_active = true";
        private const string SqlFilteredQuery = "SELECT [id], [tenant_id], [amount], [region] FROM [delta_bench_read_1m] WHERE [tenant_id] = 42 AND [is_active] = 1";
        private const int ExpectedRowCount = 1_000_000;
        private const int SqlCommandTimeoutSeconds = 600;

        private ClientCertificateCredential _credential = null!;
        private DeltaAdbcDriver _driver = null!;
        private AdbcDatabase _database = null!;
        private AdbcConnection _connection = null!;
        private string _sqlConnectionString = null!;
        private long _sqlQueryNonce;

        [Params(327680)]
        public int ConfiguredBatchSize { get; set; }

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            Logger.Info("Initializing OneLake benchmark resources...");

            Logger.Info(ConfiguredBatchSize > 0
                ? $"Using delta.batch_size={ConfiguredBatchSize} for ADBC reads."
                : "Using default DataFusion batch size for ADBC reads.");

            X509Certificate2 certificate = FindCertificate();
            _credential = new ClientCertificateCredential(TenantId, ClientId, certificate);
            _driver = new DeltaAdbcDriver();

            string storageToken = await AcquireAccessTokenAsync(StorageTokenScope, CancellationToken.None).ConfigureAwait(false);
            var adbcOptions = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["delta.table_uri"] = OneLakeTableUri,
                ["delta.storage.option.account_name"] = "onelake",
                ["delta.storage.option.bearer_token"] = storageToken,
                ["delta.storage.option.use_fabric_endpoint"] = "true",
            };

            if (ConfiguredBatchSize > 0)
            {
                adbcOptions["delta.batch_size"] = ConfiguredBatchSize.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }

            _database = _driver.Open(adbcOptions);

            _connection = _database.Connect(null);

            _sqlConnectionString = new SqlConnectionStringBuilder
            {
                DataSource = SqlEndpointServer,
                InitialCatalog = SqlEndpointDatabase,
                Encrypt = true,
                TrustServerCertificate = false,
                ConnectTimeout = SqlCommandTimeoutSeconds,
            }.ConnectionString;

            await ValidateDataSourcesAsync().ConfigureAwait(false);

            Logger.Info("Warming up ADBC full read...");
            await Adbc_FullTableRead().ConfigureAwait(false);

            Logger.Info("Warming up ADBC projected read...");
            await Adbc_ProjectedColumnsRead().ConfigureAwait(false);

            Logger.Info("Warming up ADBC filtered read...");
            await Adbc_FilteredRead().ConfigureAwait(false);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _connection?.Dispose();
            _database?.Dispose();
            _driver?.Dispose();
        }

        [Benchmark(Baseline = true, Description = "OneLake Delta ADBC full table read")]
        public async Task<ReadIterationResult> Adbc_FullTableRead()
        {
            using AdbcStatement statement = _connection.CreateStatement();
            QueryResult result = statement.ExecuteQuery();
            using var stream = result.Stream ?? throw new InvalidOperationException("ADBC full-table read returned a null stream.");
            return await ConsumeArrowStreamAsync(stream).ConfigureAwait(false);
        }

        [Benchmark(Description = "Fabric SQL analytics endpoint full table read")]
        public async Task<ReadIterationResult> SqlAnalyticsEndpoint_FullTableRead()
        {
            return await ExecuteSqlStreamingQueryAsync(BuildUncachedSqlQuery(
                    $"SELECT * FROM [{SqlBenchmarkTableName}]",
                    "full"))
                .ConfigureAwait(false);
        }

        [Benchmark(Description = "OneLake Delta ADBC projected columns read")]
        public async Task<ReadIterationResult> Adbc_ProjectedColumnsRead()
        {
            using AdbcStatement statement = _connection.CreateStatement();
            statement.SqlQuery = ProjectionSql;
            QueryResult result = statement.ExecuteQuery();
            using var stream = result.Stream ?? throw new InvalidOperationException("ADBC projected read returned a null stream.");
            return await ConsumeArrowStreamAsync(stream).ConfigureAwait(false);
        }

        [Benchmark(Description = "Fabric SQL analytics endpoint projected columns read")]
        public async Task<ReadIterationResult> SqlAnalyticsEndpoint_ProjectedColumnsRead()
        {
            return await ExecuteSqlStreamingQueryAsync(BuildUncachedSqlQuery(SqlProjectionQuery, "projection"))
                .ConfigureAwait(false);
        }

        [Benchmark(Description = "OneLake Delta ADBC filtered read")]
        public async Task<ReadIterationResult> Adbc_FilteredRead()
        {
            using AdbcStatement statement = _connection.CreateStatement();
            statement.SqlQuery = FilteredSql;
            QueryResult result = statement.ExecuteQuery();
            using var stream = result.Stream ?? throw new InvalidOperationException("ADBC filtered read returned a null stream.");
            return await ConsumeArrowStreamAsync(stream).ConfigureAwait(false);
        }

        [Benchmark(Description = "Fabric SQL analytics endpoint filtered read")]
        public async Task<ReadIterationResult> SqlAnalyticsEndpoint_FilteredRead()
        {
            return await ExecuteSqlStreamingQueryAsync(BuildUncachedSqlQuery(SqlFilteredQuery, "filtered"))
                .ConfigureAwait(false);
        }

        private async Task ValidateDataSourcesAsync()
        {
            Logger.Info("Validating benchmark table visibility through ADBC...");
            ReadIterationResult adbcValidation = await ExecuteAdbcValidationReadAsync().ConfigureAwait(false);
            if (adbcValidation.RowCount != ExpectedRowCount)
            {
                throw new InvalidOperationException(
                    $"ADBC validation expected {ExpectedRowCount:N0} rows in '{OneLakeTableUri}' but observed {adbcValidation.RowCount:N0}. " +
                    "Run the one-off Spark prep script before executing the benchmark.");
            }

            Logger.Info("Validating benchmark table visibility through SQL analytics endpoint...");
            long sqlCount = await ExecuteSqlCountAsync().ConfigureAwait(false);
            if (sqlCount != ExpectedRowCount)
            {
                throw new InvalidOperationException(
                    $"SQL endpoint validation expected {ExpectedRowCount:N0} rows in '{SqlBenchmarkTableName}' but observed {sqlCount:N0}. " +
                    "Run the one-off Spark prep script and wait for the SQL analytics endpoint to reflect the table before executing the benchmark.");
            }
        }

        private async Task<ReadIterationResult> ExecuteAdbcValidationReadAsync()
        {
            using AdbcStatement statement = _connection.CreateStatement();
            QueryResult result = statement.ExecuteQuery();
            using var stream = result.Stream ?? throw new InvalidOperationException("ADBC validation read returned a null stream.");
            return await ConsumeArrowStreamAsync(stream).ConfigureAwait(false);
        }

        private async Task<long> ExecuteSqlCountAsync()
        {
            string accessToken = await AcquireAccessTokenAsync(SqlTokenScope, CancellationToken.None).ConfigureAwait(false);
            using var connection = new SqlConnection(_sqlConnectionString)
            {
                AccessToken = accessToken,
            };

            await connection.OpenAsync().ConfigureAwait(false);

            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT COUNT(*) FROM [{SqlBenchmarkTableName}]";
            command.CommandTimeout = SqlCommandTimeoutSeconds;

            object? result = await command.ExecuteScalarAsync().ConfigureAwait(false);
            if (result == null || result == DBNull.Value)
            {
                throw new InvalidOperationException("SQL analytics endpoint returned a null row count.");
            }

            return Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
        }

        private async Task<ReadIterationResult> ExecuteSqlStreamingQueryAsync(string sql)
        {
            string accessToken = await AcquireAccessTokenAsync(SqlTokenScope, CancellationToken.None).ConfigureAwait(false);
            using var connection = new SqlConnection(_sqlConnectionString)
            {
                AccessToken = accessToken,
            };

            await connection.OpenAsync().ConfigureAwait(false);

            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.CommandTimeout = SqlCommandTimeoutSeconds;

            // Stream rows directly from the TDS reader so the benchmark measures
            // endpoint read/transport cost without DataTable materialization.
            using var reader = await command.ExecuteReaderAsync(System.Data.CommandBehavior.SequentialAccess).ConfigureAwait(false);

            long rowCount = 0;
            int fieldCount = reader.FieldCount;
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                rowCount++;
            }

            return new ReadIterationResult(rowCount, fieldCount);
        }

        private string BuildUncachedSqlQuery(string baseSql, string benchmarkTag)
        {
            long nonce = Interlocked.Increment(ref _sqlQueryNonce);
            return $"SELECT * FROM ({baseSql}) AS benchmark_source WHERE {nonce} = {nonce} OPTION (USE HINT ('DISABLE_RESULT_SET_CACHE')) /* {benchmarkTag}:{nonce} */";
        }

        private async Task<string> AcquireAccessTokenAsync(string scope, CancellationToken cancellationToken)
        {
            AccessToken token = await _credential
                .GetTokenAsync(new TokenRequestContext(new[] { scope }), cancellationToken)
                .ConfigureAwait(false);
            return token.Token;
        }

        private static async Task<ReadIterationResult> ConsumeArrowStreamAsync(Apache.Arrow.Ipc.IArrowArrayStream stream)
        {
            long rowCount = 0;
            long batchCount = 0;

            while (true)
            {
                RecordBatch? batch = await stream.ReadNextRecordBatchAsync().AsTask().ConfigureAwait(false);
                if (batch == null)
                {
                    break;
                }

                batchCount++;
                rowCount += batch.Length;
            }

            return new ReadIterationResult(rowCount, batchCount);
        }

        private static X509Certificate2 FindCertificate()
        {
            X509Certificate2? certificate = FindCertificate(StoreLocation.CurrentUser) ?? FindCertificate(StoreLocation.LocalMachine);
            if (certificate == null)
            {
                throw new InvalidOperationException(
                    $"Could not find installed certificate '{CertificateName}' for account '{AccountUpn}'. " +
                    "Install the PFX with private key into CurrentUser\\My or LocalMachine\\My before running the benchmark.");
            }

            return certificate;
        }

        private static X509Certificate2? FindCertificate(StoreLocation location)
        {
            using var store = new X509Store(StoreName.My, location);
            store.Open(OpenFlags.ReadOnly);

            return store.Certificates
                .Cast<X509Certificate2>()
                .Where(cert => cert.HasPrivateKey)
                .Where(cert => cert.NotAfter > DateTime.Now)
                .FirstOrDefault(cert =>
                    MatchesThumbprint(cert, CertificateThumbprint) ||
                    ContainsIgnoreCase(cert.FriendlyName, CertificateName) ||
                    ContainsIgnoreCase(cert.Subject, AccountUpn) ||
                    ContainsIgnoreCase(cert.Subject, "AdminUser01") ||
                    ContainsIgnoreCase(cert.Subject, "SoutheastAsia01092026"));
        }

        private static bool ContainsIgnoreCase(string? value, string search)
        {
            return value?.IndexOf(search, StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private static bool MatchesThumbprint(X509Certificate2 certificate, string thumbprint)
        {
            return string.Equals(
                certificate.Thumbprint?.Replace(" ", string.Empty),
                thumbprint.Replace(" ", string.Empty),
                StringComparison.OrdinalIgnoreCase);
        }

        public readonly struct ReadIterationResult
        {
            public ReadIterationResult(long rowCount, long unitCount)
            {
                RowCount = rowCount;
                UnitCount = unitCount;
            }

            public long RowCount { get; }

            public long UnitCount { get; }
        }
    }
}
