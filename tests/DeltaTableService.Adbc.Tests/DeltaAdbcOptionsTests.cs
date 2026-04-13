using System;
using System.Collections.Generic;
using Apache.Arrow.Adbc;
using Microsoft.DI.DeltaTableService.Adbc.Internal;
using Microsoft.DI.DeltaTableService.Client.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.DI.DeltaTableService.Adbc.Tests
{
    [TestClass]
    public class DeltaAdbcOptionsTests
    {
        [TestMethod]
        public void Parse_RequiresTableUri()
        {
            Assert.ThrowsException<ArgumentException>(() => DeltaAdbcConnectOptions.Parse(new Dictionary<string, string>()));
        }

        [TestMethod]
        public void Parse_NormalizesAzureAndGenericStorageOptions()
        {
            DeltaAdbcConnectOptions options = DeltaAdbcConnectOptions.Parse(new Dictionary<string, string>
            {
                [DeltaAdbcConnectOptions.TableUriKey] = "abfss://container@onelake.dfs.fabric.microsoft.com/tables/foo",
                [DeltaAdbcConnectOptions.AzureStorageAccountKey] = "onelake",
                [DeltaAdbcConnectOptions.AzureSasTokenKey] = "?sig=abc",
                [DeltaAdbcConnectOptions.StorageOptionPrefix + "allow_http"] = "true",
            });

            Assert.AreEqual("abfss://container@onelake.dfs.fabric.microsoft.com/tables/foo", options.TableUri);
            GenericStorageOptions? storageOptions = options.StorageOptions;
            Assert.IsNotNull(storageOptions);
            Assert.AreEqual("onelake", storageOptions!.Options["account_name"]);
            Assert.AreEqual("?sig=abc", storageOptions.Options["sas_token"]);
            Assert.AreEqual("true", storageOptions.Options["use_fabric_endpoint"]);
            Assert.AreEqual("true", storageOptions.Options["allow_http"]);
        }

        [TestMethod]
        public void ToParameterDictionary_RoundTripsParsedValues()
        {
            DeltaAdbcConnectOptions options = DeltaAdbcConnectOptions.Parse(new Dictionary<string, string>
            {
                [DeltaAdbcConnectOptions.TableUriKey] = "abfss://container@onelake.dfs.fabric.microsoft.com/tables/foo",
                [DeltaAdbcConnectOptions.StorageOptionPrefix + DeltaAdbcConnectOptions.BearerTokenStorageOptionKey] = "abc",
                [DeltaAdbcConnectOptions.StorageOptionPrefix + "use_fabric_endpoint"] = "true",
            });

            IReadOnlyDictionary<string, string> parameters = options.ToParameterDictionary();

            Assert.AreEqual("abfss://container@onelake.dfs.fabric.microsoft.com/tables/foo", parameters[DeltaAdbcConnectOptions.TableUriKey]);
            Assert.AreEqual("abc", parameters[DeltaAdbcConnectOptions.StorageOptionPrefix + DeltaAdbcConnectOptions.BearerTokenStorageOptionKey]);
            Assert.AreEqual("true", parameters[DeltaAdbcConnectOptions.StorageOptionPrefix + "use_fabric_endpoint"]);
        }

        [TestMethod]
        public void Parse_WithVersion_StoresHistoricalSnapshotVersion()
        {
            DeltaAdbcConnectOptions options = DeltaAdbcConnectOptions.Parse(new Dictionary<string, string>
            {
                [DeltaAdbcConnectOptions.TableUriKey] = "C:/tables/foo",
                [DeltaAdbcStatementOptions.VersionOptionKey] = "7",
            });

            Assert.AreEqual(7L, options.Version);
        }

        [TestMethod]
        public void ToParameterDictionary_RoundTripsVersion()
        {
            DeltaAdbcConnectOptions options = DeltaAdbcConnectOptions.Parse(new Dictionary<string, string>
            {
                [DeltaAdbcConnectOptions.TableUriKey] = "C:/tables/foo",
                [DeltaAdbcStatementOptions.VersionOptionKey] = "3",
            });

            IReadOnlyDictionary<string, string> parameters = options.ToParameterDictionary();

            Assert.AreEqual("3", parameters[DeltaAdbcStatementOptions.VersionOptionKey]);
        }

        [TestMethod]
        public void Parse_WithNegativeVersion_ThrowsArgumentException()
        {
            Assert.ThrowsException<ArgumentException>(() => DeltaAdbcConnectOptions.Parse(new Dictionary<string, string>
            {
                [DeltaAdbcConnectOptions.TableUriKey] = "C:/tables/foo",
                [DeltaAdbcStatementOptions.VersionOptionKey] = "-1",
            }));
        }
    }
}
