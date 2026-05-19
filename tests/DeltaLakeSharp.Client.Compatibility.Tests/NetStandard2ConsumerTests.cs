// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Runtime.Versioning;
using DeltaLakeSharp.Client.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DeltaLakeSharp.Client.Compatibility.Tests
{
    [TestClass]
    public sealed class NetStandard2ConsumerTests
    {
        [TestMethod]
        public void TestHostLoadsNetStandardClientAsset()
        {
            TargetFrameworkAttribute? targetFramework = Attribute.GetCustomAttribute(
                typeof(DeltaTableServiceClient).Assembly,
                typeof(TargetFrameworkAttribute)) as TargetFrameworkAttribute;

            Assert.IsNotNull(targetFramework);
            Assert.AreEqual(".NETStandard,Version=v2.0", targetFramework!.FrameworkName);
        }

        [TestMethod]
        public void FlightClientCanBeConstructedFromNetStandardTarget()
        {
            using var client = new DeltaTableServiceClient(new Uri("http://localhost:8815"));

            Assert.AreEqual(ServiceMode.V1_Spark, client.Mode);
        }

        [TestMethod]
        public void PublicModelsCanBeUsedFromNetStandardTarget()
        {
            var storageConfig = new StorageConfig("onelake", "sv=fake", evictFileSystemCache: false);
            GenericStorageOptions genericOptions = GenericStorageOptions.FromStorageConfig(storageConfig);
            var readerOptions = new DeltaDataReaderOptions
            {
                DecimalBehavior = DeltaDataReaderDecimalBehavior.OverflowDecimalAsString,
            };

            Assert.AreEqual("onelake", storageConfig.StorageAccount);
            Assert.IsFalse(storageConfig.EvictFileSystemCache);
            Assert.AreEqual("true", genericOptions.Options["use_fabric_endpoint"]);
            Assert.AreEqual(DeltaDataReaderDecimalBehavior.OverflowDecimalAsString, readerOptions.DecimalBehavior);
            Assert.AreEqual(WriteSchemaMode.Merge, WriteSchemaMode.Merge);
        }

        [TestMethod]
        public void UriLessConstructorStillRequiresNativeMode()
        {
            ArgumentException exception = Assert.ThrowsExactly<ArgumentException>(
                () => new DeltaTableServiceClient(ServiceMode.V1_Spark));

            StringAssert.Contains(exception.Message, "server URI");
        }
    }
}
