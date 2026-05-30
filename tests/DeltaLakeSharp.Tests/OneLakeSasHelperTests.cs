using System;
using System.Xml;
using System.Xml.Serialization;
using DeltaLakeSharp.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DeltaLakeSharp.Tests
{
    /// <summary>
    /// Unit tests for <see cref="OneLakeSasHelper"/> and <see cref="OneLakeEnvironment"/>.
    /// </summary>
    [TestClass]
    public class OneLakeSasHelperTests
    {
        // ================================================================== //
        //  Environment resolution
        // ================================================================== //

        [TestMethod]
        public void ResolveEnvironment_Production_ReturnsCorrectValues()
        {
            var (endpoint, dnsAccount, sasAccount) = OneLakeSasHelper.ResolveEnvironment(OneLakeEnvironment.Production);
            Assert.AreEqual(new Uri("https://onelake.dfs.fabric.microsoft.com"), endpoint);
            Assert.AreEqual("onelake", dnsAccount);
            Assert.AreEqual("onelake", sasAccount, "SAS signing account must always be 'onelake'.");
        }

        [TestMethod]
        public void ResolveEnvironment_Msit_ReturnsCorrectValues()
        {
            var (endpoint, dnsAccount, sasAccount) = OneLakeSasHelper.ResolveEnvironment(OneLakeEnvironment.Msit);
            Assert.AreEqual(new Uri("https://msit-onelake.dfs.fabric.microsoft.com"), endpoint);
            Assert.AreEqual("msit-onelake", dnsAccount);
            Assert.AreEqual("onelake", sasAccount, "SAS signing account must always be 'onelake', even for MSIT.");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void ResolveEnvironment_InvalidValue_Throws()
        {
            OneLakeSasHelper.ResolveEnvironment((OneLakeEnvironment)999);
        }

        // ================================================================== //
        //  UDK request XML
        // ================================================================== //

        [TestMethod]
        public void BuildUdkRequestXml_ContainsKeyInfoElement()
        {
            var start = new DateTimeOffset(2025, 1, 15, 10, 0, 0, TimeSpan.Zero);
            var end = new DateTimeOffset(2025, 1, 15, 11, 0, 0, TimeSpan.Zero);

            var xml = OneLakeSasHelper.BuildUdkRequestXml(start, end);

            Assert.IsTrue(xml.Contains("<KeyInfo>"), "XML should contain <KeyInfo> root element.");
            Assert.IsTrue(xml.Contains("<Start>2025-01-15T10:00:00Z</Start>"), "XML should contain the formatted start time.");
            Assert.IsTrue(xml.Contains("<Expiry>2025-01-15T11:00:00Z</Expiry>"), "XML should contain the formatted expiry time.");
        }

        [TestMethod]
        public void BuildUdkRequestXml_IsValidXml()
        {
            var start = DateTimeOffset.UtcNow;
            var end = start.AddHours(1);

            var xml = OneLakeSasHelper.BuildUdkRequestXml(start, end);

            // Parsing should not throw.
            var doc = new XmlDocument();
            doc.LoadXml(xml);

            Assert.AreEqual("KeyInfo", doc.DocumentElement?.Name);
        }

        // ================================================================== //
        //  UdkResponse deserialization
        // ================================================================== //

        [TestMethod]
        public void UdkResponse_Deserializes_AllFields()
        {
            const string xml = @"<?xml version=""1.0"" encoding=""utf-8""?>
<UserDelegationKey>
  <SignedOid>oid-value</SignedOid>
  <SignedTid>tid-value</SignedTid>
  <SignedStart>2025-01-15T10:00:00Z</SignedStart>
  <SignedExpiry>2025-01-15T11:00:00Z</SignedExpiry>
  <SignedService>b</SignedService>
  <SignedVersion>2022-11-02</SignedVersion>
  <Value>base64key==</Value>
</UserDelegationKey>";

            var serializer = new XmlSerializer(typeof(OneLakeSasHelper.UdkResponse));
            using var reader = new System.IO.StringReader(xml);
            var udk = (OneLakeSasHelper.UdkResponse)serializer.Deserialize(reader)!;

            Assert.AreEqual("oid-value", udk.SignedOid);
            Assert.AreEqual("tid-value", udk.SignedTid);
            Assert.AreEqual("2025-01-15T10:00:00Z", udk.SignedStart);
            Assert.AreEqual("2025-01-15T11:00:00Z", udk.SignedExpiry);
            Assert.AreEqual("b", udk.SignedService);
            Assert.AreEqual("2022-11-02", udk.SignedVersion);
            Assert.AreEqual("base64key==", udk.Value);
        }

        // ================================================================== //
        //  Parameter validation
        // ================================================================== //

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetStorageConfigAsync_NullWorkspaceId_Throws()
        {
            // The method should throw synchronously before any async work.
            OneLakeSasHelper.GetStorageConfigAsync(null!, "some/path").GetAwaiter().GetResult();
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetStorageConfigAsync_EmptyWorkspaceId_Throws()
        {
            OneLakeSasHelper.GetStorageConfigAsync("", "some/path").GetAwaiter().GetResult();
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetStorageConfigAsync_NullPath_Throws()
        {
            OneLakeSasHelper.GetStorageConfigAsync("workspace-id", null!).GetAwaiter().GetResult();
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void GetStorageConfigAsync_EmptyPath_Throws()
        {
            OneLakeSasHelper.GetStorageConfigAsync("workspace-id", "").GetAwaiter().GetResult();
        }
    }
}
