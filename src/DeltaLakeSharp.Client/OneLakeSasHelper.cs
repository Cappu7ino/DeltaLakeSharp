// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;
using Azure.Core;
using Azure.Identity;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Azure.Storage.Sas;
using DeltaLakeSharp.Client.Models;

namespace DeltaLakeSharp.Client
{
    /// <summary>
    /// Lightweight helper that acquires a OneLake user-delegation SAS token
    /// and returns a <see cref="StorageConfig"/> ready for use with
    /// <see cref="DeltaTableServiceClient"/> API methods.
    /// <para>
    /// The implementation mirrors the proven pattern in
    /// <c>TridentLakeClient.GetOneLakeSasToken()</c> but is fully
    /// self-contained — it uses <see cref="HttpClient"/> for the UDK POST
    /// and the Azure Storage SDK for SAS generation, avoiding any dependency
    /// on the heavy TridentLakeClient service stack.
    /// </para>
    /// </summary>
    public static class OneLakeSasHelper
    {
        /// <summary>
        /// The Azure Storage token scope used to obtain a Bearer token.
        /// </summary>
        private const string StorageTokenScope = "https://storage.azure.com/.default";

        /// <summary>
        /// The Azure Storage service version sent in the <c>x-ms-version</c> header.
        /// </summary>
        private const string StorageApiVersion = "2022-11-02";

        /// <summary>
        /// Clock-skew offset (minutes) applied to the SAS start time to
        /// account for minor time differences between client and server.
        /// </summary>
        private const int ClockSkewMinutes = -3;

        /// <summary>
        /// Default SAS token lifetime in hours.
        /// </summary>
        private const int SasLifetimeHours = 1;

        /// <summary>
        /// Shared <see cref="HttpClient"/> instance for UDK requests.
        /// </summary>
        private static readonly HttpClient SharedHttpClient = new HttpClient();

        /// <summary>
        /// Acquires a OneLake user-delegation SAS token and returns a
        /// <see cref="StorageConfig"/> that can be passed to any
        /// <see cref="DeltaTableServiceClient"/> method.
        /// </summary>
        /// <param name="workspaceId">The Fabric workspace ID (GUID).</param>
        /// <param name="path">
        /// The artifact path inside the workspace, e.g.
        /// <c>"&lt;lakehouseId&gt;/Tables/myTable"</c>.
        /// </param>
        /// <param name="environment">
        /// The OneLake environment to target. Defaults to
        /// <see cref="OneLakeEnvironment.Production"/>.
        /// </param>
        /// <param name="credential">
        /// An optional <see cref="TokenCredential"/>. When <c>null</c>,
        /// <see cref="DefaultAzureCredential"/> is used.
        /// </param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>
        /// A <see cref="StorageConfig"/> containing the OneLake storage
        /// account name and a freshly minted SAS token.
        /// </returns>
        public static async Task<StorageConfig> GetStorageConfigAsync(
            string workspaceId,
            string path,
            OneLakeEnvironment environment = OneLakeEnvironment.Production,
            TokenCredential? credential = null,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(workspaceId))
            {
                throw new ArgumentException("Workspace ID must not be null or empty.", nameof(workspaceId));
            }

            if (string.IsNullOrWhiteSpace(path))
            {
                throw new ArgumentException("Path must not be null or empty.", nameof(path));
            }

            var (dfsEndpoint, dnsAccountName, sasAccountName) = ResolveEnvironment(environment);
            credential ??= new DefaultAzureCredential();

            // 1. Obtain a Bearer token for Azure Storage.
            var tokenRequestContext = new TokenRequestContext(new[] { StorageTokenScope });
            var accessToken = await credential.GetTokenAsync(tokenRequestContext, cancellationToken).ConfigureAwait(false);

            // 2. Acquire a user delegation key from the OneLake DFS endpoint.
            var start = DateTimeOffset.UtcNow.AddMinutes(ClockSkewMinutes);
            var end = start.AddHours(SasLifetimeHours);
            var udk = await GetUserDelegationKeyAsync(dfsEndpoint, accessToken.Token, start, end, cancellationToken).ConfigureAwait(false);

            // 3. Build the SAS token using the delegation key.
            //    The signing account is always "onelake" (canonicalized resource
            //    is /blob/onelake/{workspace}/{path}) per Microsoft docs, while
            //    the StorageConfig uses the DNS account name for ABFSS URIs.
            var sasBuilder = new DataLakeSasBuilder
            {
                FileSystemName = workspaceId,
                Path = path,
                IsDirectory = true,
                Resource = "d",
                StartsOn = start,
                ExpiresOn = end
            };
            sasBuilder.SetPermissions(DataLakeSasPermissions.All);

            var sasToken = sasBuilder.ToSasQueryParameters(udk, sasAccountName).ToString();
            return new StorageConfig(dnsAccountName, sasToken);
        }

        /// <summary>
        /// Resolves the OneLake DFS endpoint URI and account names for the
        /// given <paramref name="environment"/>.
        /// </summary>
        /// <returns>
        /// A tuple with three values:
        /// <list type="bullet">
        ///   <item><description>
        ///     <c>DfsEndpoint</c> — the DFS base URI (e.g.
        ///     <c>https://msit-onelake.dfs.fabric.microsoft.com</c>).
        ///   </description></item>
        ///   <item><description>
        ///     <c>DnsAccountName</c> — the environment-specific account name
        ///     used in ABFSS URIs and <see cref="StorageConfig"/>
        ///     (e.g. <c>"msit-onelake"</c> for MSIT, <c>"onelake"</c> for
        ///     Production).
        ///   </description></item>
        ///   <item><description>
        ///     <c>SasAccountName</c> — the account name used when signing SAS
        ///     tokens. Per Microsoft documentation this is <b>always</b>
        ///     <c>"onelake"</c> regardless of environment, because the
        ///     canonicalized resource is
        ///     <c>/blob/onelake/{workspace}/{path}</c>.
        ///   </description></item>
        /// </list>
        /// </returns>
        internal static (Uri DfsEndpoint, string DnsAccountName, string SasAccountName) ResolveEnvironment(OneLakeEnvironment environment)
        {
            // The SAS signing account is always "onelake" per:
            // https://learn.microsoft.com/fabric/onelake/how-to-create-a-onelake-shared-access-signature
            const string sasAccountName = "onelake";

            return environment switch
            {
                OneLakeEnvironment.Production => (new Uri("https://onelake.dfs.fabric.microsoft.com"), "onelake", sasAccountName),
                OneLakeEnvironment.Msit => (new Uri("https://msit-onelake.dfs.fabric.microsoft.com"), "msit-onelake", sasAccountName),
                _ => throw new ArgumentOutOfRangeException(nameof(environment), environment, "Unknown OneLake environment.")
            };
        }

        /// <summary>
        /// Posts a user-delegation-key request to the OneLake DFS endpoint
        /// and returns an Azure SDK <see cref="UserDelegationKey"/>.
        /// </summary>
        private static async Task<UserDelegationKey> GetUserDelegationKeyAsync(
            Uri dfsEndpoint,
            string bearerToken,
            DateTimeOffset start,
            DateTimeOffset end,
            CancellationToken cancellationToken)
        {
            // Build the XML request body.
            var xmlBody = BuildUdkRequestXml(start, end);

            var requestUri = new Uri(dfsEndpoint, "/?restype=service&comp=userdelegationkey");
            using var request = new HttpRequestMessage(HttpMethod.Post, requestUri);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
            request.Headers.Add("x-ms-version", StorageApiVersion);
            request.Content = new StringContent(xmlBody, Encoding.UTF8, "application/xml");

            using var response = await SharedHttpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                string responseBody = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                string sanitizedBody = SanitizeForDiagnostics(responseBody);
                string diagnostics = $"OneLake user delegation key request failed. " +
                    $"Status={(int)response.StatusCode} ({response.ReasonPhrase}), " +
                    $"Endpoint={requestUri}, Body={sanitizedBody}";

                throw new HttpRequestException(diagnostics);
            }

            using var responseStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
            var udkResponse = DeserializeUdkResponse(responseStream);

            return DataLakeModelFactory.UserDelegationKey(
                udkResponse.SignedOid,
                udkResponse.SignedTid,
                ParseDateTimeOffset(udkResponse.SignedStart),
                ParseDateTimeOffset(udkResponse.SignedExpiry),
                udkResponse.SignedService,
                udkResponse.SignedVersion,
                udkResponse.Value);
        }

        private static string SanitizeForDiagnostics(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return "<empty>";
            }

            string trimmed = value.Trim();
            return trimmed.Length <= 2000 ? trimmed : trimmed.Substring(0, 2000);
        }

        /// <summary>
        /// Builds the XML body for the user delegation key request.
        /// <code>
        /// &lt;KeyInfo&gt;
        ///   &lt;Start&gt;2024-01-01T00:00:00Z&lt;/Start&gt;
        ///   &lt;Expiry&gt;2024-01-01T01:00:00Z&lt;/Expiry&gt;
        /// &lt;/KeyInfo&gt;
        /// </code>
        /// </summary>
        internal static string BuildUdkRequestXml(DateTimeOffset start, DateTimeOffset end)
        {
            var sb = new StringBuilder();
            using (var writer = XmlWriter.Create(sb, new XmlWriterSettings { Indent = true, Encoding = Encoding.UTF8 }))
            {
                writer.WriteStartDocument();
                writer.WriteStartElement("KeyInfo");
                writer.WriteElementString("Start", start.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture));
                writer.WriteElementString("Expiry", end.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture));
                writer.WriteEndElement();
                writer.WriteEndDocument();
            }

            return sb.ToString();
        }

        /// <summary>
        /// Deserializes the UDK XML response from the storage service.
        /// </summary>
        private static UdkResponse DeserializeUdkResponse(Stream responseStream)
        {
            var serializer = new XmlSerializer(typeof(UdkResponse));
            object? deserialized = serializer.Deserialize(responseStream);
            return deserialized as UdkResponse
                   ?? throw new InvalidOperationException("Failed to deserialize user delegation key response.");
        }

        /// <summary>
        /// Parses a date-time string, returning <see cref="DateTimeOffset.MinValue"/>
        /// when the value is null or whitespace.
        /// </summary>
        private static DateTimeOffset ParseDateTimeOffset(string? value)
        {
            return string.IsNullOrWhiteSpace(value)
                ? default
                : DateTimeOffset.Parse(value, null, DateTimeStyles.RoundtripKind);
        }

        /// <summary>
        /// Internal model for deserializing the XML user delegation key response
        /// returned by the OneLake DFS endpoint. Mirrors the schema used by
        /// <c>TridentLakeProxyService.Models.UserDelegationKey</c> but is
        /// self-contained within this assembly.
        /// </summary>
        [XmlRoot(ElementName = "UserDelegationKey")]
        public class UdkResponse
        {
            /// <summary>Gets or sets the signed object ID.</summary>
            [XmlElement(ElementName = "SignedOid")]
            public string? SignedOid { get; set; }

            /// <summary>Gets or sets the signed tenant ID.</summary>
            [XmlElement(ElementName = "SignedTid")]
            public string? SignedTid { get; set; }

            /// <summary>Gets or sets the signed start time.</summary>
            [XmlElement(ElementName = "SignedStart")]
            public string? SignedStart { get; set; }

            /// <summary>Gets or sets the signed expiry time.</summary>
            [XmlElement(ElementName = "SignedExpiry")]
            public string? SignedExpiry { get; set; }

            /// <summary>Gets or sets the signed service abbreviation.</summary>
            [XmlElement(ElementName = "SignedService")]
            public string? SignedService { get; set; }

            /// <summary>Gets or sets the signed storage API version.</summary>
            [XmlElement(ElementName = "SignedVersion")]
            public string? SignedVersion { get; set; }

            /// <summary>Gets or sets the base-64-encoded key value.</summary>
            [XmlElement(ElementName = "Value")]
            public string? Value { get; set; }
        }
    }
}
