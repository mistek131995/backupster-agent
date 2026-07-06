using System.Net;
using System.Text;
using BackupsterAgent.Providers.Secrets;
using Microsoft.Extensions.Logging.Abstractions;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class HashicorpVaultSecretBackendTests
{
    [Test]
    public async Task ReadKvV2Async_SendsVaultHeadersAndParsesDataObject()
    {
        var handler = new RecordingHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = Json("""{"data":{"data":{"password":"from-vault"},"metadata":{"version":2}}}"""),
        });
        var backend = new HashicorpVaultSecretBackend(
            new FakeHttpClientFactory(handler),
            NullLogger<HashicorpVaultSecretBackend>.Instance);

        var value = await backend.ReadKvV2Async(
            new Uri("https://vault.example.net/base"),
            "admin",
            "vault-token",
            "secret/team",
            "backupster/prod/main-pg",
            "2",
            "Connections['pg'].Password",
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo("{\"password\":\"from-vault\"}"));
            Assert.That(handler.Requests, Has.Count.EqualTo(1));
            Assert.That(handler.Requests[0].Method, Is.EqualTo(HttpMethod.Get));
            Assert.That(handler.Requests[0].RequestUri, Is.EqualTo(new Uri("https://vault.example.net/base/v1/secret/team/data/backupster/prod/main-pg?version=2")));
            Assert.That(handler.Requests[0].Headers.GetValues("X-Vault-Token").Single(), Is.EqualTo("vault-token"));
            Assert.That(handler.Requests[0].Headers.GetValues("X-Vault-Namespace").Single(), Is.EqualTo("admin"));
        });
    }

    [Test]
    public async Task LoginAppRoleAsync_PostsCredentialsAndParsesAuth()
    {
        string? requestBody = null;
        var handler = new RecordingHandler(async request =>
        {
            requestBody = request.Content is null
                ? null
                : await request.Content.ReadAsStringAsync();
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = Json("""{"auth":{"client_token":"login-token","lease_duration":3600,"renewable":true}}"""),
            };
        });
        var backend = new HashicorpVaultSecretBackend(
            new FakeHttpClientFactory(handler),
            NullLogger<HashicorpVaultSecretBackend>.Instance);

        var result = await backend.LoginAppRoleAsync(
            new Uri("https://vault.example.net"),
            "admin",
            "auth/custom-approle",
            "role-id",
            "secret-id",
            "Connections['pg'].Password",
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.ClientToken, Is.EqualTo("login-token"));
            Assert.That(result.LeaseDurationSeconds, Is.EqualTo(3600));
            Assert.That(result.Renewable, Is.True);
            Assert.That(handler.Requests, Has.Count.EqualTo(1));
            Assert.That(handler.Requests[0].Method, Is.EqualTo(HttpMethod.Post));
            Assert.That(handler.Requests[0].RequestUri, Is.EqualTo(new Uri("https://vault.example.net/v1/auth/custom-approle/login")));
            Assert.That(handler.Requests[0].Headers.GetValues("X-Vault-Namespace").Single(), Is.EqualTo("admin"));
            Assert.That(requestBody, Does.Contain("\"role_id\":\"role-id\""));
            Assert.That(requestBody, Does.Contain("\"secret_id\":\"secret-id\""));
        });
    }

    private static StringContent Json(string value) =>
        new(value, Encoding.UTF8, "application/json");

    private sealed class FakeHttpClientFactory : IHttpClientFactory
    {
        private readonly HttpMessageHandler _handler;

        public FakeHttpClientFactory(HttpMessageHandler handler)
        {
            _handler = handler;
        }

        public HttpClient CreateClient(string name) =>
            new(_handler, disposeHandler: false);
    }

    private sealed class RecordingHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, Task<HttpResponseMessage>> _handler;

        public RecordingHandler(Func<HttpRequestMessage, HttpResponseMessage> handler)
            : this(request => Task.FromResult(handler(request)))
        {
        }

        public RecordingHandler(Func<HttpRequestMessage, Task<HttpResponseMessage>> handler)
        {
            _handler = handler;
        }

        public List<HttpRequestMessage> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Requests.Add(request);
            return await _handler(request);
        }
    }
}
