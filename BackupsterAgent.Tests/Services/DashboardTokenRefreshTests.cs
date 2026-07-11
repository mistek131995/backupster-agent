using System.Net;
using BackupsterAgent.Configuration;
using BackupsterAgent.Providers.Secrets;
using BackupsterAgent.Services.Common.Secrets;
using BackupsterAgent.Services.Dashboard;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class DashboardTokenRefreshTests
{
    [Test]
    public async Task SuccessfulRequest_DoesNotReadTokenSecretAgain()
    {
        var secretProvider = new MutableSecretProvider("new-token");
        var authGuard = new RecordingAuthGuard();
        var handler = new RecordingHandler(_ => HttpStatusCode.NoContent);
        var client = BuildClient(secretProvider, authGuard);
        var snapshot = new DashboardTokenSnapshot("old-token");

        using var response = await client.SendAsync(
            new HttpClient(handler), snapshot, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.NoContent));
            Assert.That(secretProvider.ReadCalls, Is.Zero);
            Assert.That(authGuard.UnauthorizedCalls, Is.Zero);
            Assert.That(snapshot.Token, Is.EqualTo("old-token"));
            Assert.That(handler.Tokens, Is.EqualTo(new[] { "old-token" }));
        });
    }

    [Test]
    public async Task UnauthorizedWithRotatedSecret_RefreshesSnapshotAndRetriesOnce()
    {
        var secretProvider = new MutableSecretProvider("new-token");
        var authGuard = new RecordingAuthGuard();
        var handler = new RecordingHandler(token =>
            token == "new-token" ? HttpStatusCode.NoContent : HttpStatusCode.Unauthorized);
        var client = BuildClient(secretProvider, authGuard);
        var snapshot = new DashboardTokenSnapshot("old-token");

        using var response = await client.SendAsync(
            new HttpClient(handler), snapshot, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.NoContent));
            Assert.That(secretProvider.ReadCalls, Is.EqualTo(1));
            Assert.That(authGuard.UnauthorizedCalls, Is.Zero);
            Assert.That(snapshot.Token, Is.EqualTo("new-token"));
            Assert.That(handler.Tokens, Is.EqualTo(new[] { "old-token", "new-token" }));
        });
    }

    [Test]
    public void UnauthorizedWithUnchangedSecret_StopsAgentWithoutRetry()
    {
        var secretProvider = new MutableSecretProvider("old-token");
        var authGuard = new RecordingAuthGuard();
        var handler = new RecordingHandler(_ => HttpStatusCode.Unauthorized);
        var client = BuildClient(secretProvider, authGuard);
        var snapshot = new DashboardTokenSnapshot("old-token");

        Assert.ThrowsAsync<DashboardUnauthorizedException>(() =>
            client.SendAsync(new HttpClient(handler), snapshot, CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(secretProvider.ReadCalls, Is.EqualTo(1));
            Assert.That(authGuard.UnauthorizedCalls, Is.EqualTo(1));
            Assert.That(handler.Tokens, Is.EqualTo(new[] { "old-token" }));
        });
    }

    [Test]
    public void RefreshedTokenRejected_StopsAgentAfterOneRetry()
    {
        var secretProvider = new MutableSecretProvider("new-token");
        var authGuard = new RecordingAuthGuard();
        var handler = new RecordingHandler(_ => HttpStatusCode.Unauthorized);
        var client = BuildClient(secretProvider, authGuard);
        var snapshot = new DashboardTokenSnapshot("old-token");

        Assert.ThrowsAsync<DashboardUnauthorizedException>(() =>
            client.SendAsync(new HttpClient(handler), snapshot, CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(secretProvider.ReadCalls, Is.EqualTo(1));
            Assert.That(authGuard.UnauthorizedCalls, Is.EqualTo(1));
            Assert.That(snapshot.Token, Is.EqualTo("new-token"));
            Assert.That(handler.Tokens, Is.EqualTo(new[] { "old-token", "new-token" }));
        });
    }

    private static TestDashboardClient BuildClient(
        ISecretProvider secretProvider,
        IDashboardAuthGuard authGuard) =>
        new(
            new AgentSettings
            {
                DashboardUrl = "https://dashboard.example.test",
                TokenSecret = new SecretRef { Provider = "mutable", Name = "agent-token" },
            },
            authGuard,
            new SecretResolver(new SecretProviderFactory([secretProvider])));

    private sealed class TestDashboardClient(
        AgentSettings settings,
        IDashboardAuthGuard authGuard,
        ISecretResolver secrets)
        : DashboardClientBase(settings, authGuard, secrets)
    {
        public Task<HttpResponseMessage> SendAsync(
            HttpClient http,
            DashboardTokenSnapshot snapshot,
            CancellationToken ct) =>
            SendWithTokenRefreshAsync(
                http,
                token =>
                {
                    var request = new HttpRequestMessage(HttpMethod.Get, Settings.DashboardUrl);
                    request.Headers.Add("X-Agent-Token", token);
                    return request;
                },
                snapshot.Token!,
                snapshot,
                "TestDashboardClient.SendAsync",
                NullLogger.Instance,
                ct);
    }

    private sealed class MutableSecretProvider(string value) : ISecretProvider
    {
        public int ReadCalls { get; private set; }
        public string EmptyValueSourceName => "test secret";
        public bool SupportsSynchronousReads => false;
        public bool CanRead(string provider) => provider == "mutable";

        public Task<string> ReadAsync(SecretRef secret, string settingPath, CancellationToken ct)
        {
            ReadCalls++;
            return Task.FromResult(value);
        }

        public string Read(SecretRef secret, string settingPath) =>
            throw new NotSupportedException();
    }

    private sealed class RecordingAuthGuard : IDashboardAuthGuard
    {
        public int UnauthorizedCalls { get; private set; }

        public void OnUnauthorized(string channel, ILogger logger)
        {
            UnauthorizedCalls++;
        }
    }

    private sealed class RecordingHandler(Func<string, HttpStatusCode> responseFactory) : HttpMessageHandler
    {
        public List<string> Tokens { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var token = request.Headers.GetValues("X-Agent-Token").Single();
            Tokens.Add(token);
            return Task.FromResult(new HttpResponseMessage(responseFactory(token)));
        }
    }
}
