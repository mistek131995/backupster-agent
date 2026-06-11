using System.Net;
using System.Text.Json;
using BackupsterAgent.Configuration;
using BackupsterAgent.Contracts;
using BackupsterAgent.Enums;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Services.Dashboard;
using BackupsterAgent.Services.Dashboard.Sync;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class ConnectionSyncServiceTests
{
    [Test]
    public async Task SyncAsync_AllConnectionsHaveEmptyHost_SkipsHttpAndReturnsTrue()
    {
        var handler = new CapturingHandler();
        var service = Build(handler,
        [
            new ConnectionConfig { Name = "a", Host = "", Port = 5432 },
            new ConnectionConfig { Name = "b", Host = "   ", Port = 5432 },
        ]);

        var ok = await service.SyncAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ok, Is.True, "must succeed without sending");
            Assert.That(handler.Calls, Is.Empty, "no HTTP request expected");
        });
    }

    [Test]
    public async Task SyncAsync_AllConnectionsHaveInvalidPort_SkipsHttpAndReturnsTrue()
    {
        var handler = new CapturingHandler();
        var service = Build(handler,
        [
            new ConnectionConfig { Name = "a", Host = "db.internal", Port = 0 },
            new ConnectionConfig { Name = "b", Host = "db.internal", Port = 70_000 },
            new ConnectionConfig { Name = "c", Host = "db.internal", Port = -1 },
        ]);

        var ok = await service.SyncAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ok, Is.True);
            Assert.That(handler.Calls, Is.Empty);
        });
    }

    [Test]
    public async Task SyncAsync_MixedValidAndInvalid_SendsOnlyValid()
    {
        var handler = new CapturingHandler(HttpStatusCode.NoContent);
        var service = Build(handler,
        [
            new ConnectionConfig { Name = "empty-host", Host = "", Port = 5432 },
            new ConnectionConfig { Name = "bad-port", Host = "db.internal", Port = 0 },
            new ConnectionConfig
            {
                Name = "good",
                DatabaseType = DatabaseType.Postgres,
                Host = "db.internal",
                Port = 5432,
            },
        ]);

        var ok = await service.SyncAsync();

        Assert.That(ok, Is.True);
        Assert.That(handler.Calls, Has.Count.EqualTo(1));

        var payload = handler.Calls[0].DeserializeBody<ConnectionSyncRequestDto>();
        Assert.That(payload, Is.Not.Null);
        Assert.That(payload!.Connections, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(payload.Connections[0].Name, Is.EqualTo("good"));
            Assert.That(payload.Connections[0].Host, Is.EqualTo("db.internal"));
            Assert.That(payload.Connections[0].Port, Is.EqualTo(5432));
            Assert.That(payload.Connections[0].DatabaseType, Is.EqualTo("Postgres"));
        });
    }

    [Test]
    public async Task SyncAsync_AllValid_SendsAllAndPostsToCorrectUrlWithToken()
    {
        var handler = new CapturingHandler(HttpStatusCode.NoContent);
        var service = Build(handler,
        [
            new ConnectionConfig { Name = "a", DatabaseType = DatabaseType.Postgres, Host = "h1", Port = 5432 },
            new ConnectionConfig { Name = "b", DatabaseType = DatabaseType.Mssql, Host = "h2", Port = 1433 },
        ],
        dashboardUrl: "http://dashboard.local:8080/");

        var ok = await service.SyncAsync();

        Assert.That(ok, Is.True);
        Assert.That(handler.Calls, Has.Count.EqualTo(1));

        var call = handler.Calls[0];
        Assert.Multiple(() =>
        {
            Assert.That(call.Method, Is.EqualTo(HttpMethod.Post));
            Assert.That(call.Url, Is.EqualTo("http://dashboard.local:8080/api/v1/agent/connections"));
            Assert.That(call.AgentToken, Is.EqualTo("secret-token"));
        });

        var payload = call.DeserializeBody<ConnectionSyncRequestDto>();
        Assert.That(payload!.Connections.Select(c => c.Name), Is.EquivalentTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task SyncAsync_MongoConnectionUri_SendsSanitizedTopology()
    {
        var handler = new CapturingHandler(HttpStatusCode.NoContent);
        var service = Build(handler,
        [
            new ConnectionConfig
            {
                Name = "atlas",
                DatabaseType = DatabaseType.MongoDb,
                ConnectionUri = "mongodb://user:secret@cluster.example.net:27019/?tls=true&tlsCAFile=/etc/ca.pem",
            },
        ]);

        var ok = await service.SyncAsync();

        Assert.That(ok, Is.True);
        Assert.That(handler.Calls, Has.Count.EqualTo(1));

        var payload = handler.Calls[0].DeserializeBody<ConnectionSyncRequestDto>();
        Assert.That(payload, Is.Not.Null);
        Assert.That(payload!.Connections, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(payload.Connections[0].Name, Is.EqualTo("atlas"));
            Assert.That(payload.Connections[0].DatabaseType, Is.EqualTo("MongoDb"));
            Assert.That(payload.Connections[0].Host, Is.EqualTo("cluster.example.net"));
            Assert.That(payload.Connections[0].Port, Is.EqualTo(27019));
        });

        var body = System.Text.Encoding.UTF8.GetString(handler.Calls[0].Body);
        Assert.Multiple(() =>
        {
            Assert.That(body, Does.Not.Contain("secret"));
            Assert.That(body, Does.Not.Contain("tlsCAFile"));
            Assert.That(body, Does.Not.Contain("/etc/ca.pem"));
        });
    }

    [Test]
    public async Task SyncAsync_InvalidMongoConnectionUri_DoesNotLogSensitiveUriParts()
    {
        var handler = new CapturingHandler();
        var logger = new CapturingLogger<ConnectionSyncService>();
        var service = Build(handler,
        [
            new ConnectionConfig
            {
                Name = "atlas",
                DatabaseType = DatabaseType.MongoDb,
                ConnectionUri = "mongodb://user:secret@cluster.example.net:bad/?tls=true&tlsCAFile=/etc/ca.pem",
            },
        ],
        logger: logger);

        var ok = await service.SyncAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ok, Is.True);
            Assert.That(handler.Calls, Is.Empty);
        });

        var logText = string.Join(Environment.NewLine, logger.Messages);
        Assert.Multiple(() =>
        {
            Assert.That(logText, Does.Contain("ConnectionUri is invalid"));
            Assert.That(logText, Does.Not.Contain("secret"));
            Assert.That(logText, Does.Not.Contain("tlsCAFile"));
            Assert.That(logText, Does.Not.Contain("/etc/ca.pem"));
        });
    }

    [Test]
    public async Task SyncAsync_MongoConnectionUriWithZeroPort_SkipsHttp()
    {
        var handler = new CapturingHandler();
        var logger = new CapturingLogger<ConnectionSyncService>();
        var service = Build(handler,
        [
            new ConnectionConfig
            {
                Name = "atlas",
                DatabaseType = DatabaseType.MongoDb,
                ConnectionUri = "mongodb://user:secret@cluster.example.net:0/?tls=true&tlsCAFile=/etc/ca.pem",
            },
        ],
        logger: logger);

        var ok = await service.SyncAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ok, Is.True);
            Assert.That(handler.Calls, Is.Empty);
        });

        var logText = string.Join(Environment.NewLine, logger.Messages);
        Assert.Multiple(() =>
        {
            Assert.That(logText, Does.Contain("ConnectionUri is invalid"));
            Assert.That(logText, Does.Not.Contain("secret"));
            Assert.That(logText, Does.Not.Contain("tlsCAFile"));
            Assert.That(logText, Does.Not.Contain("/etc/ca.pem"));
        });
    }

    [Test]
    public async Task SyncAsync_MongoConnectionUriMixedWithLegacyFields_SkipsHttp()
    {
        var handler = new CapturingHandler();
        var logger = new CapturingLogger<ConnectionSyncService>();
        var service = Build(handler,
        [
            new ConnectionConfig
            {
                Name = "atlas",
                DatabaseType = DatabaseType.MongoDb,
                ConnectionUri = "mongodb://user:secret@cluster.example.net:27017/?tls=true&tlsCAFile=/etc/ca.pem",
                Host = "legacy-host",
            },
        ],
        logger: logger);

        var ok = await service.SyncAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ok, Is.True);
            Assert.That(handler.Calls, Is.Empty);
        });

        var logText = string.Join(Environment.NewLine, logger.Messages);
        Assert.Multiple(() =>
        {
            Assert.That(logText, Does.Contain("ConnectionUri is invalid"));
            Assert.That(logText, Does.Not.Contain("secret"));
            Assert.That(logText, Does.Not.Contain("tlsCAFile"));
            Assert.That(logText, Does.Not.Contain("/etc/ca.pem"));
            Assert.That(logText, Does.Not.Contain("legacy-host"));
        });
    }

    [Test]
    public async Task SyncAsync_MssqlConnectionUri_SendsSanitizedTopology()
    {
        var handler = new CapturingHandler(HttpStatusCode.NoContent);
        var service = Build(handler,
        [
            new ConnectionConfig
            {
                Name = "mssql-uri",
                DatabaseType = DatabaseType.Mssql,
                ConnectionUri = "Server=tcp:sql.example.net,1444;User ID=sql-user;Password=super-secret;Encrypt=True;TrustServerCertificate=False",
            },
        ]);

        var ok = await service.SyncAsync();

        Assert.That(ok, Is.True);
        Assert.That(handler.Calls, Has.Count.EqualTo(1));

        var payload = handler.Calls[0].DeserializeBody<ConnectionSyncRequestDto>();
        Assert.That(payload, Is.Not.Null);
        Assert.That(payload!.Connections, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(payload.Connections[0].Name, Is.EqualTo("mssql-uri"));
            Assert.That(payload.Connections[0].DatabaseType, Is.EqualTo("Mssql"));
            Assert.That(payload.Connections[0].Host, Is.EqualTo("sql.example.net"));
            Assert.That(payload.Connections[0].Port, Is.EqualTo(1444));
        });

        var body = System.Text.Encoding.UTF8.GetString(handler.Calls[0].Body);
        Assert.Multiple(() =>
        {
            Assert.That(body, Does.Not.Contain("super-secret"));
            Assert.That(body, Does.Not.Contain("sql-user"));
            Assert.That(body, Does.Not.Contain("TrustServerCertificate"));
        });
    }

    [Test]
    public async Task SyncAsync_MssqlMixedConnectionUriAndLegacyFields_SkipsHttpWithoutLeakingSecrets()
    {
        var handler = new CapturingHandler();
        var logger = new CapturingLogger<ConnectionSyncService>();
        var service = Build(handler,
        [
            new ConnectionConfig
            {
                Name = "mssql-mixed",
                DatabaseType = DatabaseType.Mssql,
                ConnectionUri = "Server=sql.example.net,1444;User ID=sql-user;Password=super-secret",
                Host = "legacy-host",
                Port = 15433,
                Username = "legacy-user",
                Password = "legacy-secret",
            },
        ],
        logger: logger);

        var ok = await service.SyncAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ok, Is.True);
            Assert.That(handler.Calls, Is.Empty);
        });

        var logText = string.Join(Environment.NewLine, logger.Messages);
        Assert.Multiple(() =>
        {
            Assert.That(logText, Does.Not.Contain("super-secret"));
            Assert.That(logText, Does.Not.Contain("legacy-secret"));
            Assert.That(logText, Does.Not.Contain("sql-user"));
            Assert.That(logText, Does.Not.Contain("sql.example.net"));
            Assert.That(logText, Does.Not.Contain("legacy-host"));
        });
    }

    [Test]
    public async Task SyncAsync_MssqlConnectionUriNamedInstance_SkipsHttpWithoutLeakingUriParts()
    {
        var handler = new CapturingHandler();
        var logger = new CapturingLogger<ConnectionSyncService>();
        var service = Build(handler,
        [
            new ConnectionConfig
            {
                Name = "mssql-uri",
                DatabaseType = DatabaseType.Mssql,
                ConnectionUri = "Server=sql.example.net\\reporting;User ID=sql-user;Password=super-secret;Encrypt=True",
            },
        ],
        logger: logger);

        var ok = await service.SyncAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ok, Is.True);
            Assert.That(handler.Calls, Is.Empty);
        });

        var logText = string.Join(Environment.NewLine, logger.Messages);
        Assert.Multiple(() =>
        {
            Assert.That(logText, Does.Contain("cannot be mapped"));
            Assert.That(logText, Does.Not.Contain("super-secret"));
            Assert.That(logText, Does.Not.Contain("sql-user"));
            Assert.That(logText, Does.Not.Contain("sql.example.net"));
            Assert.That(logText, Does.Not.Contain("reporting"));
        });
    }

    [Test]
    public async Task SyncAsync_InvalidMssqlConnectionUri_DoesNotLogSensitiveUriParts()
    {
        var handler = new CapturingHandler();
        var logger = new CapturingLogger<ConnectionSyncService>();
        var service = Build(handler,
        [
            new ConnectionConfig
            {
                Name = "mssql-uri",
                DatabaseType = DatabaseType.Mssql,
                ConnectionUri = "Server=sql.example.net;User ID=sql-user;Password=super-secret;Unsupported Keyword=value",
            },
        ],
        logger: logger);

        var ok = await service.SyncAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ok, Is.True);
            Assert.That(handler.Calls, Is.Empty);
        });

        var logText = string.Join(Environment.NewLine, logger.Messages);
        Assert.Multiple(() =>
        {
            Assert.That(logText, Does.Contain("ConnectionUri is invalid"));
            Assert.That(logText, Does.Not.Contain("super-secret"));
            Assert.That(logText, Does.Not.Contain("sql-user"));
            Assert.That(logText, Does.Not.Contain("Unsupported Keyword"));
            Assert.That(logText, Does.Not.Contain("sql.example.net"));
        });
    }

    [Test]
    public async Task SyncAsync_EmptyToken_SkipsHttpAndReturnsFalse()
    {
        var handler = new CapturingHandler();
        var service = Build(handler,
        [
            new ConnectionConfig { Name = "a", Host = "db.internal", Port = 5432 },
        ],
        token: "");

        var ok = await service.SyncAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ok, Is.False);
            Assert.That(handler.Calls, Is.Empty);
        });
    }

    [Test]
    public async Task SyncAsync_ServerReturns400_RetriesThenReturnsFalse()
    {
        var handler = new CapturingHandler(HttpStatusCode.BadRequest);
        var service = Build(handler,
        [
            new ConnectionConfig { Name = "a", Host = "db.internal", Port = 5432 },
        ]);

        var ok = await service.SyncAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ok, Is.False);
            Assert.That(handler.Calls.Count, Is.GreaterThan(1), "retry pipeline must fire at least once");
        });
    }

    private static ConnectionSyncService Build(
        HttpMessageHandler handler,
        IReadOnlyList<ConnectionConfig> connections,
        string token = "secret-token",
        string dashboardUrl = "http://dashboard.local:8080",
        ILogger<ConnectionSyncService>? logger = null)
    {
        var http = new HttpClient(handler);
        var resolver = new ConnectionResolver(connections);
        var settings = Options.Create(new AgentSettings
        {
            Token = token,
            DashboardUrl = dashboardUrl,
        });
        return new ConnectionSyncService(http, resolver, settings,
            new NullAuthGuard(),
            logger ?? NullLogger<ConnectionSyncService>.Instance);
    }

    private sealed class NullAuthGuard : IDashboardAuthGuard
    {
        public void OnUnauthorized(string channel, Microsoft.Extensions.Logging.ILogger logger) { }
    }

    private sealed class CapturingHandler : HttpMessageHandler
    {
        private readonly HttpStatusCode _status;
        public List<CapturedCall> Calls { get; } = new();

        public CapturingHandler(HttpStatusCode status = HttpStatusCode.NoContent)
        {
            _status = status;
        }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = request.Content is null
                ? Array.Empty<byte>()
                : await request.Content.ReadAsByteArrayAsync(cancellationToken);

            request.Headers.TryGetValues("X-Agent-Token", out var tokenValues);

            Calls.Add(new CapturedCall(
                request.Method,
                request.RequestUri!.ToString(),
                tokenValues?.FirstOrDefault(),
                body));

            return new HttpResponseMessage(_status);
        }
    }

    private sealed record CapturedCall(HttpMethod Method, string Url, string? AgentToken, byte[] Body)
    {
        public T? DeserializeBody<T>() => JsonSerializer.Deserialize<T>(Body,
            new JsonSerializerOptions(JsonSerializerDefaults.Web));
    }

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public List<string> Messages { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            if (exception is not null)
                message += Environment.NewLine + exception;

            Messages.Add(message);
        }
    }
}
