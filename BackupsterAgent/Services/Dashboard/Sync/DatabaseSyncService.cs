using System.Net.Http.Json;
using BackupsterAgent.Configuration;
using BackupsterAgent.Contracts;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Extensions.Options;
using Polly;

namespace BackupsterAgent.Services.Dashboard.Sync;

public sealed class DatabaseSyncService : DashboardClientBase, IDatabaseSyncService
{
    private readonly HttpClient _http;
    private readonly ConnectionResolver _connections;
    private readonly List<DatabaseConfig> _databases;
    private readonly ILogger<DatabaseSyncService> _logger;
    private readonly ResiliencePipeline _pipeline;
    private readonly SemaphoreSlim _gate = new(1, 1);

    public DatabaseSyncService(
        HttpClient http,
        ConnectionResolver connections,
        IOptions<List<DatabaseConfig>> databases,
        IOptions<AgentSettings> settings,
        IDashboardAuthGuard authGuard,
        ILogger<DatabaseSyncService> logger)
        : base(settings.Value, authGuard)
    {
        _http = http;
        _connections = connections;
        _databases = databases.Value;
        _logger = logger;
        _pipeline = BuildRetryPipeline(nameof(DatabaseSyncService), logger);
    }

    public async Task<bool> SyncAsync(CancellationToken ct = default)
    {
        if (!IsConfigured(_logger, nameof(DatabaseSyncService))) return false;

        await _gate.WaitAsync(ct);
        try
        {
            var payload = BuildPayload();

            _logger.LogInformation(
                "DatabaseSyncService: syncing {Count} database(s)",
                payload.Databases.Count);

            try
            {
                await _pipeline.ExecuteAsync(async innerCt =>
                {
                    var url = $"{Settings.DashboardUrl.TrimEnd('/')}/api/v1/agent/databases";

                    using var request = new HttpRequestMessage(HttpMethod.Post, url);
                    request.Headers.Add("X-Agent-Token", Settings.Token);
                    request.Content = JsonContent.Create(payload, options: JsonOptions);

                    var response = await _http.SendAsync(request, innerCt);
                    ThrowIfUnauthorized(response, $"{nameof(DatabaseSyncService)}.{nameof(SyncAsync)}", _logger);
                    response.EnsureSuccessStatusCode();
                }, ct);

                _logger.LogInformation(
                    "DatabaseSyncService: sync delivered ({Count} database(s))",
                    payload.Databases.Count);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "DatabaseSyncService: all retry attempts exhausted. Sync not delivered.");
                return false;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    private DatabaseSyncRequestDto BuildPayload()
    {
        var items = new List<DatabaseSyncItemDto>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var db in _databases)
        {
            if (string.IsNullOrWhiteSpace(db.Database))
            {
                _logger.LogWarning("DatabaseSyncService: skipping database with empty Database name.");
                continue;
            }

            if (!seen.Add(db.Database))
            {
                _logger.LogWarning(
                    "DatabaseSyncService: skipping duplicate database name '{Name}'.", db.Database);
                continue;
            }

            if (string.IsNullOrWhiteSpace(db.ConnectionName))
            {
                _logger.LogWarning(
                    "DatabaseSyncService: skipping '{Name}' — ConnectionName is empty.", db.Database);
                continue;
            }

            if (!_connections.TryResolve(db.ConnectionName, out var conn))
            {
                _logger.LogWarning(
                    "DatabaseSyncService: skipping '{Name}' — connection '{Conn}' not configured.",
                    db.Database, db.ConnectionName);
                continue;
            }

            items.Add(new DatabaseSyncItemDto
            {
                Name = db.Database,
                DatabaseType = conn.DatabaseType.ToString(),
            });
        }

        return new DatabaseSyncRequestDto { Databases = items };
    }
}
