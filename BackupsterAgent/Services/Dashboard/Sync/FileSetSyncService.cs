using System.Net.Http.Json;
using BackupsterAgent.Configuration;
using BackupsterAgent.Contracts;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Extensions.Options;
using Polly;

namespace BackupsterAgent.Services.Dashboard.Sync;

public sealed class FileSetSyncService : DashboardClientBase, IFileSetSyncService
{
    private readonly HttpClient _http;
    private readonly StorageResolver _storages;
    private readonly List<FileSetConfig> _fileSets;
    private readonly ILogger<FileSetSyncService> _logger;
    private readonly ResiliencePipeline _pipeline;
    private readonly SemaphoreSlim _gate = new(1, 1);

    public FileSetSyncService(
        HttpClient http,
        StorageResolver storages,
        IOptions<List<FileSetConfig>> fileSets,
        IOptions<AgentSettings> settings,
        IDashboardAuthGuard authGuard,
        ILogger<FileSetSyncService> logger)
        : base(settings.Value, authGuard)
    {
        _http = http;
        _storages = storages;
        _fileSets = fileSets.Value;
        _logger = logger;
        _pipeline = BuildRetryPipeline(nameof(FileSetSyncService), logger);
    }

    public async Task<bool> SyncAsync(CancellationToken ct = default)
    {
        if (!IsConfigured(_logger, nameof(FileSetSyncService))) return false;

        await _gate.WaitAsync(ct);
        try
        {
            var payload = BuildPayload();

            _logger.LogInformation(
                "FileSetSyncService: syncing {Count} file set(s)",
                payload.FileSets.Count);

            try
            {
                await _pipeline.ExecuteAsync(async innerCt =>
                {
                    var url = $"{Settings.DashboardUrl.TrimEnd('/')}/api/v1/agent/filesets";

                    using var request = new HttpRequestMessage(HttpMethod.Post, url);
                    request.Headers.Add("X-Agent-Token", Settings.Token);
                    request.Content = JsonContent.Create(payload, options: JsonOptions);

                    var response = await _http.SendAsync(request, innerCt);
                    ThrowIfUnauthorized(response, $"{nameof(FileSetSyncService)}.{nameof(SyncAsync)}", _logger);
                    response.EnsureSuccessStatusCode();
                }, ct);

                _logger.LogInformation(
                    "FileSetSyncService: sync delivered ({Count} file set(s))",
                    payload.FileSets.Count);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "FileSetSyncService: all retry attempts exhausted. Sync not delivered.");
                return false;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    private FileSetSyncRequestDto BuildPayload()
    {
        var items = new List<FileSetSyncItemDto>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var fs in _fileSets)
        {
            if (string.IsNullOrWhiteSpace(fs.Name))
            {
                _logger.LogWarning("FileSetSyncService: skipping file set with empty Name.");
                continue;
            }

            if (!seen.Add(fs.Name))
            {
                _logger.LogWarning(
                    "FileSetSyncService: skipping duplicate file set name '{Name}'.", fs.Name);
                continue;
            }

            if (string.IsNullOrWhiteSpace(fs.StorageName))
            {
                _logger.LogWarning(
                    "FileSetSyncService: skipping file set '{Name}' — StorageName is empty.", fs.Name);
                continue;
            }

            if (!_storages.TryResolve(fs.StorageName, out _))
            {
                _logger.LogWarning(
                    "FileSetSyncService: skipping file set '{Name}' — storage '{StorageName}' not configured.",
                    fs.Name, fs.StorageName);
                continue;
            }

            items.Add(new FileSetSyncItemDto
            {
                Name = fs.Name,
                StorageName = fs.StorageName,
            });
        }

        return new FileSetSyncRequestDto { FileSets = items };
    }
}
