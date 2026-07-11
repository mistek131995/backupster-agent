using System.Net;
using System.Net.Http.Json;
using BackupsterAgent.Configuration;
using BackupsterAgent.Contracts;
using BackupsterAgent.Enums;
using BackupsterAgent.Services.Common.Secrets;
using Microsoft.Extensions.Options;
using Polly;

namespace BackupsterAgent.Services.Dashboard.Clients;

public sealed class BackupRecordClient : DashboardClientBase, IBackupRecordClient
{
    private readonly HttpClient _http;
    private readonly ILogger<BackupRecordClient> _logger;
    private readonly ResiliencePipeline _retryPipeline;

    public BackupRecordClient(
        HttpClient http,
        IOptions<AgentSettings> settings,
        IDashboardAuthGuard authGuard,
        ISecretResolver secrets,
        ILogger<BackupRecordClient> logger)
        : base(settings.Value, authGuard, secrets)
    {
        _http = http;
        _logger = logger;
        _retryPipeline = BuildRetryPipeline(nameof(BackupRecordClient), logger);
    }

    public async Task<OpenRecordResult> OpenAsync(
        OpenBackupRecordDto dto,
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null)
    {
        var token = await ResolveTokenOrSkipAsync(_logger, nameof(BackupRecordClient), ct, tokenSnapshot);
        if (token is null)
            return new OpenRecordResult(DashboardAvailability.PermanentSkip);

        var url = $"{Settings.DashboardUrl.TrimEnd('/')}/api/v1/agent/backup-record";

        try
        {
            var response = await _retryPipeline.ExecuteAsync(async innerCt =>
            {
                return await SendWithTokenRefreshAsync(
                    _http,
                    effectiveToken =>
                    {
                        var request = new HttpRequestMessage(HttpMethod.Post, url);
                        request.Headers.Add("X-Agent-Token", effectiveToken);
                        request.Content = JsonContent.Create(dto, options: JsonOptions);
                        return request;
                    },
                    token,
                    tokenSnapshot,
                    $"{nameof(BackupRecordClient)}.{nameof(OpenAsync)}",
                    _logger,
                    innerCt);
            }, ct);

            using (response)
            {
                var availability = DashboardAvailabilityPolicy.ClassifyResponse(response);
                if (availability != DashboardAvailability.Ok)
                {
                    LogNonOk(nameof(OpenAsync), dto.DatabaseName, availability, response.StatusCode);
                    return new OpenRecordResult(availability);
                }

                var body = await response.Content.ReadFromJsonAsync<OpenBackupRecordResponseDto>(JsonOptions, ct);

                if (body is null || body.Id == Guid.Empty)
                {
                    _logger.LogWarning(
                        "BackupRecordClient: server returned empty body for '{Database}'", dto.DatabaseName);
                    return new OpenRecordResult(DashboardAvailability.PermanentSkip);
                }

                _logger.LogInformation(
                    "BackupRecordClient: opened record {Id} for '{Database}'", body.Id, dto.DatabaseName);
                return new OpenRecordResult(
                    DashboardAvailability.Ok,
                    body.Id,
                    body.BaseDumpObjectKey,
                    body.BasePgBaseManifestKey);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            var availability = DashboardAvailabilityPolicy.ClassifyException(ex);
            _logger.LogWarning(
                "BackupRecordClient: dashboard unavailable, open skipped for '{Database}' — {Availability}",
                dto.DatabaseName, availability);
            return new OpenRecordResult(availability);
        }
    }

    public async Task ReportProgressAsync(
        Guid backupRecordId,
        BackupProgressDto progress,
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null)
    {
        var token = await ResolveTokenOrSkipAsync(_logger, nameof(BackupRecordClient), ct, tokenSnapshot);
        if (token is null) return;

        var url = $"{Settings.DashboardUrl.TrimEnd('/')}/api/v1/agent/backup-record/{backupRecordId}/progress";

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(3));

        using var response = await SendWithTokenRefreshAsync(
            _http,
            effectiveToken =>
            {
                var request = new HttpRequestMessage(HttpMethod.Post, url);
                request.Headers.Add("X-Agent-Token", effectiveToken);
                request.Content = JsonContent.Create(progress, options: JsonOptions);
                return request;
            },
            token,
            tokenSnapshot,
            $"{nameof(BackupRecordClient)}.{nameof(ReportProgressAsync)}",
            _logger,
            timeoutCts.Token);
        response.EnsureSuccessStatusCode();
    }

    public async Task<FinalizeRecordResult> FinalizeAsync(
        Guid backupRecordId,
        FinalizeBackupRecordDto dto,
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null)
    {
        var token = await ResolveTokenOrSkipAsync(_logger, nameof(BackupRecordClient), ct, tokenSnapshot);
        if (token is null)
            return new FinalizeRecordResult(DashboardAvailability.PermanentSkip);

        var url = $"{Settings.DashboardUrl.TrimEnd('/')}/api/v1/agent/backup-record/{backupRecordId}";

        try
        {
            var availability = DashboardAvailability.Ok;

            await _retryPipeline.ExecuteAsync(async innerCt =>
            {
                using var response = await SendWithTokenRefreshAsync(
                    _http,
                    effectiveToken =>
                    {
                        var request = new HttpRequestMessage(HttpMethod.Patch, url);
                        request.Headers.Add("X-Agent-Token", effectiveToken);
                        request.Content = JsonContent.Create(dto, options: JsonOptions);
                        return request;
                    },
                    token,
                    tokenSnapshot,
                    $"{nameof(BackupRecordClient)}.{nameof(FinalizeAsync)}",
                    _logger,
                    innerCt);

                availability = DashboardAvailabilityPolicy.ClassifyResponse(response);
                if (availability == DashboardAvailability.OfflineRetryable)
                    response.EnsureSuccessStatusCode();
            }, ct);

            if (availability == DashboardAvailability.Ok)
            {
                _logger.LogInformation(
                    "BackupRecordClient: finalized record {Id} with status '{Status}'",
                    backupRecordId, dto.Status);
            }
            else
            {
                LogNonOk(nameof(FinalizeAsync), backupRecordId.ToString(), availability, status: null);
            }

            return new FinalizeRecordResult(availability);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            var availability = DashboardAvailabilityPolicy.ClassifyException(ex);
            _logger.LogWarning(
                "BackupRecordClient: dashboard unavailable, finalize skipped for record {Id} — {Availability}",
                backupRecordId, availability);
            return new FinalizeRecordResult(availability);
        }
    }

    public async Task<LastSuccessfulLookupResult> GetLastSuccessfulAsync(
        string database,
        string storage,
        BackupMode mode,
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null)
    {
        var token = await ResolveTokenOrSkipAsync(_logger, nameof(BackupRecordClient), ct, tokenSnapshot);
        if (token is null)
            return new LastSuccessfulLookupResult(LastSuccessfulLookupOutcome.DashboardUnavailable);

        var modeQuery = mode switch
        {
            BackupMode.Logical => "logical",
            BackupMode.Physical => "physical",
            BackupMode.PhysicalDifferential => "physicalDifferential",
            _ => mode.ToString().ToLowerInvariant(),
        };

        var url = $"{Settings.DashboardUrl.TrimEnd('/')}/api/v1/agent/backup-records/last-successful" +
                  $"?database={Uri.EscapeDataString(database)}" +
                  $"&storage={Uri.EscapeDataString(storage)}" +
                  $"&mode={Uri.EscapeDataString(modeQuery)}";

        try
        {
            using var response = await SendWithTokenRefreshAsync(
                _http,
                effectiveToken =>
                {
                    var request = new HttpRequestMessage(HttpMethod.Get, url);
                    request.Headers.Add("X-Agent-Token", effectiveToken);
                    return request;
                },
                token,
                tokenSnapshot,
                $"{nameof(BackupRecordClient)}.{nameof(GetLastSuccessfulAsync)}",
                _logger,
                ct);

            if (response.StatusCode == HttpStatusCode.NotFound)
                return new LastSuccessfulLookupResult(LastSuccessfulLookupOutcome.NotFound);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning(
                    "BackupRecordClient.GetLastSuccessfulAsync: HTTP {Status} for '{Database}' on '{Storage}' — treating as dashboard unavailable",
                    (int)response.StatusCode, database, storage);
                return new LastSuccessfulLookupResult(LastSuccessfulLookupOutcome.DashboardUnavailable);
            }

            var body = await response.Content.ReadFromJsonAsync<LastSuccessfulBackupResponseDto>(JsonOptions, ct);

            if (body is null || body.Id == Guid.Empty)
                return new LastSuccessfulLookupResult(LastSuccessfulLookupOutcome.NotFound);

            _logger.LogInformation(
                "BackupRecordClient.GetLastSuccessfulAsync: found {Id} for '{Database}' on '{Storage}' (mode={Mode})",
                body.Id, database, storage, mode);

            return new LastSuccessfulLookupResult(LastSuccessfulLookupOutcome.Found, body);
        }
        catch (DashboardUnauthorizedException)
        {
            throw;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "BackupRecordClient.GetLastSuccessfulAsync: dashboard unavailable for '{Database}' on '{Storage}'",
                database, storage);
            return new LastSuccessfulLookupResult(LastSuccessfulLookupOutcome.DashboardUnavailable);
        }
    }

    private void LogNonOk(string channel, string subject, DashboardAvailability availability, System.Net.HttpStatusCode? status)
    {
        _logger.LogWarning(
            "BackupRecordClient.{Channel}: '{Subject}' classified as {Availability}{Status}",
            channel, subject, availability,
            status.HasValue ? $" (HTTP {(int)status.Value})" : string.Empty);
    }
}
