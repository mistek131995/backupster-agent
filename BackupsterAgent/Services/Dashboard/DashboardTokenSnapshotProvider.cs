using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Secrets;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Services.Dashboard;

public sealed class DashboardTokenSnapshotProvider : IDashboardTokenSnapshotProvider
{
    private readonly AgentSettings _settings;
    private readonly ISecretResolver _secrets;
    private readonly ILogger<DashboardTokenSnapshotProvider> _logger;

    public DashboardTokenSnapshotProvider(
        IOptions<AgentSettings> settings,
        ISecretResolver secrets,
        ILogger<DashboardTokenSnapshotProvider> logger)
    {
        _settings = settings.Value;
        _secrets = secrets;
        _logger = logger;
    }

    public async Task<DashboardTokenSnapshot> CaptureAsync(CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(_settings.DashboardUrl))
        {
            _logger.LogWarning("DashboardTokenSnapshotProvider: AgentSettings.DashboardUrl is not configured.");
            return new DashboardTokenSnapshot(null);
        }

        string token;
        try
        {
            token = await _secrets.ResolveStringAsync(
                _settings.TokenSecret,
                _settings.Token,
                "AgentSettings.Token",
                ct);
        }
        catch (SecretResolutionException ex)
        {
            _logger.LogError(ex, "DashboardTokenSnapshotProvider: failed to resolve AgentSettings.Token.");
            return new DashboardTokenSnapshot(null);
        }

        if (!string.IsNullOrWhiteSpace(token))
            return new DashboardTokenSnapshot(token);

        _logger.LogWarning("DashboardTokenSnapshotProvider: AgentSettings.Token is not configured.");
        return new DashboardTokenSnapshot(null);
    }
}
