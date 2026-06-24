namespace BackupsterAgent.Configuration;

public sealed class AgentSettings
{
    public string Token { get; init; } = string.Empty;
    public SecretRef? TokenSecret { get; init; }
    public string DashboardUrl { get; init; } = string.Empty;
}
