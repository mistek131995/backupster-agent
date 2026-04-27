namespace BackupsterAgent.Services.Common.Processes;

public sealed class ExternalProcessRequest
{
    public required string FileName { get; init; }
    public required IReadOnlyList<string> Arguments { get; init; }
    public IReadOnlyDictionary<string, string?>? EnvironmentOverrides { get; init; }
    public bool RedirectStandardInput { get; init; }
}
