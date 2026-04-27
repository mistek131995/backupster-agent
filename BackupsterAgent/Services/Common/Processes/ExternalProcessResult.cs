namespace BackupsterAgent.Services.Common.Processes;

public sealed class ExternalProcessResult
{
    public required int ExitCode { get; init; }
    public required string Stdout { get; init; }
    public required string Stderr { get; init; }
}
