namespace BackupsterAgent.Services.Common.Processes;

public interface IExternalProcessRunner
{
    Task<ExternalProcessResult> RunAsync(
        ExternalProcessRequest request,
        Func<Stream, CancellationToken, Task>? handleStdout,
        Func<StreamWriter, CancellationToken, Task>? handleStdin,
        CancellationToken ct);
}
