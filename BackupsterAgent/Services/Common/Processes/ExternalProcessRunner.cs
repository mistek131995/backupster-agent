using System.Diagnostics;
using System.Text;

namespace BackupsterAgent.Services.Common.Processes;

public sealed class ExternalProcessRunner : IExternalProcessRunner
{
    private readonly ILogger<ExternalProcessRunner> _logger;

    public ExternalProcessRunner(ILogger<ExternalProcessRunner> logger)
    {
        _logger = logger;
    }

    public async Task<ExternalProcessResult> RunAsync(
        ExternalProcessRequest request,
        Func<Stream, CancellationToken, Task>? handleStdout,
        Func<StreamWriter, CancellationToken, Task>? handleStdin,
        CancellationToken ct)
    {
        var psi = new ProcessStartInfo
        {
            FileName = request.FileName,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            RedirectStandardInput = request.RedirectStandardInput,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        foreach (var arg in request.Arguments)
            psi.ArgumentList.Add(arg);

        if (request.EnvironmentOverrides is not null)
        {
            foreach (var (key, value) in request.EnvironmentOverrides)
                psi.Environment[key] = value;
        }

        using var process = new Process { StartInfo = psi };

        process.Start();
        _logger.LogDebug(
            "External process started: {FileName} (PID {Pid})", request.FileName, process.Id);

        using var killReg = ct.Register(() =>
        {
            try
            {
                if (!process.HasExited)
                    process.Kill(entireProcessTree: true);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Failed to kill external process {FileName} (PID {Pid}) on cancellation",
                    request.FileName, process.Id);
            }
        });

        var stderrTask = process.StandardError.ReadToEndAsync(ct);

        Task<string>? stdoutTextTask = null;
        Task? stdoutHandlerTask = null;
        if (handleStdout is null)
            stdoutTextTask = process.StandardOutput.ReadToEndAsync(ct);
        else
            stdoutHandlerTask = handleStdout(process.StandardOutput.BaseStream, ct);

        Task? stdinTask = null;
        if (request.RedirectStandardInput)
        {
            if (handleStdin is null)
                process.StandardInput.Close();
            else
                stdinTask = WriteStdinAsync(process.StandardInput, handleStdin, ct);
        }

        var pumpTasks = new List<Task> { stderrTask };
        if (stdoutTextTask is not null) pumpTasks.Add(stdoutTextTask);
        if (stdoutHandlerTask is not null) pumpTasks.Add(stdoutHandlerTask);
        if (stdinTask is not null) pumpTasks.Add(stdinTask);

        try
        {
            await Task.WhenAll(pumpTasks);
            await process.WaitForExitAsync(ct);
        }
        catch
        {
            try
            {
                if (!process.HasExited)
                    process.Kill(entireProcessTree: true);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Failed to kill external process {FileName} (PID {Pid}) after error",
                    request.FileName, process.Id);
            }
            throw;
        }

        return new ExternalProcessResult
        {
            ExitCode = process.ExitCode,
            Stdout = stdoutTextTask is null ? string.Empty : await stdoutTextTask,
            Stderr = await stderrTask,
        };
    }

    private static async Task WriteStdinAsync(
        StreamWriter stdin,
        Func<StreamWriter, CancellationToken, Task> handler,
        CancellationToken ct)
    {
        try
        {
            await handler(stdin, ct);
        }
        finally
        {
            stdin.Close();
        }
    }
}
