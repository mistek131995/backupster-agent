using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace BackupsterAgent.Services.Common.Processes;

public sealed class ExternalProcessRunner : IExternalProcessRunner
{
    private static readonly Encoding ChildProcessEncoding = ResolveChildProcessEncoding();

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
            StandardOutputEncoding = ChildProcessEncoding,
            StandardErrorEncoding = ChildProcessEncoding,
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

        Exception? pumpFailure = null;
        try
        {
            await Task.WhenAll(pumpTasks);
        }
        catch (OperationCanceledException)
        {
            KillProcessTree(process, request.FileName);
            throw;
        }
        catch (Exception ex)
        {
            pumpFailure = ex;
        }

        try
        {
            await process.WaitForExitAsync(ct);
        }
        catch (OperationCanceledException)
        {
            KillProcessTree(process, request.FileName);
            throw;
        }

        var stderrText = await DrainAsync(stderrTask);
        var stdoutText = stdoutTextTask is null ? string.Empty : await DrainAsync(stdoutTextTask);

        if (pumpFailure is not null && process.ExitCode == 0)
            throw pumpFailure;

        return new ExternalProcessResult
        {
            ExitCode = process.ExitCode,
            Stdout = Sanitize(stdoutText),
            Stderr = Sanitize(stderrText),
        };
    }

    private static async Task<string> DrainAsync(Task<string> task)
    {
        try { return await task; }
        catch { return string.Empty; }
    }

    private void KillProcessTree(Process process, string fileName)
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
                fileName, process.Id);
        }
    }

    private static Encoding ResolveChildProcessEncoding()
    {
        if (!OperatingSystem.IsWindows())
            return Encoding.UTF8;

        try
        {
            var oemCodePage = CultureInfo.CurrentCulture.TextInfo.OEMCodePage;
            if (oemCodePage > 0)
                return Encoding.GetEncoding(oemCodePage);
        }
        catch
        {
        }

        return Encoding.UTF8;
    }

    private static string Sanitize(string value)
    {
        if (string.IsNullOrEmpty(value) || !value.Contains('�'))
            return value;

        return value.Replace('�', '?');
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
