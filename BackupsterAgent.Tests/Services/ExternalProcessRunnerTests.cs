using BackupsterAgent.Services.Common.Processes;
using Microsoft.Extensions.Logging.Abstractions;

namespace BackupsterAgent.Tests.Services;

public sealed class ExternalProcessRunnerTests
{
    private static (string fileName, string[] argPrefix) ShellExit(int code) =>
        OperatingSystem.IsWindows()
            ? ("cmd.exe", new[] { "/c", $"exit {code}" })
            : ("/bin/sh", new[] { "-c", $"exit {code}" });

    private static (string fileName, string[] argPrefix) ShellEchoStderrExit(string text, int code) =>
        OperatingSystem.IsWindows()
            ? ("cmd.exe", new[] { "/c", $"echo {text} 1>&2 & exit {code}" })
            : ("/bin/sh", new[] { "-c", $"echo {text} >&2; exit {code}" });

    private static ExternalProcessRunner CreateRunner() => new(NullLogger<ExternalProcessRunner>.Instance);

    [Test]
    public async Task PumpFailure_ProcessExitsNonZero_ReturnsResultWithExitCode()
    {
        var (fileName, args) = ShellEchoStderrExit("oops", 7);
        var runner = CreateRunner();

        var result = await runner.RunAsync(
            new ExternalProcessRequest
            {
                FileName = fileName,
                Arguments = args,
                RedirectStandardInput = true,
            },
            handleStdout: null,
            handleStdin: async (_, _) =>
            {
                await Task.Yield();
                throw new IOException("simulated broken pipe");
            },
            CancellationToken.None);

        Assert.That(result.ExitCode, Is.EqualTo(7));
        Assert.That(result.Stderr, Does.Contain("oops"));
    }

    [Test]
    public void PumpFailure_ProcessExitsZero_RethrowsPumpException()
    {
        var (fileName, args) = ShellExit(0);
        var runner = CreateRunner();

        var ex = Assert.ThrowsAsync<IOException>(() => runner.RunAsync(
            new ExternalProcessRequest
            {
                FileName = fileName,
                Arguments = args,
                RedirectStandardInput = true,
            },
            handleStdout: null,
            handleStdin: async (_, _) =>
            {
                await Task.Yield();
                throw new IOException("real source-side IO error");
            },
            CancellationToken.None));

        Assert.That(ex!.Message, Is.EqualTo("real source-side IO error"));
    }

    [Test]
    public void PumpThrowsOperationCanceled_PropagatesAsCancellation()
    {
        var (fileName, args) = ShellExit(0);
        var runner = CreateRunner();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(() => runner.RunAsync(
            new ExternalProcessRequest
            {
                FileName = fileName,
                Arguments = args,
                RedirectStandardInput = true,
            },
            handleStdout: null,
            handleStdin: async (_, ct) =>
            {
                await Task.Yield();
                ct.ThrowIfCancellationRequested();
            },
            cts.Token));
    }

    [Test]
    public async Task HappyPath_ProcessExitsZero_ReturnsResult()
    {
        var (fileName, args) = ShellEchoStderrExit("done", 0);
        var runner = CreateRunner();

        var result = await runner.RunAsync(
            new ExternalProcessRequest
            {
                FileName = fileName,
                Arguments = args,
                RedirectStandardInput = false,
            },
            handleStdout: null,
            handleStdin: null,
            CancellationToken.None);

        Assert.That(result.ExitCode, Is.Zero);
        Assert.That(result.Stderr, Does.Contain("done"));
    }
}
