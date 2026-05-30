using BackupsterAgent.Configuration;
using BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;
using BackupsterAgent.Services.Common.Processes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Tests.Providers;

public sealed class MysqlSystemdControllerTests
{
    [Test]
    public void Mask_NonZeroExit_Throws()
    {
        var runner = new RecordingSystemctlRunner { ExitCodes = { ["mask"] = 1 } };
        var controller = CreateController(runner);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => controller.MaskAsync("mysql.service"));

        Assert.That(ex!.Message, Does.Contain("замаскировать"));
        Assert.That(runner.Verbs, Does.Contain("mask"));
    }

    [Test]
    public void Mask_ZeroExit_Completes()
    {
        var runner = new RecordingSystemctlRunner();
        var controller = CreateController(runner);

        Assert.DoesNotThrowAsync(() => controller.MaskAsync("mysql.service"));
        Assert.That(runner.Verbs, Is.EqualTo(new[] { "mask" }));
    }

    [Test]
    public void Mask_WhenSystemctlHangs_ThrowsTimeoutInsteadOfHanging()
    {
        var runner = new RecordingSystemctlRunner { HangingVerbs = { "mask" } };
        var controller = CreateController(runner, timeoutSeconds: 1);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => controller.MaskAsync("mysql.service"));

        Assert.That(ex!.Message, Does.Contain("не завершилась"));
    }

    [Test]
    public void Unmask_NonZeroExit_DoesNotThrow()
    {
        var runner = new RecordingSystemctlRunner { ExitCodes = { ["unmask"] = 1 } };
        var controller = CreateController(runner);

        Assert.DoesNotThrowAsync(() => controller.TryUnmaskAsync("mysql.service"));
        Assert.That(runner.Verbs, Does.Contain("unmask"));
    }

    [Test]
    public void Unmask_RunnerThrows_DoesNotThrow()
    {
        var runner = new ThrowingSystemctlRunner();
        var controller = CreateController(runner);

        Assert.DoesNotThrowAsync(() => controller.TryUnmaskAsync("mysql.service"));
    }

    [Test]
    public void Unmask_WhenSystemctlHangs_DoesNotThrowAndDoesNotHang()
    {
        var runner = new RecordingSystemctlRunner { HangingVerbs = { "unmask" } };
        var controller = CreateController(runner, timeoutSeconds: 1);

        Assert.DoesNotThrowAsync(() => controller.TryUnmaskAsync("mysql.service"));
    }

    [Test]
    public void Stop_WhenSystemctlHangs_ThrowsTimeoutInsteadOfHanging()
    {
        var runner = new RecordingSystemctlRunner { HangingVerbs = { "stop" } };
        var controller = CreateController(runner, stopStartTimeoutSeconds: 1);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
            controller.StopAsync("mysql.service", CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("не завершилась"));
    }

    private static MysqlSystemdController CreateController(
        IExternalProcessRunner runner, int? timeoutSeconds = null, int? stopStartTimeoutSeconds = null)
    {
        var settings = new RestoreSettings
        {
            SystemctlTimeoutSeconds = timeoutSeconds ?? new RestoreSettings().SystemctlTimeoutSeconds,
            SystemctlStopStartTimeoutSeconds = stopStartTimeoutSeconds ?? new RestoreSettings().SystemctlStopStartTimeoutSeconds,
        };

        return new MysqlSystemdController(
            NullLogger<MysqlSystemdController>.Instance, runner, Options.Create(settings));
    }

    private sealed class ThrowingSystemctlRunner : IExternalProcessRunner
    {
        public Task<ExternalProcessResult> RunAsync(
            ExternalProcessRequest request,
            Func<Stream, CancellationToken, Task>? handleStdout,
            Func<StreamWriter, CancellationToken, Task>? handleStdin,
            CancellationToken ct) =>
            throw new InvalidOperationException("systemctl unavailable");
    }
}

internal sealed class RecordingSystemctlRunner : IExternalProcessRunner
{
    public Dictionary<string, int> ExitCodes { get; } = new();
    public HashSet<string> HangingVerbs { get; } = new();
    public List<string> Verbs { get; } = new();

    public async Task<ExternalProcessResult> RunAsync(
        ExternalProcessRequest request,
        Func<Stream, CancellationToken, Task>? handleStdout,
        Func<StreamWriter, CancellationToken, Task>? handleStdin,
        CancellationToken ct)
    {
        var verb = request.Arguments[0];
        Verbs.Add(verb);

        if (HangingVerbs.Contains(verb))
            await Task.Delay(Timeout.Infinite, ct);

        var exitCode = ExitCodes.TryGetValue(verb, out var code) ? code : 0;
        return new ExternalProcessResult
        {
            ExitCode = exitCode,
            Stdout = string.Empty,
            Stderr = exitCode == 0 ? string.Empty : $"systemctl {verb} failed",
        };
    }
}
