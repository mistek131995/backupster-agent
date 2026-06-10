using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Processes;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Providers.Restore.Common;

public sealed class SystemdServiceController
{
    private readonly ILogger<SystemdServiceController> _logger;
    private readonly IExternalProcessRunner _processRunner;
    private readonly RestoreSettings _restoreSettings;

    public SystemdServiceController(
        ILogger<SystemdServiceController> logger,
        IExternalProcessRunner processRunner,
        IOptions<RestoreSettings> restoreSettings)
    {
        _logger = logger;
        _processRunner = processRunner;
        _restoreSettings = restoreSettings.Value;
    }

    public async Task MaskAsync(string serviceName, string subject)
    {
        _logger.LogInformation("Masking {Subject} '{ServiceName}' to block systemd auto-restart", subject, serviceName);

        var result = await RunForServiceAsync("mask", serviceName, CancellationToken.None, _restoreSettings.SystemctlTimeoutSeconds);
        if (result.ExitCode != 0)
            throw new InvalidOperationException(
                $"Не удалось замаскировать {subject} '{serviceName}' (код {result.ExitCode}). " +
                "Восстановление прервано: без маскировки systemd может перезапустить сервис во время подмены data directory." +
                FormatOutput(result));
    }

    public async Task TryUnmaskAsync(string serviceName, string subject)
    {
        try
        {
            var result = await RunForServiceAsync("unmask", serviceName, CancellationToken.None, _restoreSettings.SystemctlTimeoutSeconds);
            if (result.ExitCode != 0)
                _logger.LogError(
                    "Failed to unmask {Subject} '{ServiceName}' (exit code {ExitCode}) - it may remain masked and require a manual 'systemctl unmask'.{Output}",
                    subject, serviceName, result.ExitCode, FormatOutput(result));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to unmask {Subject} '{ServiceName}' - it may remain masked and require a manual 'systemctl unmask'.",
                subject, serviceName);
        }
    }

    public async Task StopAsync(string serviceName, string subject, CancellationToken ct)
    {
        _logger.LogInformation("Stopping {Subject} '{ServiceName}'", subject, serviceName);

        var result = await RunForServiceAsync("stop", serviceName, ct, _restoreSettings.SystemctlStopStartTimeoutSeconds);
        if (result.ExitCode != 0)
            throw new InvalidOperationException(
                $"Не удалось остановить {subject} '{serviceName}' (код {result.ExitCode}). " +
                "Убедитесь, что агент имеет права на управление сервисом (root или настроенный polkit/sudoers)." +
                FormatOutput(result));
    }

    public async Task StartAsync(string serviceName, string subject, CancellationToken ct)
    {
        _logger.LogInformation("Starting {Subject} '{ServiceName}'", subject, serviceName);

        var result = await RunForServiceAsync("start", serviceName, ct, _restoreSettings.SystemctlStopStartTimeoutSeconds);
        if (result.ExitCode != 0)
            throw new InvalidOperationException(
                $"Не удалось запустить {subject} '{serviceName}' (код {result.ExitCode}). " +
                "Убедитесь, что агент имеет права на управление сервисом и восстановленный data directory доступен пользователю сервиса." +
                FormatOutput(result));
    }

    public async Task<bool> IsActiveAsync(string serviceName, CancellationToken ct)
    {
        try
        {
            var result = await RunForServiceAsync("is-active", serviceName, ct, _restoreSettings.SystemctlTimeoutSeconds);
            return result.ExitCode == 0;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to check if systemd service '{ServiceName}' is active - assuming active",
                serviceName);
            return true;
        }
    }

    public async Task EnsureMainPidAsync(
        string serviceName,
        int expectedPid,
        string subject,
        string processDescription,
        CancellationToken ct)
    {
        var result = await RunAsync("show", [serviceName, "--property", "MainPID", "--value"],
            ct, _restoreSettings.SystemctlTimeoutSeconds);

        if (result.ExitCode != 0)
        {
            LogPermissionCommandFailure("systemctl show", result);
            throw new RestorePermissionException(
                $"Не удалось проверить {subject} '{serviceName}' (код {result.ExitCode}). " +
                $"Physical restore не будет выполнять swap без однозначной связи systemd-сервиса с {processDescription}.");
        }

        var raw = result.Stdout.Trim();
        if (!int.TryParse(raw, out var mainPid) || mainPid != expectedPid)
            throw new RestorePermissionException(
                $"Небезопасно управлять {processDescription} через {subject} '{serviceName}': MainPID={raw}, ожидаемый PID={expectedPid}. " +
                "Physical restore не будет выполнять swap, если service manager не указывает ровно на этот инстанс.");
    }

    public async Task<ExternalProcessResult> RunForServiceAsync(
        string verb,
        string serviceName,
        CancellationToken ct,
        int timeoutSeconds) =>
        await RunAsync(verb, [serviceName], ct, timeoutSeconds);

    public async Task<ExternalProcessResult> RunAsync(
        string verb,
        IReadOnlyList<string> arguments,
        CancellationToken ct,
        int timeoutSeconds)
    {
        timeoutSeconds = Math.Max(timeoutSeconds, 1);
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));

        var request = new ExternalProcessRequest
        {
            FileName = "systemctl",
            Arguments = new[] { verb }.Concat(arguments).ToArray(),
            EnvironmentOverrides = new Dictionary<string, string?>
            {
                ["LC_MESSAGES"] = "C",
                ["LANG"] = "C",
            },
        };

        try
        {
            return await _processRunner.RunAsync(request, handleStdout: null, handleStdin: null, timeoutCts.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new InvalidOperationException(
                $"Команда 'systemctl {verb} {string.Join(" ", arguments)}' не завершилась за {timeoutSeconds} секунд - возможно, systemd не отвечает.");
        }
    }

    public static string FormatOutput(ExternalProcessResult result)
    {
        var output = string.IsNullOrWhiteSpace(result.Stderr) ? result.Stdout : result.Stderr;
        return string.IsNullOrWhiteSpace(output) ? string.Empty : $" Вывод: {Truncate(output.Trim(), 2000)}";
    }

    private void LogPermissionCommandFailure(string operation, ExternalProcessResult result)
    {
        _logger.LogWarning(
            "SystemdServiceController: {Operation} failed. ExitCode: {ExitCode}. Stdout: {Stdout}. Stderr: {Stderr}",
            operation,
            result.ExitCode,
            Truncate(result.Stdout.Trim(), 2000),
            Truncate(result.Stderr.Trim(), 2000));
    }

    private static string Truncate(string value, int maxLength) =>
        value.Length <= maxLength ? value : value[..maxLength] + "...";
}
