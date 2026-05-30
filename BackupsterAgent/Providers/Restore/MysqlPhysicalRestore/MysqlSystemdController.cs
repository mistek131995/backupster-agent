using BackupsterAgent.Configuration;
using BackupsterAgent.Services.Common.Processes;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

public sealed class MysqlSystemdController
{
    private readonly ILogger<MysqlSystemdController> _logger;
    private readonly IExternalProcessRunner _processRunner;
    private readonly RestoreSettings _restoreSettings;

    public MysqlSystemdController(
        ILogger<MysqlSystemdController> logger,
        IExternalProcessRunner processRunner,
        IOptions<RestoreSettings> restoreSettings)
    {
        _logger = logger;
        _processRunner = processRunner;
        _restoreSettings = restoreSettings.Value;
    }

    public async Task MaskAsync(string serviceName)
    {
        _logger.LogInformation("Masking MySQL service '{ServiceName}' to block systemd auto-restart", serviceName);

        var result = await RunSystemctlAsync("mask", serviceName, CancellationToken.None, _restoreSettings.SystemctlTimeoutSeconds);

        if (result.ExitCode != 0)
            throw new InvalidOperationException(
                $"Не удалось замаскировать MySQL-сервис '{serviceName}' (код {result.ExitCode}). " +
                "Восстановление прервано: без маскировки systemd может перезапустить MySQL во время подмены datadir." +
                (string.IsNullOrWhiteSpace(result.Stderr) ? "" : $" Вывод: {result.Stderr.Trim()}"));
    }

    public async Task TryUnmaskAsync(string serviceName)
    {
        try
        {
            var result = await RunSystemctlAsync("unmask", serviceName, CancellationToken.None, _restoreSettings.SystemctlTimeoutSeconds);

            if (result.ExitCode != 0)
                _logger.LogError(
                    "Failed to unmask MySQL service '{ServiceName}' (exit code {ExitCode}) — it may remain masked and require a manual 'systemctl unmask'.{Stderr}",
                    serviceName, result.ExitCode,
                    string.IsNullOrWhiteSpace(result.Stderr) ? "" : $" Output: {result.Stderr.Trim()}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to unmask MySQL service '{ServiceName}' — it may remain masked and require a manual 'systemctl unmask'.",
                serviceName);
        }
    }

    public async Task StopAsync(string serviceName, CancellationToken ct)
    {
        _logger.LogInformation("Stopping MySQL service '{ServiceName}'", serviceName);

        var result = await RunSystemctlAsync("stop", serviceName, ct, _restoreSettings.SystemctlStopStartTimeoutSeconds);

        if (result.ExitCode != 0)
            throw new InvalidOperationException(
                $"Не удалось остановить MySQL-сервис '{serviceName}' (код {result.ExitCode}). " +
                "Убедитесь, что агент имеет права на управление сервисом (запущен от root или настроен sudoers/polkit)." +
                (string.IsNullOrWhiteSpace(result.Stderr) ? "" : $" Вывод: {result.Stderr.Trim()}"));
    }

    public async Task StartAsync(string serviceName, CancellationToken ct)
    {
        _logger.LogInformation("Starting MySQL service '{ServiceName}'", serviceName);

        var result = await RunSystemctlAsync("start", serviceName, ct, _restoreSettings.SystemctlStopStartTimeoutSeconds);

        if (result.ExitCode != 0)
            throw new InvalidOperationException(
                $"Не удалось запустить MySQL-сервис '{serviceName}' (код {result.ExitCode}). " +
                "Убедитесь, что агент имеет права на управление сервисом." +
                (string.IsNullOrWhiteSpace(result.Stderr) ? "" : $" Вывод: {result.Stderr.Trim()}"));
    }

    public async Task<bool> IsActiveAsync(string serviceName, CancellationToken ct)
    {
        try
        {
            var result = await RunSystemctlAsync("is-active", serviceName, ct, _restoreSettings.SystemctlTimeoutSeconds);
            return result.ExitCode == 0;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to check if service '{ServiceName}' is running — assuming running", serviceName);
            return true;
        }
    }

    private async Task<ExternalProcessResult> RunSystemctlAsync(string verb, string serviceName, CancellationToken ct, int timeoutSeconds)
    {
        timeoutSeconds = Math.Max(timeoutSeconds, 1);
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));

        var request = new ExternalProcessRequest
        {
            FileName = "systemctl",
            Arguments = new[] { verb, serviceName },
        };

        try
        {
            return await _processRunner.RunAsync(request, handleStdout: null, handleStdin: null, timeoutCts.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new InvalidOperationException(
                $"Команда 'systemctl {verb} {serviceName}' не завершилась за {timeoutSeconds} секунд — возможно, systemd не отвечает.");
        }
    }
}
