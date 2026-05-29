using System.Diagnostics;

namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

public sealed class MysqlSystemdController
{
    private readonly ILogger<MysqlSystemdController> _logger;

    public MysqlSystemdController(ILogger<MysqlSystemdController> logger)
    {
        _logger = logger;
    }

    public async Task MaskAsync(string serviceName, CancellationToken ct)
    {
        _logger.LogInformation("Masking MySQL service '{ServiceName}' to block systemd auto-restart", serviceName);

        var psi = new ProcessStartInfo
        {
            FileName = "systemctl",
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        psi.ArgumentList.Add("mask");
        psi.ArgumentList.Add(serviceName);

        using var process = new Process { StartInfo = psi };
        process.Start();
        var stderrTask = process.StandardError.ReadToEndAsync(ct);
        var stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
        await process.WaitForExitAsync(ct);
        var stderr = await stderrTask;
        await stdoutTask;

        if (process.ExitCode != 0)
            throw new InvalidOperationException(
                $"Не удалось замаскировать MySQL-сервис '{serviceName}' (код {process.ExitCode}). " +
                "Восстановление прервано: без маскировки systemd может перезапустить MySQL во время подмены datadir." +
                (string.IsNullOrWhiteSpace(stderr) ? "" : $" Вывод: {stderr.Trim()}"));
    }

    public async Task TryUnmaskAsync(string serviceName)
    {
        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = "systemctl",
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            psi.ArgumentList.Add("unmask");
            psi.ArgumentList.Add(serviceName);

            using var process = new Process { StartInfo = psi };
            process.Start();
            var stderrTask = process.StandardError.ReadToEndAsync();
            var stdoutTask = process.StandardOutput.ReadToEndAsync();
            await process.WaitForExitAsync();
            await stderrTask;
            await stdoutTask;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to unmask MySQL service '{ServiceName}'", serviceName);
        }
    }

    public async Task StopAsync(string serviceName, CancellationToken ct)
    {
        _logger.LogInformation("Stopping MySQL service '{ServiceName}'", serviceName);

        var psi = new ProcessStartInfo
        {
            FileName = "systemctl",
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        psi.ArgumentList.Add("stop");
        psi.ArgumentList.Add(serviceName);

        using var process = new Process { StartInfo = psi };
        process.Start();
        var stderrTask = process.StandardError.ReadToEndAsync(ct);
        var stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
        await process.WaitForExitAsync(ct);
        var stderr = await stderrTask;
        await stdoutTask;

        if (process.ExitCode != 0)
            throw new InvalidOperationException(
                $"Не удалось остановить MySQL-сервис '{serviceName}' (код {process.ExitCode}). " +
                "Убедитесь, что агент имеет права на управление сервисом (запущен от root или настроен sudoers/polkit)." +
                (string.IsNullOrWhiteSpace(stderr) ? "" : $" Вывод: {stderr.Trim()}"));
    }

    public async Task StartAsync(string serviceName, CancellationToken ct)
    {
        _logger.LogInformation("Starting MySQL service '{ServiceName}'", serviceName);

        var psi = new ProcessStartInfo
        {
            FileName = "systemctl",
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        psi.ArgumentList.Add("start");
        psi.ArgumentList.Add(serviceName);

        using var process = new Process { StartInfo = psi };
        process.Start();
        var stderrTask = process.StandardError.ReadToEndAsync(ct);
        var stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
        await process.WaitForExitAsync(ct);
        var stderr = await stderrTask;
        await stdoutTask;

        if (process.ExitCode != 0)
            throw new InvalidOperationException(
                $"Не удалось запустить MySQL-сервис '{serviceName}' (код {process.ExitCode}). " +
                "Убедитесь, что агент имеет права на управление сервисом." +
                (string.IsNullOrWhiteSpace(stderr) ? "" : $" Вывод: {stderr.Trim()}"));
    }

    public async Task<bool> IsActiveAsync(string serviceName, CancellationToken ct)
    {
        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = "systemctl",
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            psi.ArgumentList.Add("is-active");
            psi.ArgumentList.Add(serviceName);

            using var process = new Process { StartInfo = psi };
            process.Start();
            await process.StandardOutput.ReadToEndAsync(ct);
            await process.WaitForExitAsync(ct);

            return process.ExitCode == 0;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to check if service '{ServiceName}' is running — assuming running", serviceName);
            return true;
        }
    }
}
