using System.Diagnostics;
using System.Text;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using Npgsql;

namespace BackupsterAgent.Providers.Restore;

public sealed class PostgresPhysicalRestoreProvider : IRestoreProvider
{
    private readonly ILogger<PostgresPhysicalRestoreProvider> _logger;

    private string? _pgDataPath;

    public PostgresPhysicalRestoreProvider(ILogger<PostgresPhysicalRestoreProvider> logger)
    {
        _logger = logger;
    }

    public async Task ValidatePermissionsAsync(ConnectionConfig connection, string targetDatabase, CancellationToken ct)
    {
        await CheckPgCtlAsync(ct);

        _pgDataPath = await QueryDataDirectoryAsync(connection, ct);
        _logger.LogInformation("Resolved PGDATA from cluster: '{PgDataPath}'", _pgDataPath);

        if (!Directory.Exists(_pgDataPath))
            throw new RestorePermissionException(
                $"Каталог PGDATA '{_pgDataPath}' недоступен на хосте агента. " +
                "Физическое восстановление требует, чтобы агент и PostgreSQL выполнялись на одном хосте.");
    }

    public async Task PrepareTargetDatabaseAsync(ConnectionConfig connection, string targetDatabase, bool replaceExisting, CancellationToken ct)
    {
        var pgDataPath = _pgDataPath!;

        _logger.LogInformation("Stopping PostgreSQL cluster at '{PgDataPath}'", pgDataPath);
        await RunPgCtlAsync(["stop", "-D", pgDataPath, "-m", "fast", "-w"], ct);
        _logger.LogInformation("PostgreSQL cluster stopped");

        _logger.LogInformation("Clearing PGDATA directory '{PgDataPath}'", pgDataPath);
        ClearDirectory(pgDataPath);
    }

    public async Task RestoreAsync(ConnectionConfig connection, string targetDatabase, string restoreFilePath, CancellationToken ct)
    {
        var pgDataPath = _pgDataPath!;

        _logger.LogInformation(
            "Extracting base archive '{ArchivePath}' to '{PgDataPath}'",
            restoreFilePath, pgDataPath);
        await ExtractTarGzAsync(restoreFilePath, pgDataPath, ct);
        _logger.LogInformation("Archive extracted successfully");

        _logger.LogInformation("Starting PostgreSQL cluster at '{PgDataPath}'", pgDataPath);
        await RunPgCtlAsync(["start", "-D", pgDataPath, "-w"], ct);
        _logger.LogInformation("PostgreSQL cluster started");
    }

    private async Task CheckPgCtlAsync(CancellationToken ct)
    {
        var psi = new ProcessStartInfo
        {
            FileName = "pg_ctl",
            ArgumentList = { "--version" },
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        using var process = new Process { StartInfo = psi };
        try
        {
            process.Start();
            await process.WaitForExitAsync(ct);
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            throw new RestorePermissionException(
                $"pg_ctl не найден на хосте агента ({ex.Message}). " +
                "Установите postgresql и убедитесь, что pg_ctl есть в PATH.");
        }

        if (process.ExitCode != 0)
            throw new RestorePermissionException(
                $"pg_ctl --version вернул код {process.ExitCode}. " +
                "Убедитесь, что postgresql установлен и pg_ctl есть в PATH.");
    }

    private async Task<string> QueryDataDirectoryAsync(ConnectionConfig connection, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(BuildConnectionString(connection));
        await conn.OpenAsync(ct);

        await using var cmd = new NpgsqlCommand("SHOW data_directory;", conn);
        var result = await cmd.ExecuteScalarAsync(ct);

        if (result is not string path || string.IsNullOrWhiteSpace(path))
            throw new RestorePermissionException(
                $"Не удалось получить путь PGDATA из кластера '{connection.Name}'.");

        return path;
    }

    private async Task RunPgCtlAsync(string[] args, CancellationToken ct)
    {
        var psi = new ProcessStartInfo
        {
            FileName = "pg_ctl",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        foreach (var arg in args)
            psi.ArgumentList.Add(arg);

        using var process = new Process { StartInfo = psi };
        process.Start();

        using var reg = ct.Register(() =>
        {
            try { process.Kill(entireProcessTree: true); }
            catch (Exception ex) { _logger.LogWarning(ex, "Failed to kill pg_ctl process"); }
        });

        var stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
        var stderrTask = process.StandardError.ReadToEndAsync(ct);

        await Task.WhenAll(stdoutTask, stderrTask);
        await process.WaitForExitAsync(ct);

        var stdout = stdoutTask.Result.Trim();
        var stderr = stderrTask.Result.Trim();

        if (stdout.Length > 0)
            _logger.LogDebug("pg_ctl stdout: {Output}", stdout);

        if (process.ExitCode != 0)
        {
            var detail = string.IsNullOrEmpty(stderr) ? stdout : stderr;
            throw new InvalidOperationException($"pg_ctl exited with code {process.ExitCode}: {detail}");
        }
    }

    private async Task ExtractTarGzAsync(string archivePath, string targetDir, CancellationToken ct)
    {
        var psi = new ProcessStartInfo
        {
            FileName = "tar",
            ArgumentList = { "-xzf", archivePath, "-C", targetDir },
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        using var process = new Process { StartInfo = psi };
        process.Start();
        _logger.LogInformation("tar process started (PID {Pid})", process.Id);

        using var reg = ct.Register(() =>
        {
            try { process.Kill(entireProcessTree: true); }
            catch (Exception ex) { _logger.LogWarning(ex, "Failed to kill tar process"); }
        });

        var stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
        var stderrTask = process.StandardError.ReadToEndAsync(ct);

        await Task.WhenAll(stdoutTask, stderrTask);
        await process.WaitForExitAsync(ct);

        if (process.ExitCode != 0)
        {
            var stderr = stderrTask.Result.Trim();
            var stdout = stdoutTask.Result.Trim();
            var detail = string.IsNullOrEmpty(stderr) ? stdout : stderr;
            throw new InvalidOperationException($"tar extraction failed (exit code {process.ExitCode}): {detail}");
        }
    }

    private static void ClearDirectory(string path)
    {
        foreach (var file in Directory.GetFiles(path))
            File.Delete(file);
        foreach (var dir in Directory.GetDirectories(path))
            Directory.Delete(dir, recursive: true);
    }

    private static string BuildConnectionString(ConnectionConfig connection) =>
        new NpgsqlConnectionStringBuilder
        {
            Host = connection.Host,
            Port = connection.Port,
            Username = connection.Username,
            Password = connection.Password,
            Database = "postgres",
        }.ToString();
}
