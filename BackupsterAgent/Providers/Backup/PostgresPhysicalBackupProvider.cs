using System.Diagnostics;
using System.Text;
using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;

namespace BackupsterAgent.Providers.Backup;

public sealed class PostgresPhysicalBackupProvider : IBackupProvider
{
    private readonly ILogger<PostgresPhysicalBackupProvider> _logger;

    public PostgresPhysicalBackupProvider(ILogger<PostgresPhysicalBackupProvider> logger)
    {
        _logger = logger;
    }

    public async Task<BackupResult> BackupAsync(DatabaseConfig config, ConnectionConfig connection, CancellationToken ct)
    {
        await PreflightCheckAsync(ct);

        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        var fileName = $"{config.Database}_{timestamp}.tar.gz";
        var tempDir = Path.Combine(config.OutputPath, $"pgbase-{Guid.NewGuid():N}");
        var outputFile = Path.Combine(config.OutputPath, fileName);

        Directory.CreateDirectory(config.OutputPath);

        _logger.LogInformation(
            "Starting PostgreSQL physical backup (pg_basebackup). Host: '{Host}:{Port}', Output: '{OutputFile}'",
            connection.Host, connection.Port, outputFile);

        var psi = new ProcessStartInfo
        {
            FileName = "pg_basebackup",
            ArgumentList =
            {
                "-h", connection.Host,
                "-p", connection.Port.ToString(),
                "-U", connection.Username,
                "--format=tar",
                "--wal-method=fetch",
                "--checkpoint=fast",
                "--gzip",
                "-D", tempDir,
            },
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        psi.Environment["PGPASSWORD"] = connection.Password;

        var sw = Stopwatch.StartNew();
        using var process = new Process { StartInfo = psi };

        try
        {
            process.Start();
            _logger.LogInformation("pg_basebackup process started (PID {Pid})", process.Id);

            using var reg = ct.Register(() =>
            {
                try { process.Kill(entireProcessTree: true); }
                catch (Exception ex) { _logger.LogWarning(ex, "Failed to kill pg_basebackup process"); }
            });

            var stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
            var stderrTask = process.StandardError.ReadToEndAsync(ct);

            await Task.WhenAll(stdoutTask, stderrTask);
            await process.WaitForExitAsync(ct);
            sw.Stop();

            var stderr = stderrTask.Result.Trim();

            if (process.ExitCode != 0)
            {
                _logger.LogError("pg_basebackup failed. ExitCode: {ExitCode}. Stderr: {Stderr}",
                    process.ExitCode, stderr);
                throw new InvalidOperationException(
                    $"pg_basebackup exited with code {process.ExitCode}: {stderr}");
            }

            var baseTar = Path.Combine(tempDir, "base.tar.gz");
            if (!File.Exists(baseTar))
            {
                var found = Directory.Exists(tempDir)
                    ? string.Join(", ", Directory.GetFiles(tempDir).Select(Path.GetFileName))
                    : "(directory missing)";
                throw new InvalidOperationException(
                    $"pg_basebackup did not produce expected file 'base.tar.gz'. Found: {found}");
            }

            File.Move(baseTar, outputFile, overwrite: true);

            var sizeBytes = new FileInfo(outputFile).Length;
            _logger.LogInformation(
                "PostgreSQL physical backup completed. File: '{FilePath}', Size: {SizeBytes} bytes, Duration: {DurationMs} ms",
                outputFile, sizeBytes, sw.ElapsedMilliseconds);

            return new BackupResult
            {
                FilePath = outputFile,
                SizeBytes = sizeBytes,
                DurationMs = sw.ElapsedMilliseconds,
                Success = true,
            };
        }
        finally
        {
            TryDeleteDirectory(tempDir);
        }
    }

    private async Task PreflightCheckAsync(CancellationToken ct)
    {
        var psi = new ProcessStartInfo
        {
            FileName = "pg_basebackup",
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
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                "pg_basebackup is not available on this host. " +
                "Install the postgresql-client package and ensure pg_basebackup is in PATH.", ex);
        }

        if (process.ExitCode != 0)
            throw new InvalidOperationException(
                $"pg_basebackup --version returned exit code {process.ExitCode}. " +
                "Ensure the postgresql-client package is installed and the binary is in PATH.");
    }

    private void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete temp directory '{Path}'", path);
        }
    }
}
