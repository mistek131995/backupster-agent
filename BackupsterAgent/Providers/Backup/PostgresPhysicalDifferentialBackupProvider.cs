using System.Diagnostics;
using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Backup;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;

namespace BackupsterAgent.Providers.Backup;

public sealed class PostgresPhysicalDifferentialBackupProvider : IDifferentialBackupProvider
{
    private const int MinimumSupportedMajorVersion = 17;

    private readonly ILogger<PostgresPhysicalDifferentialBackupProvider> _logger;
    private readonly PostgresBinaryResolver _binaryResolver;
    private readonly IExternalProcessRunner _processRunner;
    private readonly PostgresPhysicalBackupProvider _fullProvider;

    public PostgresPhysicalDifferentialBackupProvider(
        ILogger<PostgresPhysicalDifferentialBackupProvider> logger,
        PostgresBinaryResolver binaryResolver,
        IExternalProcessRunner processRunner,
        PostgresPhysicalBackupProvider fullProvider)
    {
        _logger = logger;
        _binaryResolver = binaryResolver;
        _processRunner = processRunner;
        _fullProvider = fullProvider;
    }

    public async Task ValidatePermissionsAsync(ConnectionConfig connection, string database, CancellationToken ct)
    {
        await EnsureMinimumVersionAsync(connection, ct);
        await _fullProvider.ValidatePermissionsAsync(connection, database, ct);
    }

    public async Task<BackupResult> BackupAsync(
        DatabaseConfig config,
        ConnectionConfig connection,
        DifferentialBackupContext context,
        CancellationToken ct)
    {
        await EnsureMinimumVersionAsync(connection, ct);

        if (string.IsNullOrWhiteSpace(context.BasePgBaseManifestPath))
            throw new InvalidOperationException(
                "Дифференциальный бэкап PostgreSQL требует backup_manifest от родительского бэкапа, " +
                "но дашборд не передал путь к нему. Возможно, родительский полный бэкап был создан " +
                "старой версией агента без сохранения backup_manifest.");

        if (!File.Exists(context.BasePgBaseManifestPath))
            throw new InvalidOperationException(
                $"Файл backup_manifest от родительского бэкапа недоступен по пути '{context.BasePgBaseManifestPath}'. " +
                "Возможно, файл повреждён или удалён.");

        var binary = await _binaryResolver.ResolveAsync(connection, "pg_basebackup", ct);

        await _fullProvider.WarnIfInsufficientWalSendersAsync(connection, ct);

        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        var fileName = $"{config.Database}_{timestamp}_diff{PgBaseFormatDetector.ContainerExtension}";
        var manifestFileName = $"{config.Database}_{timestamp}_diff.backup_manifest";
        var tempDir = Path.Combine(config.OutputPath, $"pgbase-diff-{Guid.NewGuid():N}");
        var outputFile = Path.Combine(config.OutputPath, fileName);
        var manifestOutputFile = Path.Combine(config.OutputPath, manifestFileName);

        Directory.CreateDirectory(config.OutputPath);

        _logger.LogInformation(
            "Starting PostgreSQL differential backup (pg_basebackup --incremental --wal-method=stream). Host: '{Host}:{Port}', Output: '{OutputFile}', BaseManifest: '{BaseManifest}', Binary: '{Binary}'",
            connection.Host, connection.Port, outputFile, context.BasePgBaseManifestPath, binary);

        var request = new ExternalProcessRequest
        {
            FileName = binary,
            Arguments = new[]
            {
                "-h", connection.Host,
                "-p", connection.Port.ToString(),
                "-U", connection.Username,
                "--format=tar",
                "--wal-method=stream",
                "--checkpoint=fast",
                "--gzip",
                $"--incremental={context.BasePgBaseManifestPath}",
                "-D", tempDir,
            },
            EnvironmentOverrides = new Dictionary<string, string?>
            {
                ["PGPASSWORD"] = connection.Password,
                ["LC_MESSAGES"] = "C",
                ["LANG"] = "C",
            },
        };

        var sw = Stopwatch.StartNew();

        try
        {
            var result = await _processRunner.RunAsync(request, handleStdout: null, handleStdin: null, ct);
            sw.Stop();

            var stderr = result.Stderr.Trim();

            if (result.ExitCode != 0)
            {
                _logger.LogError(
                    "pg_basebackup --incremental failed. ExitCode: {ExitCode}. Stderr: {Stderr}",
                    result.ExitCode, stderr);
                throw new InvalidOperationException(
                    $"pg_basebackup --incremental завершился с кодом {result.ExitCode}: {stderr}");
            }

            var baseTar = Path.Combine(tempDir, "base.tar.gz");
            var pgWalTar = Path.Combine(tempDir, "pg_wal.tar.gz");

            if (!File.Exists(baseTar) || !File.Exists(pgWalTar))
            {
                var found = Directory.Exists(tempDir)
                    ? string.Join(", ", Directory.GetFiles(tempDir).Select(Path.GetFileName))
                    : "(directory missing)";
                throw new InvalidOperationException(
                    $"pg_basebackup --incremental не создал ожидаемые файлы 'base.tar.gz' и 'pg_wal.tar.gz'. Найдено: {found}");
            }

            await PgBaseContainer.WriteAsync(outputFile, baseTar, pgWalTar, ct);

            var manifestSource = Path.Combine(tempDir, "backup_manifest");
            string? manifestPath = null;
            if (File.Exists(manifestSource))
            {
                PgBaseContainer.MoveFileIntoPlace(manifestSource, manifestOutputFile);
                manifestPath = manifestOutputFile;
                _logger.LogInformation(
                    "PostgreSQL differential backup_manifest captured: '{ManifestPath}'", manifestOutputFile);
            }
            else
            {
                _logger.LogWarning(
                    "PostgreSQL differential backup_manifest not found in '{TempDir}' — future incrementals based on this backup will not be possible.",
                    tempDir);
            }

            var sizeBytes = new FileInfo(outputFile).Length;
            _logger.LogInformation(
                "PostgreSQL differential backup completed. File: '{FilePath}', Size: {SizeBytes} bytes, Duration: {DurationMs} ms",
                outputFile, sizeBytes, sw.ElapsedMilliseconds);

            return new BackupResult
            {
                FilePath = outputFile,
                SizeBytes = sizeBytes,
                DurationMs = sw.ElapsedMilliseconds,
                Success = true,
                PgBaseManifestPath = manifestPath,
            };
        }
        finally
        {
            TryDeleteDirectory(tempDir);
        }
    }

    private async Task EnsureMinimumVersionAsync(ConnectionConfig connection, CancellationToken ct)
    {
        var major = await _binaryResolver.GetMajorVersionAsync(connection, ct);
        if (major < MinimumSupportedMajorVersion)
            throw new BackupPermissionException(
                $"Дифференциальный бэкап PostgreSQL требует версии {MinimumSupportedMajorVersion}+, " +
                $"но кластер '{connection.Name}' работает на версии {major}. " +
                "Используйте полный (physical) бэкап или обновите PostgreSQL.");
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
