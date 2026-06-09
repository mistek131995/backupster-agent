using System.Diagnostics;
using System.Text.Json;
using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Backup;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using Npgsql;

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
        await EnsureWalSummarizationEnabledAsync(connection, ct);
        await _fullProvider.ValidatePermissionsAsync(connection, database, ct);
    }

    public async Task<BackupResult> BackupAsync(
        DatabaseConfig config,
        ConnectionConfig connection,
        DifferentialBackupContext context,
        CancellationToken ct)
    {
        await EnsureMinimumVersionAsync(connection, ct);
        await EnsureWalSummarizationEnabledAsync(connection, ct);

        if (string.IsNullOrWhiteSpace(context.BasePgBaseManifestPath))
            throw new InvalidOperationException(
                "Дифференциальный бэкап PostgreSQL требует backup_manifest от родительского бэкапа, " +
                "но дашборд не передал путь к нему. Возможно, родительский полный бэкап был создан " +
                "старой версией агента без сохранения backup_manifest.");

        if (!File.Exists(context.BasePgBaseManifestPath))
            throw new InvalidOperationException(
                $"Файл backup_manifest от родительского бэкапа недоступен по пути '{context.BasePgBaseManifestPath}'. " +
                "Возможно, файл повреждён или удалён.");

        await EnsureBaseManifestIsReachableFromCurrentClusterAsync(
            connection, context.BasePgBaseManifestPath, ct);

        var binary = await _binaryResolver.ResolveAsync(connection, "pg_basebackup", ct);

        await _fullProvider.WarnIfInsufficientWalSendersAsync(connection, ct);

        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        var fileName = $"{config.DatabasePathSegment}_{timestamp}_diff{PgBaseFormatDetector.ContainerExtension}";
        var manifestFileName = $"{config.DatabasePathSegment}_{timestamp}_diff.backup_manifest";
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

        var completed = false;

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
                throw BuildPgBasebackupFailure(result.ExitCode, stderr);
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

            var unsupportedArchives = PgBaseContainer.FindUnsupportedPgBasebackupArchives(tempDir);
            if (unsupportedArchives.Length > 0)
                throw new InvalidOperationException(
                    $"pg_basebackup --incremental создал дополнительные tar-архивы ({string.Join(", ", unsupportedArchives)}). " +
                    "Дифференциальный physical-режим Backupster не поддерживает tablespaces: эти файлы не могут быть безопасно восстановлены. " +
                    "Уберите tablespaces или используйте logical-режим (BackupMode=Logical).");

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

            var backupResult = new BackupResult
            {
                FilePath = outputFile,
                SizeBytes = sizeBytes,
                DurationMs = sw.ElapsedMilliseconds,
                Success = true,
                PgBaseManifestPath = manifestPath,
            };

            completed = true;
            return backupResult;
        }
        finally
        {
            if (!completed)
            {
                TryDeleteFile(outputFile);
                TryDeleteFile(manifestOutputFile);
            }

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

    private async Task EnsureWalSummarizationEnabledAsync(ConnectionConfig connection, CancellationToken ct)
    {
        var raw = await PostgresQueryRetry.ExecuteAsync(
            _logger, "SHOW summarize_wal", connection.Name,
            async innerCt =>
            {
                await using var conn = new NpgsqlConnection(PostgresConnectionFactory.BuildAdminConnectionString(connection));
                await conn.OpenAsync(innerCt);
                await using var cmd = new NpgsqlCommand("SHOW summarize_wal;", conn);
                return (string?)await cmd.ExecuteScalarAsync(innerCt) ?? string.Empty;
            }, ct);

        if (!string.Equals(raw, "on", StringComparison.OrdinalIgnoreCase))
            throw new BackupPermissionException(
                $"Дифференциальный бэкап PostgreSQL требует включенного WAL summarizer на подключении '{connection.Name}' (summarize_wal=on). " +
                "Выполните от имени администратора PostgreSQL: ALTER SYSTEM SET summarize_wal = 'on'; SELECT pg_reload_conf(); " +
                "После включения дождитесь появления WAL summaries и повторите бэкап.");
    }

    private static Exception BuildPgBasebackupFailure(int exitCode, string stderr)
    {
        if (IsWalSummaryFailure(stderr))
            return new DifferentialChainBrokenException(
                "Дифференциальная цепочка PostgreSQL сломана: текущий кластер не может продолжить инкрементальный бэкап от родительского backup_manifest. " +
                "Серверу не хватает WAL summaries для диапазона между родительским бэкапом и текущим бэкапом. " +
                "Запускаем новый полный бэкап автоматически, чтобы восстановить цепочку. " +
                $"Ошибка pg_basebackup: {stderr}");

        return new InvalidOperationException(
            $"pg_basebackup --incremental завершился с кодом {exitCode}: {stderr}");
    }

    internal static bool IsWalSummaryFailure(string stderr) =>
        stderr.Contains("WAL summaries", StringComparison.OrdinalIgnoreCase)
        || stderr.Contains("wal summaries", StringComparison.OrdinalIgnoreCase)
        || stderr.Contains("wal summar", StringComparison.OrdinalIgnoreCase);

    private async Task EnsureBaseManifestIsReachableFromCurrentClusterAsync(
        ConnectionConfig connection,
        string baseManifestPath,
        CancellationToken ct)
    {
        var required = ReadRequiredWalPosition(baseManifestPath);
        var current = await GetCurrentWalPositionAsync(connection, ct);

        if (current.Timeline == required.Timeline && ComparePgLsn(current.Lsn, required.EndLsn) >= 0)
            return;

        throw new DifferentialChainBrokenException(
            $"Дифференциальная цепочка PostgreSQL сломана: текущая WAL-позиция кластера (timeline={current.Timeline}, lsn={current.Lsn}) не продолжает родительский backup_manifest (timeline={required.Timeline}, endLsn={required.EndLsn}). " +
            "Скорее всего, БД была восстановлена в более старую точку или сменилась timeline. " +
            "Запускаем новый полный бэкап автоматически, чтобы восстановить цепочку.");
    }

    private async Task<PgWalPosition> GetCurrentWalPositionAsync(ConnectionConfig connection, CancellationToken ct)
    {
        return await PostgresQueryRetry.ExecuteAsync(
            _logger, "SELECT current PostgreSQL WAL position", connection.Name,
            async innerCt =>
            {
                await using var conn = new NpgsqlConnection(PostgresConnectionFactory.BuildAdminConnectionString(connection));
                await conn.OpenAsync(innerCt);
                await using var cmd = new NpgsqlCommand("""
                    WITH current_position AS (
                        SELECT CASE
                            WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()
                            ELSE pg_current_wal_lsn()
                        END AS lsn
                    )
                    SELECT lsn::text, pg_walfile_name(lsn)
                    FROM current_position;
                    """, conn);

                await using var reader = await cmd.ExecuteReaderAsync(innerCt);
                if (!await reader.ReadAsync(innerCt))
                    throw new InvalidOperationException(
                        $"PostgreSQL '{connection.Name}' не вернул текущую WAL-позицию.");

                if (reader.IsDBNull(0) || reader.IsDBNull(1))
                    throw new InvalidOperationException(
                        $"PostgreSQL '{connection.Name}' вернул пустую текущую WAL-позицию.");

                var lsn = reader.GetString(0);
                var walFileName = reader.GetString(1);
                return new PgWalPosition(ParseWalFileTimeline(walFileName), lsn);
            }, ct);
    }

    internal static PgManifestWalRequirement ReadRequiredWalPosition(string manifestPath)
    {
        using var document = JsonDocument.Parse(File.ReadAllBytes(manifestPath));
        if (!document.RootElement.TryGetProperty("WAL-Ranges", out var ranges)
            || ranges.ValueKind != JsonValueKind.Array)
            throw new InvalidOperationException(
                $"backup_manifest '{manifestPath}' не содержит секцию WAL-Ranges.");

        PgManifestWalRequirement? latest = null;
        ulong latestEndLsnValue = 0;

        foreach (var range in ranges.EnumerateArray())
        {
            if (!range.TryGetProperty("Timeline", out var timelineElement)
                || !range.TryGetProperty("End-LSN", out var endLsnElement))
                continue;

            var endLsn = endLsnElement.GetString();
            if (string.IsNullOrWhiteSpace(endLsn))
                continue;

            var value = ParsePgLsn(endLsn);
            if (latest is not null && value <= latestEndLsnValue)
                continue;

            latest = new PgManifestWalRequirement(timelineElement.GetInt64(), endLsn);
            latestEndLsnValue = value;
        }

        return latest ?? throw new InvalidOperationException(
            $"backup_manifest '{manifestPath}' не содержит конечную WAL-позицию в секции WAL-Ranges.");
    }

    internal static string ReadRequiredWalEndLsn(string manifestPath)
        => ReadRequiredWalPosition(manifestPath).EndLsn;

    internal static long ParseWalFileTimeline(string walFileName)
    {
        if (walFileName.Length < 8)
            throw new FormatException($"Некорректное имя WAL-файла PostgreSQL '{walFileName}'.");

        return Convert.ToInt64(walFileName[..8], 16);
    }

    internal readonly record struct PgManifestWalRequirement(long Timeline, string EndLsn);

    private readonly record struct PgWalPosition(long Timeline, string Lsn);

    internal static int ComparePgLsn(string left, string right) =>
        ParsePgLsn(left).CompareTo(ParsePgLsn(right));

    internal static ulong ParsePgLsn(string value)
    {
        var parts = value.Split('/');
        if (parts.Length != 2)
            throw new FormatException($"Некорректный PostgreSQL LSN '{value}'.");

        return (Convert.ToUInt64(parts[0], 16) << 32) + Convert.ToUInt64(parts[1], 16);
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

    private void TryDeleteFile(string path)
    {
        try
        {
            if (File.Exists(path)) File.Delete(path);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete backup artifact '{Path}'", path);
        }
    }
}
