using System.Diagnostics;
using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Backup;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using Npgsql;

namespace BackupsterAgent.Providers.Backup;

public sealed class PostgresPhysicalBackupProvider : IBackupProvider
{
    private readonly ILogger<PostgresPhysicalBackupProvider> _logger;
    private readonly PostgresBinaryResolver _binaryResolver;
    private readonly IExternalProcessRunner _processRunner;

    public PostgresPhysicalBackupProvider(
        ILogger<PostgresPhysicalBackupProvider> logger,
        PostgresBinaryResolver binaryResolver,
        IExternalProcessRunner processRunner)
    {
        _logger = logger;
        _binaryResolver = binaryResolver;
        _processRunner = processRunner;
    }

    public async Task ValidatePermissionsAsync(ConnectionConfig connection, string database, CancellationToken ct)
    {
        var binary = await _binaryResolver.ResolveAsync(connection, "pg_basebackup", ct);
        await EnsureBinaryAvailableAsync(binary, ct);

        var connString = new NpgsqlConnectionStringBuilder
        {
            Host = connection.Host,
            Port = connection.Port,
            Username = connection.Username,
            Password = connection.Password,
            Database = "postgres",
            TcpKeepAlive = true,
            KeepAlive = 30,
        }.ToString();

        var (isSuperuser, hasReplication) = await PostgresQueryRetry.ExecuteAsync(
            _logger, "SELECT rolsuper, rolreplication", connection.Name,
            async innerCt =>
            {
                await using var conn = new NpgsqlConnection(connString);
                await conn.OpenAsync(innerCt);

                await using var cmd = new NpgsqlCommand(
                    "SELECT rolsuper, rolreplication FROM pg_roles WHERE rolname = current_user;", conn);
                await using var reader = await cmd.ExecuteReaderAsync(innerCt);

                if (!await reader.ReadAsync(innerCt))
                    throw new BackupPermissionException(
                        $"Пользователь '{connection.Username}' не найден в pg_roles — проверьте корректность credentials для подключения '{connection.Name}'.");

                return (reader.GetBoolean(0), reader.GetBoolean(1));
            }, ct);

        if (!isSuperuser && !hasReplication)
            throw new BackupPermissionException(
                $"Пользователь '{connection.Username}' подключения '{connection.Name}' не имеет прав для physical бэкапа. " +
                "Требуется привилегия REPLICATION или superuser. " +
                $"Выдайте права: ALTER ROLE \"{connection.Username}\" WITH REPLICATION;");

        var extraTablespaces = await PostgresQueryRetry.ExecuteAsync(
            _logger, "SELECT pg_tablespace", connection.Name,
            async innerCt =>
            {
                await using var conn = new NpgsqlConnection(connString);
                await conn.OpenAsync(innerCt);

                await using var cmd = new NpgsqlCommand(
                    "SELECT string_agg(spcname, ', ' ORDER BY spcname) " +
                    "FROM pg_tablespace WHERE spcname NOT IN ('pg_default', 'pg_global');", conn);
                return await cmd.ExecuteScalarAsync(innerCt) as string;
            }, ct);

        if (!string.IsNullOrEmpty(extraTablespaces))
            throw new BackupPermissionException(
                $"Кластер '{connection.Name}' использует tablespaces ({extraTablespaces}), " +
                "но physical-режим Backupster их не поддерживает: данные tablespace остались бы вне архива, " +
                "а restore развернул бы кластер с битыми ссылками на пустые каталоги. " +
                "Используйте logical-режим (BackupMode=Logical) — он не зависит от tablespaces.");

        var pgdata = await PostgresQueryRetry.ExecuteAsync(
            _logger, "SHOW data_directory", connection.Name,
            async innerCt =>
            {
                await using var conn = new NpgsqlConnection(connString);
                await conn.OpenAsync(innerCt);

                await using var cmd = new NpgsqlCommand("SHOW data_directory;", conn);
                return (string?)await cmd.ExecuteScalarAsync(innerCt) ?? string.Empty;
            }, ct);

        if (string.IsNullOrWhiteSpace(pgdata))
            throw new InvalidOperationException(
                $"Не удалось получить путь PGDATA из кластера '{connection.Name}'.");

        _logger.LogInformation("Resolved PGDATA from cluster: '{PgDataPath}'", pgdata);

        if (!Directory.Exists(pgdata))
            throw new InvalidOperationException(
                $"Каталог PGDATA '{pgdata}' недоступен на хосте агента. " +
                "Физический бэкап требует, чтобы агент и PostgreSQL выполнялись на одном хосте. " +
                "Если агент удалённый — используйте режим logical.");
    }

    public async Task<BackupResult> BackupAsync(DatabaseConfig config, ConnectionConfig connection, CancellationToken ct)
    {
        var binary = await _binaryResolver.ResolveAsync(connection, "pg_basebackup", ct);

        await WarnIfInsufficientWalSendersAsync(connection, ct);

        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        var fileName = $"{config.Database}_{timestamp}{PgBaseFormatDetector.ContainerExtension}";
        var manifestFileName = $"{config.Database}_{timestamp}.backup_manifest";
        var tempDir = Path.Combine(config.OutputPath, $"pgbase-{Guid.NewGuid():N}");
        var outputFile = Path.Combine(config.OutputPath, fileName);
        var manifestOutputFile = Path.Combine(config.OutputPath, manifestFileName);

        Directory.CreateDirectory(config.OutputPath);

        _logger.LogInformation(
            "Starting PostgreSQL physical backup (pg_basebackup --wal-method=stream). Host: '{Host}:{Port}', Output: '{OutputFile}', Binary: '{Binary}'",
            connection.Host, connection.Port, outputFile, binary);

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
                _logger.LogError("pg_basebackup failed. ExitCode: {ExitCode}. Stderr: {Stderr}",
                    result.ExitCode, stderr);
                throw new InvalidOperationException(
                    $"pg_basebackup завершился с кодом {result.ExitCode}: {stderr}");
            }

            var baseTar = Path.Combine(tempDir, "base.tar.gz");
            var pgWalTar = Path.Combine(tempDir, "pg_wal.tar.gz");

            if (!File.Exists(baseTar) || !File.Exists(pgWalTar))
            {
                var found = Directory.Exists(tempDir)
                    ? string.Join(", ", Directory.GetFiles(tempDir).Select(Path.GetFileName))
                    : "(directory missing)";
                throw new InvalidOperationException(
                    $"pg_basebackup не создал ожидаемые файлы 'base.tar.gz' и 'pg_wal.tar.gz'. Найдено: {found}");
            }

            await PgBaseContainer.WriteAsync(outputFile, baseTar, pgWalTar, ct);

            var manifestSource = Path.Combine(tempDir, "backup_manifest");
            string? manifestPath = null;
            if (File.Exists(manifestSource))
            {
                PgBaseContainer.MoveFileIntoPlace(manifestSource, manifestOutputFile);
                manifestPath = manifestOutputFile;
                _logger.LogInformation(
                    "PostgreSQL backup_manifest captured: '{ManifestPath}'", manifestOutputFile);
            }
            else
            {
                _logger.LogWarning(
                    "PostgreSQL backup_manifest not found in '{TempDir}' — incremental backups based on this full backup will not be possible.",
                    tempDir);
            }

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
                PgBaseManifestPath = manifestPath,
            };
        }
        finally
        {
            TryDeleteDirectory(tempDir);
        }
    }

    internal async Task WarnIfInsufficientWalSendersAsync(ConnectionConfig connection, CancellationToken ct)
    {
        try
        {
            var connString = new NpgsqlConnectionStringBuilder
            {
                Host = connection.Host,
                Port = connection.Port,
                Username = connection.Username,
                Password = connection.Password,
                Database = "postgres",
                TcpKeepAlive = true,
                KeepAlive = 30,
            }.ToString();

            var raw = await PostgresQueryRetry.ExecuteAsync(
                _logger, "SHOW max_wal_senders", connection.Name,
                async innerCt =>
                {
                    await using var conn = new NpgsqlConnection(connString);
                    await conn.OpenAsync(innerCt);
                    await using var cmd = new NpgsqlCommand("SHOW max_wal_senders;", conn);
                    return (string?)await cmd.ExecuteScalarAsync(innerCt) ?? string.Empty;
                }, ct);

            if (int.TryParse(raw, out var senders) && senders < 2)
                _logger.LogWarning(
                    "PostgreSQL max_wal_senders={Value} on '{Connection}'. " +
                    "Backupster requires --wal-method=stream which needs at least 2 wal senders " +
                    "(one for base, one for WAL). pg_basebackup is likely to fail. " +
                    "Set max_wal_senders >= 2 in postgresql.conf and reload.",
                    senders, connection.Name);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex,
                "Failed to probe max_wal_senders for '{Connection}' — proceeding with pg_basebackup, " +
                "any insufficient-senders condition will surface as a pg_basebackup error.",
                connection.Name);
        }
    }

    private async Task EnsureBinaryAvailableAsync(string binary, CancellationToken ct)
    {
        var request = new ExternalProcessRequest
        {
            FileName = binary,
            Arguments = new[] { "--version" },
            EnvironmentOverrides = new Dictionary<string, string?>
            {
                ["LC_MESSAGES"] = "C",
                ["LANG"] = "C",
            },
        };

        ExternalProcessResult result;
        try
        {
            result = await _processRunner.RunAsync(request, handleStdout: null, handleStdin: null, ct);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Бинарник {binary} недоступен на хосте агента. " +
                $"Установите пакет postgresql и убедитесь, что {binary} находится в PATH.", ex);
        }

        if (result.ExitCode != 0)
            throw new InvalidOperationException(
                $"{binary} --version вернул код {result.ExitCode}. " +
                $"Убедитесь, что пакет postgresql установлен и {binary} находится в PATH.");
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
