using System.Diagnostics;
using System.IO.Compression;
using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using MySqlConnector;

namespace BackupsterAgent.Providers.Backup;

public sealed class MysqlPhysicalBackupProvider : IBackupProvider
{
    private readonly ILogger<MysqlPhysicalBackupProvider> _logger;
    private readonly MysqlBinaryResolver _binaryResolver;
    private readonly IExternalProcessRunner _processRunner;

    public MysqlPhysicalBackupProvider(
        ILogger<MysqlPhysicalBackupProvider> logger,
        MysqlBinaryResolver binaryResolver,
        IExternalProcessRunner processRunner)
    {
        _logger = logger;
        _binaryResolver = binaryResolver;
        _processRunner = processRunner;
    }

    public async Task ValidatePermissionsAsync(ConnectionConfig connection, string database, CancellationToken ct)
    {
        EnsureSupportedOperatingSystem();

        var xtrabackup = _binaryResolver.Resolve(connection, "xtrabackup");
        await EnsureBinaryAvailableAsync(xtrabackup, ct);

        const string sql = @"
SELECT
  MAX(CASE WHEN PRIVILEGE_TYPE IN ('RELOAD',             'ALL PRIVILEGES') THEN 1 ELSE 0 END) AS has_reload,
  MAX(CASE WHEN PRIVILEGE_TYPE IN ('PROCESS',            'ALL PRIVILEGES') THEN 1 ELSE 0 END) AS has_process,
  MAX(CASE WHEN PRIVILEGE_TYPE IN ('REPLICATION CLIENT', 'ALL PRIVILEGES') THEN 1 ELSE 0 END) AS has_repl,
  MAX(CASE WHEN PRIVILEGE_TYPE IN ('BACKUP_ADMIN',       'ALL PRIVILEGES') THEN 1 ELSE 0 END) AS has_backup_admin
FROM information_schema.USER_PRIVILEGES
WHERE GRANTEE = CONCAT('''', SUBSTRING_INDEX(CURRENT_USER(), '@', 1), '''@''',
                        SUBSTRING_INDEX(CURRENT_USER(), '@', -1), '''');";

        await using var conn = new MySqlConnection(BuildConnectionString(connection));
        await conn.OpenAsync(ct);

        bool hasReload, hasProcess, hasRepl, hasBackupAdmin;
        await using (var cmd = new MySqlCommand(sql, conn))
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            if (!await reader.ReadAsync(ct))
                throw new BackupPermissionException("Не удалось прочитать данные о правах пользователя.");

            hasReload      = !reader.IsDBNull(0) && reader.GetInt32(0) == 1;
            hasProcess     = !reader.IsDBNull(1) && reader.GetInt32(1) == 1;
            hasRepl        = !reader.IsDBNull(2) && reader.GetInt32(2) == 1;
            hasBackupAdmin = !reader.IsDBNull(3) && reader.GetInt32(3) == 1;
        }

        var missing = new List<string>();
        if (!hasReload && !hasBackupAdmin)
            missing.Add("RELOAD (или BACKUP_ADMIN для MySQL 8.0.24+)");
        if (!hasProcess)
            missing.Add("PROCESS");
        if (!hasRepl)
            missing.Add("REPLICATION CLIENT");

        if (missing.Count > 0)
            throw new BackupPermissionException(
                $"Пользователь '{connection.Username}' подключения '{connection.Name}' не имеет привилегий для физического бэкапа: {string.Join(", ", missing)}. " +
                $"Выдайте права: GRANT RELOAD, PROCESS, REPLICATION CLIENT ON *.* TO '{connection.Username}'@'%'; FLUSH PRIVILEGES;");

        var datadir = await QueryDataDirectoryAsync(conn, ct);
        if (!Directory.Exists(datadir))
            throw new BackupPermissionException(
                $"Каталог данных MySQL '{datadir}' недоступен на хосте агента. " +
                "Физический бэкап через XtraBackup требует, чтобы агент и MySQL выполнялись на одном хосте. " +
                "Если агент удалённый — используйте режим logical.");
    }

    public async Task<BackupResult> BackupAsync(DatabaseConfig config, ConnectionConfig connection, CancellationToken ct)
    {
        EnsureSupportedOperatingSystem();

        var xtrabackup = _binaryResolver.Resolve(connection, "xtrabackup");

        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        var fileName = $"{config.DatabasePathSegment}_{timestamp}.xbstream.gz";
        var outputFile = Path.Combine(config.OutputPath, fileName);
        var tempDir = Path.Combine(config.OutputPath, $"xtra-{Guid.NewGuid():N}");

        Directory.CreateDirectory(config.OutputPath);
        Directory.CreateDirectory(tempDir);

        _logger.LogInformation(
            "Starting MySQL physical backup (XtraBackup). Database: '{Database}', Host: '{Host}:{Port}', Output: '{OutputFile}', Binary: '{Binary}'",
            config.Database, connection.Host, connection.Port, outputFile, xtrabackup);

        var request = new ExternalProcessRequest
        {
            FileName = xtrabackup,
            Arguments = new[]
            {
                "--backup",
                "--stream=xbstream",
                "--target-dir=" + tempDir,
                "--host=" + connection.Host,
                "--port=" + connection.Port.ToString(),
                "--user=" + connection.Username,
            },
            EnvironmentOverrides = new Dictionary<string, string?>
            {
                ["MYSQL_PWD"] = connection.Password,
            },
        };

        var sw = Stopwatch.StartNew();

        ExternalProcessResult result;
        try
        {
            result = await _processRunner.RunAsync(
                request,
                handleStdout: async (stdout, innerCt) =>
                {
                    await using var fileStream = new FileStream(outputFile, FileMode.Create, FileAccess.Write,
                        FileShare.None, bufferSize: 65536, useAsync: true);
                    await using var gzipStream = new GZipStream(fileStream, CompressionLevel.Optimal);
                    await stdout.CopyToAsync(gzipStream, innerCt);
                },
                handleStdin: null,
                ct);
        }
        catch
        {
            TryDeleteFile(outputFile);
            throw;
        }
        finally
        {
            TryDeleteDirectory(tempDir);
        }

        sw.Stop();

        if (result.ExitCode != 0)
        {
            TryDeleteFile(outputFile);
            var stderr = result.Stderr.Trim();
            _logger.LogError("xtrabackup failed. ExitCode: {ExitCode}. Stderr: {Stderr}", result.ExitCode, stderr);
            throw new InvalidOperationException($"xtrabackup завершился с кодом {result.ExitCode}: {stderr}");
        }

        var fileInfo = new FileInfo(outputFile);
        _logger.LogInformation(
            "MySQL physical backup completed successfully. File: '{FilePath}', Size: {SizeBytes} bytes, Duration: {DurationMs} ms",
            outputFile, fileInfo.Length, sw.ElapsedMilliseconds);

        return new BackupResult
        {
            FilePath = outputFile,
            SizeBytes = fileInfo.Length,
            DurationMs = sw.ElapsedMilliseconds,
            Success = true,
        };
    }

    private async Task EnsureBinaryAvailableAsync(string binary, CancellationToken ct)
    {
        var request = new ExternalProcessRequest
        {
            FileName = binary,
            Arguments = new[] { "--version" },
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
                "Установите пакет percona-xtrabackup и убедитесь, что xtrabackup находится в PATH " +
                "(или задайте ConnectionConfig.BinPath с каталогом бинарников XtraBackup).", ex);
        }

        if (result.ExitCode != 0)
            throw new InvalidOperationException(
                $"{binary} --version вернул код {result.ExitCode}. " +
                "Убедитесь, что percona-xtrabackup установлен и xtrabackup находится в PATH " +
                "(или задайте ConnectionConfig.BinPath).");
    }

    private static void EnsureSupportedOperatingSystem()
    {
        if (!OperatingSystem.IsLinux())
            throw new BackupPermissionException(
                "Физический бэкап MySQL через Percona XtraBackup поддерживается только на Linux. " +
                "На Windows используйте режим logical или запустите агента на Linux-хосте рядом с MySQL.");
    }

    private static async Task<string> QueryDataDirectoryAsync(MySqlConnection conn, CancellationToken ct)
    {
        await using var cmd = new MySqlCommand("SELECT @@datadir;", conn);
        var result = await cmd.ExecuteScalarAsync(ct);
        return result as string ?? string.Empty;
    }

    private static string BuildConnectionString(ConnectionConfig connection) =>
        new MySqlConnectionStringBuilder
        {
            Server = connection.Host,
            Port = (uint)connection.Port,
            UserID = connection.Username,
            Password = connection.Password,
        }.ToString();

    private void TryDeleteFile(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch (Exception ex) { _logger.LogWarning(ex, "Could not delete partial file '{Path}'", path); }
    }

    private void TryDeleteDirectory(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); }
        catch (Exception ex) { _logger.LogWarning(ex, "Could not delete temp directory '{Path}'", path); }
    }
}
