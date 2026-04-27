using System.Diagnostics;
using System.IO.Compression;
using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using MySqlConnector;

namespace BackupsterAgent.Providers.Backup;

public sealed class MysqlLogicalBackupProvider : IBackupProvider
{
    private readonly ILogger<MysqlLogicalBackupProvider> _logger;
    private readonly MysqlBinaryResolver _binaryResolver;
    private readonly IExternalProcessRunner _processRunner;

    public MysqlLogicalBackupProvider(
        ILogger<MysqlLogicalBackupProvider> logger,
        MysqlBinaryResolver binaryResolver,
        IExternalProcessRunner processRunner)
    {
        _logger = logger;
        _binaryResolver = binaryResolver;
        _processRunner = processRunner;
    }

    public async Task ValidatePermissionsAsync(ConnectionConfig connection, string database, CancellationToken ct)
    {
        var mysqldump = _binaryResolver.Resolve(connection, "mysqldump");
        await EnsureBinaryAvailableAsync(mysqldump, ct);

        const string globalSql = @"
SELECT
  MAX(CASE WHEN PRIVILEGE_TYPE IN ('SELECT', 'ALL PRIVILEGES') THEN 1 ELSE 0 END) AS has_select,
  MAX(CASE WHEN PRIVILEGE_TYPE IN ('PROCESS', 'ALL PRIVILEGES') THEN 1 ELSE 0 END) AS has_process
FROM information_schema.USER_PRIVILEGES;";

        const string schemaSql = @"
SELECT
  MAX(CASE WHEN PRIVILEGE_TYPE IN ('SELECT', 'ALL PRIVILEGES') THEN 1 ELSE 0 END) AS has_select
FROM information_schema.SCHEMA_PRIVILEGES
WHERE TABLE_SCHEMA = @db;";

        await using var conn = new MySqlConnection(BuildConnectionString(connection));
        await conn.OpenAsync(ct);

        bool globalSelect, globalProcess;
        await using (var cmd = new MySqlCommand(globalSql, conn))
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            if (!await reader.ReadAsync(ct))
                throw new BackupPermissionException("Не удалось прочитать данные о правах пользователя.");
            globalSelect  = reader.GetInt32(0) == 1;
            globalProcess = reader.GetInt32(1) == 1;
        }

        var hasSelect = globalSelect;
        if (!hasSelect)
        {
            await using var cmd2 = new MySqlCommand(schemaSql, conn);
            cmd2.Parameters.AddWithValue("@db", database);
            await using var reader2 = await cmd2.ExecuteReaderAsync(ct);
            hasSelect = await reader2.ReadAsync(ct) && reader2.GetInt32(0) == 1;
        }

        if (!hasSelect)
            throw new BackupPermissionException(
                $"Пользователь '{connection.Username}' подключения '{connection.Name}' не имеет права SELECT на БД '{database}'. " +
                $"Выдайте права: GRANT SELECT ON `{database}`.* TO '{connection.Username}'@'%'; FLUSH PRIVILEGES;");

        if (!globalProcess)
            throw new BackupPermissionException(
                $"Пользователь '{connection.Username}' подключения '{connection.Name}' не имеет глобальной привилегии PROCESS, " +
                "необходимой для --single-transaction (mysqldump). " +
                $"Выдайте права: GRANT PROCESS ON *.* TO '{connection.Username}'@'%'; FLUSH PRIVILEGES;");
    }

    public async Task<BackupResult> BackupAsync(DatabaseConfig config, ConnectionConfig connection, CancellationToken ct)
    {
        var mysqldump = _binaryResolver.Resolve(connection, "mysqldump");

        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        var fileName = $"{config.Database}_{timestamp}.sql.gz";
        var outputFile = Path.Combine(config.OutputPath, fileName);

        Directory.CreateDirectory(config.OutputPath);

        _logger.LogInformation(
            "Starting MySQL logical backup. Database: '{Database}', Host: '{Host}:{Port}', Output: '{OutputFile}', Binary: '{Binary}'",
            config.Database, connection.Host, connection.Port, outputFile, mysqldump);

        var request = new ExternalProcessRequest
        {
            FileName = mysqldump,
            Arguments = new[]
            {
                "-h", connection.Host,
                "-P", connection.Port.ToString(),
                "-u", connection.Username,
                "--single-transaction",
                "--quick",
                "--routines",
                "--triggers",
                "--events",
                "--hex-blob",
                "--set-gtid-purged=OFF",
                "--default-character-set=utf8mb4",
                config.Database,
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

        sw.Stop();

        if (result.ExitCode != 0)
        {
            TryDeleteFile(outputFile);
            var stderr = result.Stderr.Trim();
            _logger.LogError("mysqldump failed. ExitCode: {ExitCode}. Stderr: {Stderr}", result.ExitCode, stderr);
            throw new InvalidOperationException($"mysqldump завершился с кодом {result.ExitCode}: {stderr}");
        }

        var fileInfo = new FileInfo(outputFile);
        _logger.LogInformation(
            "MySQL logical backup completed successfully. File: '{FilePath}', Size: {SizeBytes} bytes, Duration: {DurationMs} ms",
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
                "Установите пакет mysql-client и убедитесь, что mysqldump находится в PATH " +
                "(или задайте ConnectionConfig.BinPath с каталогом клиентских бинарников MySQL).", ex);
        }

        if (result.ExitCode != 0)
            throw new InvalidOperationException(
                $"{binary} --version вернул код {result.ExitCode}. " +
                "Убедитесь, что пакет mysql-client установлен и mysqldump находится в PATH " +
                "(или задайте ConnectionConfig.BinPath).");
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
}
