using System.Diagnostics;
using System.IO.Compression;
using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using Npgsql;

namespace BackupsterAgent.Providers.Backup;

public sealed class PostgresLogicalBackupProvider : IBackupProvider
{
    private readonly ILogger<PostgresLogicalBackupProvider> _logger;
    private readonly PostgresBinaryResolver _binaryResolver;
    private readonly IExternalProcessRunner _processRunner;

    public PostgresLogicalBackupProvider(
        ILogger<PostgresLogicalBackupProvider> logger,
        PostgresBinaryResolver binaryResolver,
        IExternalProcessRunner processRunner)
    {
        _logger = logger;
        _binaryResolver = binaryResolver;
        _processRunner = processRunner;
    }

    public async Task ValidatePermissionsAsync(ConnectionConfig connection, string database, CancellationToken ct)
    {
        var binary = await _binaryResolver.ResolveAsync(connection, "pg_dump", ct);
        await EnsureBinaryAvailableAsync(binary, ct);

        const string sql = @"
SELECT rolsuper,
       has_database_privilege(current_user, current_database(), 'CONNECT') AS can_connect,
       has_schema_privilege(current_user, 'public', 'USAGE') AS can_use_public
FROM pg_roles WHERE rolname = current_user;";

        var connString = new NpgsqlConnectionStringBuilder
        {
            Host = connection.Host,
            Port = connection.Port,
            Username = connection.Username,
            Password = connection.Password,
            Database = database,
        }.ToString();

        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync(ct);

        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);

        if (!await reader.ReadAsync(ct))
            throw new BackupPermissionException(
                $"Пользователь '{connection.Username}' не найден в pg_roles — проверьте корректность credentials для подключения '{connection.Name}'.");

        var isSuperuser  = reader.GetBoolean(0);
        var canConnect   = reader.GetBoolean(1);
        var canUsePublic = reader.GetBoolean(2);

        if (!isSuperuser && !(canConnect && canUsePublic))
            throw new BackupPermissionException(
                $"Пользователь '{connection.Username}' подключения '{connection.Name}' не имеет прав для бэкапа БД '{database}'. " +
                "Требуется superuser, либо CONNECT на БД и USAGE на схему public. " +
                $"Выдайте права: GRANT CONNECT ON DATABASE \"{database}\" TO \"{connection.Username}\"; " +
                $"GRANT USAGE ON SCHEMA public TO \"{connection.Username}\";");
    }

    public async Task<BackupResult> BackupAsync(DatabaseConfig config, ConnectionConfig connection, CancellationToken ct)
    {
        var binary = await _binaryResolver.ResolveAsync(connection, "pg_dump", ct);

        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        var fileName = $"{config.Database}_{timestamp}.sql.gz";
        var outputFile = Path.Combine(config.OutputPath, fileName);

        Directory.CreateDirectory(config.OutputPath);

        _logger.LogInformation(
            "Starting PostgreSQL logical backup. Database: '{Database}', Host: '{Host}:{Port}', Output: '{OutputFile}', Binary: '{Binary}'",
            config.Database, connection.Host, connection.Port, outputFile, binary);

        var request = new ExternalProcessRequest
        {
            FileName = binary,
            Arguments = new[]
            {
                "-h", connection.Host,
                "-p", connection.Port.ToString(),
                "-U", connection.Username,
                "-F", "p",
                "--clean",
                "--if-exists",
                config.Database,
            },
            EnvironmentOverrides = new Dictionary<string, string?>
            {
                ["PGPASSWORD"] = connection.Password,
                ["LC_MESSAGES"] = "C",
                ["LANG"] = "C",
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
            _logger.LogError("pg_dump failed. ExitCode: {ExitCode}. Stderr: {Stderr}", result.ExitCode, stderr);
            throw new InvalidOperationException($"pg_dump завершился с кодом {result.ExitCode}: {stderr}");
        }

        var fileInfo = new FileInfo(outputFile);
        _logger.LogInformation(
            "PostgreSQL logical backup completed successfully. File: '{FilePath}', Size: {SizeBytes} bytes, Duration: {DurationMs} ms",
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
                $"Установите пакет postgresql-client и убедитесь, что {binary} находится в PATH.", ex);
        }

        if (result.ExitCode != 0)
            throw new InvalidOperationException(
                $"{binary} --version вернул код {result.ExitCode}. " +
                $"Убедитесь, что пакет postgresql-client установлен и {binary} находится в PATH.");
    }

    private void TryDeleteFile(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch (Exception ex) { _logger.LogWarning(ex, "Could not delete partial file '{Path}'", path); }
    }
}
