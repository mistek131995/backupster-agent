using System.Diagnostics;
using System.IO.Compression;
using System.Text;
using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using MongoDB.Driver;

namespace BackupsterAgent.Providers.Backup;

public sealed class MongoLogicalBackupProvider : IBackupProvider
{
    private readonly ILogger<MongoLogicalBackupProvider> _logger;
    private readonly MongoBinaryResolver _binaryResolver;
    private readonly IExternalProcessRunner _processRunner;

    public MongoLogicalBackupProvider(
        ILogger<MongoLogicalBackupProvider> logger,
        MongoBinaryResolver binaryResolver,
        IExternalProcessRunner processRunner)
    {
        _logger = logger;
        _binaryResolver = binaryResolver;
        _processRunner = processRunner;
    }

    public async Task ValidatePermissionsAsync(ConnectionConfig connection, string database, CancellationToken ct)
    {
        var mongodump = _binaryResolver.Resolve(connection, "mongodump");
        await EnsureBinaryAvailableAsync(mongodump, ct);

        var settings = BuildClientSettings(connection);
        var client = new MongoClient(settings);

        try
        {
            var db = client.GetDatabase(database);
            using var cursor = await db.ListCollectionNamesAsync(cancellationToken: ct);
            await cursor.MoveNextAsync(ct);
        }
        catch (MongoAuthenticationException)
        {
            throw new BackupPermissionException(
                $"Не удалось аутентифицироваться в MongoDB. " +
                $"Проверьте логин и пароль пользователя '{connection.Username}' подключения '{connection.Name}'.");
        }
        catch (MongoCommandException ex) when (ex.CodeName == "Unauthorized")
        {
            throw new BackupPermissionException(
                $"Пользователь '{connection.Username}' подключения '{connection.Name}' " +
                $"не имеет прав на чтение БД '{database}'. " +
                $"Выдайте роль: db.grantRolesToUser('{connection.Username}', " +
                $"[{{role: 'read', db: '{database}'}}])");
        }
    }

    public async Task<BackupResult> BackupAsync(DatabaseConfig config, ConnectionConfig connection, CancellationToken ct)
    {
        var mongodump = _binaryResolver.Resolve(connection, "mongodump");

        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        var fileName = $"{config.DatabasePathSegment}_{timestamp}.archive.gz";
        var outputFile = Path.Combine(config.OutputPath, fileName);

        Directory.CreateDirectory(config.OutputPath);

        _logger.LogInformation(
            "Starting MongoDB logical backup. Database: '{Database}', Host: '{Host}:{Port}', Output: '{OutputFile}', Binary: '{Binary}'",
            config.Database, connection.Host, connection.Port, outputFile, mongodump);

        var configDir = Path.Combine(Path.GetTempPath(), $"backupster-mongo-{Guid.NewGuid():N}");
        string? configPath = null;

        try
        {
            var uri = BuildMongoUri(connection);
            configPath = await WriteTempConfigAsync(uri, configDir, ct);

            var request = new ExternalProcessRequest
            {
                FileName = mongodump,
                Arguments = new[]
                {
                    $"--config={configPath}",
                    $"--db={config.Database}",
                    "--archive",
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
                _logger.LogError("mongodump failed. ExitCode: {ExitCode}. Stderr: {Stderr}", result.ExitCode, stderr);
                throw new InvalidOperationException($"mongodump завершился с кодом {result.ExitCode}: {stderr}");
            }

            var fileInfo = new FileInfo(outputFile);
            _logger.LogInformation(
                "MongoDB logical backup completed successfully. File: '{FilePath}', Size: {SizeBytes} bytes, Duration: {DurationMs} ms",
                outputFile, fileInfo.Length, sw.ElapsedMilliseconds);

            return new BackupResult
            {
                FilePath = outputFile,
                SizeBytes = fileInfo.Length,
                DurationMs = sw.ElapsedMilliseconds,
                Success = true,
            };
        }
        finally
        {
            TryDeleteDirectory(configDir);
        }
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
                "Установите пакет mongodb-database-tools и убедитесь, что mongodump находится в PATH " +
                "(или задайте ConnectionConfig.BinPath с каталогом бинарников MongoDB).", ex);
        }

        if (result.ExitCode != 0)
            throw new InvalidOperationException(
                $"{binary} --version вернул код {result.ExitCode}. " +
                "Убедитесь, что пакет mongodb-database-tools установлен и mongodump находится в PATH " +
                "(или задайте ConnectionConfig.BinPath).");
    }

    private static MongoClientSettings BuildClientSettings(ConnectionConfig connection)
    {
        var settings = new MongoClientSettings
        {
            Server = new MongoServerAddress(connection.Host, connection.Port),
            ConnectTimeout = TimeSpan.FromSeconds(10),
            ServerSelectionTimeout = TimeSpan.FromSeconds(10),
        };

        if (!string.IsNullOrEmpty(connection.Username))
        {
            settings.Credential = MongoCredential.CreateCredential(
                "admin", connection.Username, connection.Password);
        }

        return settings;
    }

    private static string BuildMongoUri(ConnectionConfig connection)
    {
        var host = $"{connection.Host}:{connection.Port}";
        if (string.IsNullOrEmpty(connection.Username))
            return $"mongodb://{host}/?authSource=admin";

        var user = Uri.EscapeDataString(connection.Username);
        var pass = Uri.EscapeDataString(connection.Password);
        return $"mongodb://{user}:{pass}@{host}/?authSource=admin";
    }

    private static async Task<string> WriteTempConfigAsync(string uri, string dir, CancellationToken ct)
    {
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, "config.yaml");
        var escaped = uri.Replace("'", "''");
        await File.WriteAllTextAsync(path, $"uri: '{escaped}'\n", Encoding.UTF8, ct);

        if (!OperatingSystem.IsWindows())
            File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);

        return path;
    }

    private void TryDeleteFile(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch (Exception ex) { _logger.LogWarning(ex, "Could not delete partial file '{Path}'", path); }
    }

    private void TryDeleteDirectory(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, true); }
        catch (Exception ex) { _logger.LogWarning(ex, "Could not delete temp directory '{Path}'", path); }
    }
}
