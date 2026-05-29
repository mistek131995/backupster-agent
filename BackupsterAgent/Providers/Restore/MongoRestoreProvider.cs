using System.Text;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using MongoDB.Bson;
using MongoDB.Driver;

namespace BackupsterAgent.Providers.Restore;

public sealed class MongoRestoreProvider : IRestoreProvider
{
    private readonly ILogger<MongoRestoreProvider> _logger;
    private readonly MongoBinaryResolver _binaryResolver;
    private readonly IExternalProcessRunner _processRunner;

    public MongoRestoreProvider(
        ILogger<MongoRestoreProvider> logger,
        MongoBinaryResolver binaryResolver,
        IExternalProcessRunner processRunner)
    {
        _logger = logger;
        _binaryResolver = binaryResolver;
        _processRunner = processRunner;
    }

    public async Task ValidatePermissionsAsync(ConnectionConfig connection, string targetDatabase, CancellationToken ct)
    {
        var mongorestore = _binaryResolver.Resolve(connection, "mongorestore");
        await EnsureBinaryAvailableAsync(mongorestore, "mongorestore", ct);

        var settings = BuildClientSettings(connection);
        var client = new MongoClient(settings);

        try
        {
            var db = client.GetDatabase(targetDatabase);
            const string probeCollection = "__backupster_restore_check";
            var collection = db.GetCollection<BsonDocument>(probeCollection);
            await collection.InsertOneAsync(new BsonDocument(), cancellationToken: ct);
            await db.DropCollectionAsync(probeCollection, ct);
        }
        catch (MongoAuthenticationException)
        {
            throw new RestorePermissionException(
                $"Не удалось аутентифицироваться в MongoDB. " +
                $"Проверьте логин и пароль пользователя '{connection.Username}' подключения '{connection.Name}'.");
        }
        catch (MongoCommandException ex) when (ex.CodeName == "Unauthorized")
        {
            throw new RestorePermissionException(
                $"Пользователь '{connection.Username}' подключения '{connection.Name}' " +
                $"не имеет прав для восстановления БД '{targetDatabase}'. " +
                $"Выдайте роль: use admin; db.grantRolesToUser('{connection.Username}', " +
                $"[{{role: 'dbOwner', db: '{targetDatabase}'}}])");
        }
    }

    public Task ValidateRestoreSourceAsync(ConnectionConfig connection, string restoreFilePath, CancellationToken ct)
    {
        if (!File.Exists(restoreFilePath))
            throw new InvalidOperationException(
                $"Файл бэкапа '{restoreFilePath}' не найден после распаковки. Файл повреждён или хранилище недоступно.");

        var info = new FileInfo(restoreFilePath);
        if (info.Length == 0)
            throw new InvalidOperationException(
                $"Файл бэкапа '{restoreFilePath}' пустой. Создайте новый бэкап и повторите восстановление.");

        return Task.CompletedTask;
    }

    public async Task PrepareTargetDatabaseAsync(ConnectionConfig connection, string targetDatabase, CancellationToken ct)
    {
        var settings = BuildClientSettings(connection);
        var client = new MongoClient(settings);

        try
        {
            await client.DropDatabaseAsync(targetDatabase, ct);
        }
        catch (MongoCommandException ex) when (ex.CodeName == "Unauthorized")
        {
            throw new RestorePermissionException(
                $"Пользователь '{connection.Username}' подключения '{connection.Name}' " +
                $"не имеет прав на удаление БД '{targetDatabase}'. " +
                $"Для восстановления требуется роль dbOwner: " +
                $"use admin; db.grantRolesToUser('{connection.Username}', " +
                $"[{{role: 'dbOwner', db: '{targetDatabase}'}}])");
        }

        _logger.LogInformation("MongoDB target database '{Database}' prepared (dropped)", targetDatabase);
    }

    public async Task RestoreAsync(ConnectionConfig connection, string targetDatabase, string sourceDatabaseName, string restoreFilePath, CancellationToken ct)
    {
        var mongorestore = _binaryResolver.Resolve(connection, "mongorestore");

        var configDir = Path.Combine(Path.GetTempPath(), $"backupster-mongo-{Guid.NewGuid():N}");

        try
        {
            var uri = BuildMongoUri(connection);
            var configPath = await WriteTempConfigAsync(uri, configDir, ct);

            var args = new List<string>
            {
                $"--config={configPath}",
                "--archive",
                "--drop",
            };

            if (!string.Equals(sourceDatabaseName, targetDatabase, StringComparison.Ordinal))
            {
                args.Add($"--nsFrom={sourceDatabaseName}.*");
                args.Add($"--nsTo={targetDatabase}.*");
            }
            else
            {
                args.Add($"--db={targetDatabase}");
            }

            var request = new ExternalProcessRequest
            {
                FileName = mongorestore,
                Arguments = args.ToArray(),
                RedirectStandardInput = true,
            };

            var result = await _processRunner.RunAsync(
                request,
                handleStdout: null,
                handleStdin: async (stdin, innerCt) =>
                {
                    await using var source = new FileStream(
                        restoreFilePath, FileMode.Open, FileAccess.Read, FileShare.Read,
                        bufferSize: 65536, useAsync: true);
                    await source.CopyToAsync(stdin.BaseStream, innerCt);
                },
                ct);

            if (result.ExitCode != 0)
            {
                var stderr = result.Stderr.Trim();
                var stdout = result.Stdout.Trim();
                var detail = string.IsNullOrEmpty(stderr) ? stdout : stderr;
                throw new InvalidOperationException($"mongorestore завершился с ошибкой (код {result.ExitCode}): {detail}");
            }

            _logger.LogInformation("MongoDB restore completed for database '{Database}'", targetDatabase);
        }
        finally
        {
            TryDeleteDirectory(configDir);
        }
    }

    private async Task EnsureBinaryAvailableAsync(string binary, string name, CancellationToken ct)
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
            throw new RestorePermissionException(
                $"Бинарник {name} недоступен на хосте агента ({ex.Message}). " +
                "Установите MongoDB Database Tools и убедитесь, что mongorestore находится в PATH " +
                "(или задайте ConnectionConfig.BinPath).");
        }

        if (result.ExitCode != 0)
            throw new RestorePermissionException(
                $"{binary} --version вернул код {result.ExitCode}. " +
                "Убедитесь, что MongoDB Database Tools установлены и " + name + " находится в PATH " +
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

    private void TryDeleteDirectory(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, true); }
        catch (Exception ex) { _logger.LogWarning(ex, "Could not delete temp directory '{Path}'", path); }
    }
}
