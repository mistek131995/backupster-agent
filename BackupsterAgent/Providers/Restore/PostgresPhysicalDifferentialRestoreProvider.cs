using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;

namespace BackupsterAgent.Providers.Restore;

public sealed class PostgresPhysicalDifferentialRestoreProvider : IDifferentialRestoreProvider
{
    private const int MinimumSupportedMajorVersion = 17;

    private readonly ILogger<PostgresPhysicalDifferentialRestoreProvider> _logger;
    private readonly PostgresPhysicalRestoreProvider _fullProvider;
    private readonly PostgresBinaryResolver _binaryResolver;
    private readonly IExternalProcessRunner _processRunner;

    public PostgresPhysicalDifferentialRestoreProvider(
        ILogger<PostgresPhysicalDifferentialRestoreProvider> logger,
        PostgresPhysicalRestoreProvider fullProvider,
        PostgresBinaryResolver binaryResolver,
        IExternalProcessRunner processRunner)
    {
        _logger = logger;
        _fullProvider = fullProvider;
        _binaryResolver = binaryResolver;
        _processRunner = processRunner;
    }

    public async Task ValidatePermissionsAsync(ConnectionConfig connection, string targetDatabase, CancellationToken ct)
    {
        await EnsureMinimumVersionAsync(connection, ct);
        await _fullProvider.ValidatePermissionsAsync(connection, targetDatabase, ct);
    }

    public async Task ValidateRestoreSourceAsync(
        ConnectionConfig connection,
        IReadOnlyList<DifferentialRestoreChainItem> chain,
        CancellationToken ct)
    {
        if (chain.Count == 0)
            throw new InvalidOperationException("Цепочка восстановления пуста.");

        if (chain[0].BackupMode != BackupMode.Physical)
            throw new InvalidOperationException(
                $"Первый элемент цепочки должен быть полным бэкапом (Physical), получен '{chain[0].BackupMode}'.");

        for (var i = 1; i < chain.Count; i++)
        {
            if (chain[i].BackupMode != BackupMode.PhysicalDifferential)
                throw new InvalidOperationException(
                    $"Элемент цепочки #{i} должен быть дифференциальным бэкапом, получен '{chain[i].BackupMode}'.");
        }

        foreach (var item in chain)
        {
            if (!File.Exists(item.DumpFilePath))
                throw new InvalidOperationException(
                    $"Файл дампа цепочки '{item.DumpFilePath}' (recordId={item.BackupRecordId}) недоступен на хосте агента.");

            if (string.IsNullOrWhiteSpace(item.PgBaseManifestFilePath))
                throw new InvalidOperationException(
                    $"Невозможно восстановить цепочку: у бэкапа recordId={item.BackupRecordId} отсутствует backup_manifest. " +
                    "Вероятно, бэкап создан старой версией агента без сохранения backup_manifest. " +
                    "Сделайте новый полный (Physical) бэкап и пересоберите цепочку.");

            if (!File.Exists(item.PgBaseManifestFilePath))
                throw new InvalidOperationException(
                    $"Файл backup_manifest цепочки '{item.PgBaseManifestFilePath}' (recordId={item.BackupRecordId}) недоступен на хосте агента.");

            await _fullProvider.ValidateRestoreSourceAsync(connection, item.DumpFilePath, ct);
        }
    }

    public Task PrepareTargetDatabaseAsync(ConnectionConfig connection, string targetDatabase, CancellationToken ct) =>
        Task.CompletedTask;

    public async Task RestoreAsync(
        ConnectionConfig connection,
        string targetDatabase,
        IReadOnlyList<DifferentialRestoreChainItem> chain,
        CancellationToken ct)
    {
        await EnsureMinimumVersionAsync(connection, ct);

        var pgCombineBinary = await _binaryResolver.ResolveAsync(connection, "pg_combinebackup", ct);

        _logger.LogInformation(
            "PostgreSQL differential restore starting. Target: '{Target}', Chain length: {Length}, pg_combinebackup: '{Binary}'",
            targetDatabase, chain.Count, pgCombineBinary);

        await _fullProvider.ExecuteRestoreAsync(connection, async (stagingPath, populateCt) =>
        {
            var combineWorkDir = stagingPath + ".combine";

            try
            {
                Directory.CreateDirectory(combineWorkDir);
                PostgresPhysicalRestoreProvider.WriteMarkerFile(combineWorkDir);

                var extractedDirs = new List<string>(chain.Count);
                for (var i = 0; i < chain.Count; i++)
                {
                    if (populateCt.IsCancellationRequested) populateCt.ThrowIfCancellationRequested();

                    var item = chain[i];
                    var extractDir = Path.Combine(combineWorkDir, $"chain-{i}");
                    Directory.CreateDirectory(extractDir);

                    _logger.LogInformation(
                        "Extracting chain item #{Index} (recordId={RecordId}, mode={Mode}) to '{ExtractDir}'",
                        i, item.BackupRecordId, item.BackupMode, extractDir);

                    await _fullProvider.ExtractDumpAsync(item.DumpFilePath, extractDir, populateCt);

                    var manifestDest = Path.Combine(extractDir, "backup_manifest");
                    File.Copy(item.PgBaseManifestFilePath!, manifestDest, overwrite: true);

                    _logger.LogInformation(
                        "Placed backup_manifest for chain item #{Index} at '{ManifestPath}'",
                        i, manifestDest);

                    extractedDirs.Add(extractDir);
                }

                if (Directory.Exists(stagingPath))
                    Directory.Delete(stagingPath, recursive: true);

                _logger.LogInformation(
                    "Running pg_combinebackup → '{Output}' from {Count} chain dir(s)", stagingPath, extractedDirs.Count);

                await RunPgCombineBackupAsync(pgCombineBinary, stagingPath, extractedDirs, populateCt);

                if (!Directory.Exists(stagingPath))
                    throw new InvalidOperationException(
                        $"pg_combinebackup завершился без ошибок, но каталог '{stagingPath}' отсутствует.");

                PostgresPhysicalRestoreProvider.WriteMarkerFile(stagingPath);

                _logger.LogInformation(
                    "Combined cluster materialized at staging '{StagingPath}'", stagingPath);
            }
            finally
            {
                TryDeleteDirectory(combineWorkDir);
            }
        }, ct);

        _logger.LogInformation(
            "PostgreSQL differential restore completed. Target: '{Target}', Chain length: {Length}",
            targetDatabase, chain.Count);
    }

    private async Task RunPgCombineBackupAsync(
        string binary,
        string outputDir,
        IReadOnlyList<string> sourceDirs,
        CancellationToken ct)
    {
        var args = new List<string> { "-o", outputDir };
        foreach (var dir in sourceDirs) args.Add(dir);

        var request = new ExternalProcessRequest
        {
            FileName = binary,
            Arguments = args.ToArray(),
            EnvironmentOverrides = new Dictionary<string, string?>
            {
                ["LC_MESSAGES"] = "C",
                ["LANG"] = "C",
            },
        };

        var result = await _processRunner.RunAsync(request, handleStdout: null, handleStdin: null, ct);

        if (result.ExitCode != 0)
        {
            var stderr = result.Stderr.Trim();
            _logger.LogError(
                "pg_combinebackup failed. ExitCode: {ExitCode}. Stderr: {Stderr}",
                result.ExitCode, stderr);
            throw new InvalidOperationException(
                $"pg_combinebackup завершился с кодом {result.ExitCode}: {stderr}");
        }
    }

    private async Task EnsureMinimumVersionAsync(ConnectionConfig connection, CancellationToken ct)
    {
        var major = await _binaryResolver.GetMajorVersionAsync(connection, ct);
        if (major < MinimumSupportedMajorVersion)
            throw new RestorePermissionException(
                $"Восстановление цепочки дифференциальных бэкапов PostgreSQL требует версии {MinimumSupportedMajorVersion}+, " +
                $"но кластер '{connection.Name}' работает на версии {major}. " +
                "Обновите PostgreSQL или восстановите БД из последнего полного бэкапа.");
    }

    private void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete directory '{Path}'", path);
        }
    }
}
