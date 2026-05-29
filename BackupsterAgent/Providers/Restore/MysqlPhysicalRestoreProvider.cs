using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

namespace BackupsterAgent.Providers.Restore;

public sealed class MysqlPhysicalRestoreProvider : IRestoreProvider
{
    private readonly ILogger<MysqlPhysicalRestoreProvider> _logger;
    private readonly MysqlBinaryResolver _binaryResolver;
    private readonly MysqlServerProbe _probe;
    private readonly MysqlBackupExtractor _extractor;
    private readonly MysqlInstanceInspector _inspector;
    private readonly MysqlLifecycleManager _lifecycle;
    private readonly MysqlDatadirSwapper _swapper;
    private readonly MysqlDatadirSwapCoordinator _coordinator;

    public MysqlPhysicalRestoreProvider(
        ILogger<MysqlPhysicalRestoreProvider> logger,
        MysqlBinaryResolver binaryResolver,
        MysqlServerProbe probe,
        MysqlBackupExtractor extractor,
        MysqlInstanceInspector inspector,
        MysqlLifecycleManager lifecycle,
        MysqlDatadirSwapper swapper,
        MysqlDatadirSwapCoordinator coordinator)
    {
        _logger = logger;
        _binaryResolver = binaryResolver;
        _probe = probe;
        _extractor = extractor;
        _inspector = inspector;
        _lifecycle = lifecycle;
        _swapper = swapper;
        _coordinator = coordinator;
    }

    public async Task ValidatePermissionsAsync(ConnectionConfig connection, string targetDatabase, CancellationToken ct)
    {
        MysqlPlatformGuard.EnsureSupportedOperatingSystem();

        var xtrabackup = _binaryResolver.Resolve(connection, "xtrabackup");
        await _extractor.EnsureBinaryAvailableAsync(xtrabackup, "xtrabackup", ct);

        var xbstream = _binaryResolver.Resolve(connection, "xbstream");
        await _extractor.EnsureBinaryAvailableAsync(xbstream, "xbstream", ct);

        var datadir = await _probe.QueryDataDirectoryAsync(connection, ct);
        _logger.LogInformation("Resolved MySQL datadir: '{DataDir}'", datadir);

        if (!Directory.Exists(datadir))
            throw new RestorePermissionException(
                $"Каталог данных MySQL '{datadir}' недоступен на хосте агента. " +
                "Физическое восстановление через XtraBackup требует, чтобы агент и MySQL выполнялись на одном хосте.");

        var realDatadir = _swapper.ResolveRealPath(datadir);
        if (!string.Equals(realDatadir, datadir, StringComparison.Ordinal))
            _logger.LogInformation(
                "MySQL datadir '{DataDir}' resolves to real path '{RealPath}'",
                datadir, realDatadir);

        var (parent, _) = MysqlDatadirSwapper.SplitPath(realDatadir);
        _swapper.EnsureSameFsRename(parent, realDatadir);

        var serviceName = await _inspector.DetectServiceNameAsync(connection, ct);
        if (serviceName is not null)
        {
            _logger.LogInformation(
                "MySQL is managed by service '{ServiceName}', will use service management for restore",
                serviceName);
        }
        else
        {
            await _probe.EnsureShutdownPrivilegeAsync(connection, ct);
            _lifecycle.ResolveMysqld(connection);
        }
    }

    public Task ValidateRestoreSourceAsync(ConnectionConfig connection, string restoreFilePath, CancellationToken ct)
    {
        if (!File.Exists(restoreFilePath))
            throw new InvalidOperationException(
                $"Файл бэкапа '{restoreFilePath}' не найден на хосте агента.");
        return Task.CompletedTask;
    }

    public Task PrepareTargetDatabaseAsync(ConnectionConfig connection, string targetDatabase, CancellationToken ct) =>
        Task.CompletedTask;

    public async Task RestoreAsync(ConnectionConfig connection, string targetDatabase, string sourceDatabaseName, string restoreFilePath, CancellationToken ct)
    {
        MysqlPlatformGuard.EnsureSupportedOperatingSystem();

        var xtrabackup = _binaryResolver.Resolve(connection, "xtrabackup");
        var xbstream = _binaryResolver.Resolve(connection, "xbstream");
        var datadir = await _probe.QueryDataDirectoryAsync(connection, ct);

        if (!Directory.Exists(datadir))
            throw new RestorePermissionException(
                $"Каталог данных MySQL '{datadir}' недоступен на хосте агента.");

        var realDatadir = _swapper.ResolveRealPath(datadir);
        var (parent, leaf) = MysqlDatadirSwapper.SplitPath(realDatadir);

        _swapper.CleanupOrphanStagingDirs(parent, leaf);

        var guid = Guid.NewGuid().ToString("N")[..8];
        var stagingPath = Path.Combine(parent, $"{leaf}.new-{guid}");
        var oldPath = Path.Combine(parent, $"{leaf}.old-{guid}");
        var failedPath = Path.Combine(parent, $"{leaf}.failed-{guid}");

        Directory.CreateDirectory(stagingPath);
        MysqlDatadirSwapper.WriteMarkerFile(stagingPath);

        MysqlInstanceInfo instanceInfo;
        try
        {
            _logger.LogInformation("Extracting xbstream archive to staging '{StagingPath}'", stagingPath);
            await _extractor.ExtractXbstreamAsync(xbstream, restoreFilePath, stagingPath, ct);

            _logger.LogInformation("Running xtrabackup --prepare on '{StagingPath}'", stagingPath);
            await _extractor.PrepareBackupAsync(xtrabackup, stagingPath, ct);

            _swapper.EnsureSameFsRename(parent, realDatadir);

            instanceInfo = await _inspector.DetectInstanceInfoAsync(connection, realDatadir, ct);

            _logger.LogInformation("Stopping MySQL to swap datadir");
            await _lifecycle.StopMysqlAsync(connection, instanceInfo, ct);
        }
        catch
        {
            _swapper.TryDeleteDirectory(stagingPath);
            throw;
        }

        var context = new MysqlDatadirSwapContext(
            connection, realDatadir, stagingPath, oldPath, failedPath, instanceInfo);
        await _coordinator.SwapAsync(context, ct);
    }
}
