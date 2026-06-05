using BackupsterAgent.Configuration;

namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

public sealed class MysqlDatadirSwapCoordinator
{
    private readonly ILogger<MysqlDatadirSwapCoordinator> _logger;
    private readonly IMysqlDatadirSwapper _swapper;
    private readonly IMysqlLifecycleManager _lifecycle;
    private readonly IMysqlServerProbe _probe;

    public MysqlDatadirSwapCoordinator(
        ILogger<MysqlDatadirSwapCoordinator> logger,
        IMysqlDatadirSwapper swapper,
        IMysqlLifecycleManager lifecycle,
        IMysqlServerProbe probe)
    {
        _logger = logger;
        _swapper = swapper;
        _lifecycle = lifecycle;
        _probe = probe;
    }

    public async Task SwapAsync(MysqlDatadirSwapContext context, CancellationToken ct)
    {
        var connection = context.Connection;
        var realDatadir = context.RealDatadir;
        var stagingPath = context.StagingPath;
        var oldPath = context.OldPath;
        var failedPath = context.FailedPath;
        var instanceInfo = context.InstanceInfo;

        try
        {
            try
            {
                _swapper.MoveDirectory(realDatadir, oldPath);
                _swapper.MoveDirectory(stagingPath, realDatadir);

                await _swapper.FixOwnershipAsync(realDatadir, instanceInfo, ct);

                _logger.LogInformation("Starting MySQL after restore");
                await _lifecycle.StartMysqlAsync(connection, realDatadir, instanceInfo, ct);

                _logger.LogInformation("MySQL started successfully after physical restore");
                _swapper.TryDeleteDirectory(oldPath);
            }
            catch (Exception swapException)
            {
                await RecoverAsync(connection, realDatadir, oldPath, stagingPath, failedPath, instanceInfo, swapException);

                if (swapException is OperationCanceledException)
                    throw;

                throw new InvalidOperationException(
                    $"Восстановление не удалось ({swapException.Message}). " +
                    "MySQL возвращён в исходное состояние.",
                    swapException);
            }
        }
        finally
        {
            if (instanceInfo.ServiceName is not null)
                await _lifecycle.TryUnmaskServiceAsync(instanceInfo.ServiceName);
        }
    }

    private async Task RecoverAsync(
        ConnectionConfig connection, string realDatadir, string oldPath, string stagingPath, string failedPath,
        MysqlInstanceInfo instanceInfo, Exception originalException)
    {
        _logger.LogError(originalException,
            "Restore swap failed at MySQL datadir '{DataDir}'. Attempting recovery.", realDatadir);

        try
        {
            var freshPid = await _probe.GetMysqlPidAsync(connection, CancellationToken.None);
            if (freshPid != instanceInfo.Pid)
            {
                _logger.LogInformation(
                    "Recovery: MySQL PID changed ({OldPid} -> {NewPid}), using fresh value",
                    instanceInfo.Pid, freshPid);
                instanceInfo = instanceInfo with { Pid = freshPid };
            }

            _logger.LogInformation("Stopping MySQL before recovery");
            await _lifecycle.StopMysqlAsync(connection, instanceInfo, CancellationToken.None,
                unmaskServiceOnFailure: false);
        }
        catch (Exception stopEx)
        {
            _logger.LogWarning(stopEx, "Failed to stop MySQL before recovery — it may already be stopped");
        }

        var datadirExists = _swapper.DirectoryExists(realDatadir);
        var oldExists = _swapper.DirectoryExists(oldPath);

        if (datadirExists && oldExists)
        {
            _logger.LogWarning("Both datadir and backup exist. Moving new to '{FailedPath}', restoring backup.", failedPath);

            if (!_swapper.TryMoveDirectory(realDatadir, failedPath))
            {
                _swapper.TryDeleteDirectory(stagingPath);
                throw new InvalidOperationException(
                    $"Не удалось убрать повреждённый каталог данных '{realDatadir}'. " +
                    $"Рабочие данные находятся в '{oldPath}'. " +
                    $"Переместите их в '{realDatadir}' вручную и запустите MySQL.",
                    originalException);
            }

            if (!_swapper.TryMoveDirectory(oldPath, realDatadir))
            {
                _swapper.TryDeleteDirectory(stagingPath);
                throw new InvalidOperationException(
                    $"Не удалось вернуть исходный каталог данных на место. " +
                    $"Рабочие данные находятся в '{oldPath}'. " +
                    $"Переместите их в '{realDatadir}' вручную и запустите MySQL.",
                    originalException);
            }
        }
        else if (oldExists && !datadirExists)
        {
            _logger.LogWarning("Datadir missing, restoring from '{OldPath}'", oldPath);

            if (!_swapper.TryMoveDirectory(oldPath, realDatadir))
            {
                _swapper.TryDeleteDirectory(stagingPath);
                throw new InvalidOperationException(
                    $"Не удалось вернуть исходный каталог данных на место. " +
                    $"Рабочие данные находятся в '{oldPath}'. " +
                    $"Переместите их в '{realDatadir}' вручную и запустите MySQL.",
                    originalException);
            }
        }
        else if (!datadirExists && !oldExists)
        {
            throw new InvalidOperationException(
                $"Каталог данных MySQL '{realDatadir}' и резервная копия '{oldPath}' оба отсутствуют. " +
                "Данные могут быть утеряны. Проверьте файловую систему.",
                originalException);
        }

        _swapper.TryDeleteDirectory(stagingPath);

        if (_swapper.DirectoryExists(realDatadir))
        {
            try
            {
                await _lifecycle.StartMysqlAsync(connection, realDatadir, instanceInfo, CancellationToken.None);
                _logger.LogInformation("MySQL restarted on original datadir after restore failure");
            }
            catch (Exception startEx)
            {
                _logger.LogError(startEx, "Failed to start MySQL after rollback");
                throw new InvalidOperationException(
                    $"После отката datadir MySQL не запускается ({startEx.Message}). Запустите вручную.",
                    originalException);
            }
        }
    }
}
