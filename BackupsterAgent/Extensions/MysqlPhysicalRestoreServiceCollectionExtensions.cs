using BackupsterAgent.Providers.Restore;
using BackupsterAgent.Providers.Restore.Common;
using BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace BackupsterAgent.Extensions;

public static class MysqlPhysicalRestoreServiceCollectionExtensions
{
    public static IServiceCollection AddMysqlPhysicalRestore(this IServiceCollection services)
    {
        services.TryAddSingleton<RestorePathResolver>();
        services.TryAddSingleton<RestoreMarkerStore>();
        services.TryAddSingleton<FilesystemRenamePreflight>();
        services.TryAddSingleton<LinuxProcessInspector>();
        services.TryAddSingleton<SystemdUnitDetector>();
        services.TryAddSingleton<SystemdServiceController>();
        services.AddSingleton<MysqlServerProbe>();
        services.AddSingleton<MysqlBackupExtractor>();
        services.AddSingleton<MysqlInstanceInspector>();
        services.AddSingleton<MysqlSystemdController>();
        services.AddSingleton<MysqlLifecycleManager>();
        services.AddSingleton<MysqlDatadirSwapper>();
        services.AddSingleton<IMysqlServerProbe>(sp => sp.GetRequiredService<MysqlServerProbe>());
        services.AddSingleton<IMysqlLifecycleManager>(sp => sp.GetRequiredService<MysqlLifecycleManager>());
        services.AddSingleton<IMysqlDatadirSwapper>(sp => sp.GetRequiredService<MysqlDatadirSwapper>());
        services.AddSingleton<MysqlDatadirSwapCoordinator>();
        services.AddSingleton<MysqlPhysicalRestoreProvider>();
        return services;
    }
}
