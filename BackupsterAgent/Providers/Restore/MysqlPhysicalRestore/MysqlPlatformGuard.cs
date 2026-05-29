using BackupsterAgent.Exceptions;

namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

internal static class MysqlPlatformGuard
{
    public static void EnsureSupportedOperatingSystem()
    {
        if (!OperatingSystem.IsLinux())
            throw new RestorePermissionException(
                "Физическое восстановление MySQL через Percona XtraBackup поддерживается только на Linux. " +
                "На Windows используйте logical-режим или запустите агента на Linux-хосте рядом с MySQL.");
    }
}
