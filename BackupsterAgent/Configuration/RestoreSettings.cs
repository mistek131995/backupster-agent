namespace BackupsterAgent.Configuration;

public sealed class RestoreSettings
{
    // Каталог для временных файлов восстановления (распаковка, манифесты)
    public string? TempPath { get; init; }

    // Landing zone для файлов: используется только при восстановлении манифеста без roots[] и без targetFileRoot (legacy v1); очищается перед restore
    public string? FileRestoreBasePath { get; init; }

    // Таймаут запуска PostgreSQL через pg_ctl при восстановлении, сек
    public int PgCtlStartTimeoutSeconds { get; init; } = 600;

    // Таймаут смены владельца (chown -R) каталога данных, сек
    public int ChownTimeoutSeconds { get; init; } = 1800;

    // Таймаут мгновенных команд systemctl (mask/unmask/is-active), сек
    public int SystemctlTimeoutSeconds { get; init; } = 60;

    // Таймаут долгих команд systemctl (stop/start), сек
    public int SystemctlStopStartTimeoutSeconds { get; init; } = 1800;
}
