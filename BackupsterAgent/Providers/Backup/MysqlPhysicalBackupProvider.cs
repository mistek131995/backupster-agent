using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;

namespace BackupsterAgent.Providers.Backup;

public sealed class MysqlPhysicalBackupProvider : IBackupProvider
{
    public Task ValidatePermissionsAsync(ConnectionConfig connection, string database, CancellationToken ct) =>
        Task.CompletedTask;

    public Task<BackupResult> BackupAsync(DatabaseConfig config, ConnectionConfig connection, CancellationToken ct) =>
        throw new NotSupportedException(
            "Физический бэкап MySQL не поддерживается. По вопросам реализации обращайтесь: support@backupster.io");
}
