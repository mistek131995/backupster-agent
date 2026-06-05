using BackupsterAgent.Configuration;

namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

public interface IMysqlDatadirSwapper
{
    void MoveDirectory(string from, string to);

    bool DirectoryExists(string path);

    Task FixOwnershipAsync(string newDatadir, MysqlInstanceInfo instanceInfo, CancellationToken ct);

    bool TryMoveDirectory(string from, string to);

    void TryDeleteDirectory(string path);
}
