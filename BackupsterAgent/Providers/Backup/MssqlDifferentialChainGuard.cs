using Microsoft.Data.SqlClient;

namespace BackupsterAgent.Providers.Backup;

public sealed class MssqlDifferentialChainGuard
{
    private const string Sql = @"
DECLARE @parent_backup_set_id INT;

SELECT TOP (1)
    @parent_backup_set_id = bs.backup_set_id
FROM msdb.dbo.backupset bs
INNER JOIN msdb.dbo.backupmediafamily bmf
    ON bmf.media_set_id = bs.media_set_id
WHERE bs.database_name = @db
  AND bs.type = 'D'
  AND bs.is_copy_only = 0
  AND RIGHT(bmf.physical_device_name, LEN(@parentFileName)) = @parentFileName
ORDER BY bs.backup_set_id DESC;

SELECT
    CASE WHEN @parent_backup_set_id IS NULL THEN 0 ELSE 1 END AS parent_count,
    CASE
        WHEN @parent_backup_set_id IS NULL THEN CAST(0 AS BIGINT)
        ELSE (
            SELECT COUNT_BIG(*)
            FROM msdb.dbo.backupset
            WHERE database_name = @db
              AND type IN ('D', 'F', 'P')
              AND is_copy_only = 0
              AND backup_set_id > @parent_backup_set_id
        )
    END AS foreign_count;";

    public async Task<MssqlDifferentialChainCheck> InspectAsync(
        SqlConnection connection,
        string database,
        string parentBackupFileName,
        CancellationToken ct)
    {
        await using var cmd = new SqlCommand(Sql, connection) { CommandTimeout = 30 };
        cmd.Parameters.Add(new SqlParameter("@db", System.Data.SqlDbType.NVarChar, 128) { Value = database });
        cmd.Parameters.Add(new SqlParameter("@parentFileName", System.Data.SqlDbType.NVarChar, 260) { Value = parentBackupFileName });

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        int parentCount;
        long foreignCount;
        if (!await reader.ReadAsync(ct))
        {
            parentCount = 0;
            foreignCount = 0;
        }
        else
        {
            parentCount = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
            foreignCount = reader.IsDBNull(1) ? 0 : reader.GetInt64(1);
        }

        return Classify(parentCount, foreignCount);
    }

    public static MssqlDifferentialChainCheck Classify(int parentCount, long foreignCount)
    {
        if (parentCount == 0) return MssqlDifferentialChainCheck.ParentMissing;
        if (foreignCount > 0) return MssqlDifferentialChainCheck.ForeignFullDetected;
        return MssqlDifferentialChainCheck.Ok;
    }
}
