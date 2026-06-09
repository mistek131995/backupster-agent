using Microsoft.Data.SqlClient;

namespace BackupsterAgent.Providers.Backup;

public sealed class MssqlDifferentialChainGuard
{
    private const string Sql = @"
DECLARE @parent_backup_set_id INT;
DECLARE @parent_backup_set_uuid UNIQUEIDENTIFIER;

SELECT TOP (1)
    @parent_backup_set_id = bs.backup_set_id,
    @parent_backup_set_uuid = bs.backup_set_uuid
FROM msdb.dbo.backupset bs
INNER JOIN msdb.dbo.backupmediafamily bmf
    ON bmf.media_set_id = bs.media_set_id
WHERE bs.database_name = @db
  AND bs.type = 'D'
  AND bs.is_copy_only = 0
  AND RIGHT(bmf.physical_device_name, LEN(@parentFileName)) = @parentFileName
ORDER BY bs.backup_set_id DESC;

DECLARE @data_file_count INT = 0;
DECLARE @null_base_count INT = 0;
DECLARE @distinct_base_count INT = 0;
DECLARE @current_base_guid UNIQUEIDENTIFIER;

SELECT
    @data_file_count = COUNT(1),
    @null_base_count = COALESCE(SUM(CASE WHEN differential_base_guid IS NULL THEN 1 ELSE 0 END), 0)
FROM sys.master_files
WHERE database_id = DB_ID(@db)
  AND type = 0;

SELECT @distinct_base_count = COUNT(1)
FROM (
    SELECT differential_base_guid
    FROM sys.master_files
    WHERE database_id = DB_ID(@db)
      AND type = 0
      AND differential_base_guid IS NOT NULL
    GROUP BY differential_base_guid
) bases;

IF @data_file_count > 0
   AND @null_base_count = 0
   AND @distinct_base_count = 1
BEGIN
    SELECT TOP (1)
        @current_base_guid = differential_base_guid
    FROM sys.master_files
    WHERE database_id = DB_ID(@db)
      AND type = 0
      AND differential_base_guid IS NOT NULL;
END

SELECT
    CASE WHEN @parent_backup_set_id IS NULL THEN 0 ELSE 1 END AS parent_count,
    CASE WHEN @parent_backup_set_uuid IS NULL THEN 0 ELSE 1 END AS parent_uuid_present,
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
    END AS foreign_count,
    CASE
        WHEN @data_file_count = 0
          OR @null_base_count > 0
          OR @distinct_base_count <> 1 THEN CAST(1 AS BIT)
        ELSE CAST(0 AS BIT)
    END AS base_is_ambiguous,
    CASE
        WHEN @current_base_guid IS NOT NULL
         AND @parent_backup_set_uuid IS NOT NULL
         AND @current_base_guid = @parent_backup_set_uuid THEN CAST(1 AS BIT)
        ELSE CAST(0 AS BIT)
    END AS base_matches_parent;";

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
        bool parentBackupSetUuidPresent;
        long foreignCount;
        bool baseIsAmbiguous;
        bool baseMatchesParent;
        if (!await reader.ReadAsync(ct))
        {
            parentCount = 0;
            parentBackupSetUuidPresent = false;
            foreignCount = 0;
            baseIsAmbiguous = true;
            baseMatchesParent = false;
        }
        else
        {
            parentCount = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
            parentBackupSetUuidPresent = !reader.IsDBNull(1) && reader.GetInt32(1) == 1;
            foreignCount = reader.IsDBNull(2) ? 0 : reader.GetInt64(2);
            baseIsAmbiguous = reader.IsDBNull(3) || reader.GetBoolean(3);
            baseMatchesParent = !reader.IsDBNull(4) && reader.GetBoolean(4);
        }

        return Classify(
            parentCount,
            parentBackupSetUuidPresent,
            foreignCount,
            baseIsAmbiguous,
            baseMatchesParent);
    }

    public static MssqlDifferentialChainCheck Classify(
        int parentCount,
        bool parentBackupSetUuidPresent,
        long foreignCount,
        bool baseIsAmbiguous,
        bool baseMatchesParent)
    {
        if (parentCount == 0 || !parentBackupSetUuidPresent) return MssqlDifferentialChainCheck.ParentMissing;
        if (baseIsAmbiguous) return MssqlDifferentialChainCheck.BaseUnknownOrAmbiguous;
        if (!baseMatchesParent) return MssqlDifferentialChainCheck.BaseDiverged;
        if (foreignCount > 0) return MssqlDifferentialChainCheck.ForeignFullDetected;
        return MssqlDifferentialChainCheck.Ok;
    }
}
