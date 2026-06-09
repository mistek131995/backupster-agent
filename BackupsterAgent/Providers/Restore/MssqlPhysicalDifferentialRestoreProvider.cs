using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Data.SqlClient;

namespace BackupsterAgent.Providers.Restore;

public sealed class MssqlPhysicalDifferentialRestoreProvider : IDifferentialRestoreProvider
{
    private readonly ILogger<MssqlPhysicalDifferentialRestoreProvider> _logger;
    private readonly MssqlPhysicalRestoreProvider _fullProvider;

    public MssqlPhysicalDifferentialRestoreProvider(
        ILogger<MssqlPhysicalDifferentialRestoreProvider> logger,
        MssqlPhysicalRestoreProvider fullProvider)
    {
        _logger = logger;
        _fullProvider = fullProvider;
    }

    public Task ValidatePermissionsAsync(ConnectionConfig connection, string targetDatabase, CancellationToken ct) =>
        _fullProvider.ValidatePermissionsAsync(connection, targetDatabase, ct);

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
            await _fullProvider.ValidateRestoreSourceAsync(connection, item.DumpFilePath, ct);
    }

    public Task PrepareTargetDatabaseAsync(ConnectionConfig connection, string targetDatabase, CancellationToken ct) =>
        _fullProvider.PrepareTargetDatabaseAsync(connection, targetDatabase, ct);

    public async Task RestoreAsync(
        ConnectionConfig connection,
        string targetDatabase,
        IReadOnlyList<DifferentialRestoreChainItem> chain,
        CancellationToken ct)
    {
        if (chain.Count < 2)
            throw new InvalidOperationException(
                $"Цепочка восстановления MSSQL должна содержать минимум один полный и один дифференциальный бэкап, получено {chain.Count}.");

        _logger.LogInformation(
            "MSSQL differential restore starting. Target: '{Target}', Chain length: {Length}",
            targetDatabase, chain.Count);

        var fullItem = chain[0];
        var lastDiffItem = chain[^1];

        _logger.LogInformation(
            "Step 1/2: RESTORE FULL '{Path}' WITH NORECOVERY", fullItem.DumpFilePath);

        await _fullProvider.RestoreFromBakAsync(
            connection, targetDatabase, fullItem.DumpFilePath, withRecovery: false, ct);

        var quoted = QuoteIdentifier(targetDatabase);
        var escapedDiffPath = lastDiffItem.DumpFilePath.Replace("'", "''");
        var diffSql = $"RESTORE DATABASE {quoted} FROM DISK = N'{escapedDiffPath}' WITH FILE = 1, RECOVERY;";

        _logger.LogInformation(
            "Step 2/2: RESTORE DIFFERENTIAL '{Path}' WITH RECOVERY", lastDiffItem.DumpFilePath);

        try
        {
            await using var conn = new SqlConnection(MssqlConnectionFactory.BuildMasterConnectionString(connection));
            await conn.OpenAsync(ct);
            await using var cmd = new SqlCommand(diffSql, conn) { CommandTimeout = 0 };
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (SqlException ex) when (ex.Number == 3136)
        {
            throw new DifferentialChainBrokenException(
                $"Цепочка восстановления MSSQL для БД '{targetDatabase}' сломана: дифференциальный бэкап '{lastDiffItem.DumpFilePath}' не соответствует восстановленному полному бэкапу '{fullItem.DumpFilePath}'. " +
                "Скорее всего, выбранная цепочка опирается на другой full backup или база была восстановлена в более старое состояние. Запустите новый полный бэкап.",
                ex);
        }

        _logger.LogInformation("MSSQL differential restore completed for database '{Database}'", targetDatabase);
    }

    private static string QuoteIdentifier(string name)
    {
        if (string.IsNullOrEmpty(name))
            throw new ArgumentException("Database name cannot be empty", nameof(name));
        return "[" + name.Replace("]", "]]") + "]";
    }
}
