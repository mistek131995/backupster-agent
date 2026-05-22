using System.Formats.Tar;

namespace BackupsterAgent.Services.Backup;

public static class PgBaseContainer
{
    public const string BaseEntryName = "base.tar.gz";
    public const string WalEntryName = "pg_wal.tar.gz";

    public static async Task WriteAsync(
        string outputPath,
        string baseTarGzPath,
        string pgWalTarGzPath,
        CancellationToken ct)
    {
        if (!File.Exists(baseTarGzPath))
            throw new FileNotFoundException(
                $"Файл базы для упаковки не найден: '{baseTarGzPath}'.", baseTarGzPath);

        if (!File.Exists(pgWalTarGzPath))
            throw new FileNotFoundException(
                $"Файл WAL для упаковки не найден: '{pgWalTarGzPath}'.", pgWalTarGzPath);

        await using var output = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.None);
        await using var writer = new TarWriter(output, TarEntryFormat.Pax, leaveOpen: false);

        await writer.WriteEntryAsync(baseTarGzPath, BaseEntryName, ct);
        await writer.WriteEntryAsync(pgWalTarGzPath, WalEntryName, ct);
    }

    public static async Task<PgBaseContainerEntries> ExtractAsync(
        string inputPath,
        string destDir,
        CancellationToken ct)
    {
        if (!File.Exists(inputPath))
            throw new FileNotFoundException(
                $"Контейнер pgbase для распаковки не найден: '{inputPath}'.", inputPath);

        Directory.CreateDirectory(destDir);

        string? baseTarGzPath = null;
        string? pgWalTarGzPath = null;

        await using (var input = new FileStream(inputPath, FileMode.Open, FileAccess.Read, FileShare.Read))
        await using (var reader = new TarReader(input, leaveOpen: false))
        {
            TarEntry? entry;
            while ((entry = await reader.GetNextEntryAsync(copyData: false, cancellationToken: ct)) is not null)
            {
                if (entry.EntryType != TarEntryType.RegularFile)
                    continue;

                var dest = entry.Name switch
                {
                    BaseEntryName => Path.Combine(destDir, BaseEntryName),
                    WalEntryName => Path.Combine(destDir, WalEntryName),
                    _ => null,
                };

                if (dest is null)
                    continue;

                await entry.ExtractToFileAsync(dest, overwrite: true, cancellationToken: ct);

                if (entry.Name == BaseEntryName) baseTarGzPath = dest;
                else if (entry.Name == WalEntryName) pgWalTarGzPath = dest;
            }
        }

        if (baseTarGzPath is null)
            throw new InvalidOperationException(
                $"Контейнер pgbase '{inputPath}' не содержит обязательной записи '{BaseEntryName}'. " +
                "Возможно, файл повреждён или создан несовместимой версией агента.");

        if (pgWalTarGzPath is null)
            throw new InvalidOperationException(
                $"Контейнер pgbase '{inputPath}' не содержит обязательной записи '{WalEntryName}'. " +
                "Возможно, файл повреждён или создан несовместимой версией агента.");

        return new PgBaseContainerEntries(baseTarGzPath, pgWalTarGzPath);
    }
}

public sealed record PgBaseContainerEntries(string BaseTarGzPath, string PgWalTarGzPath);
