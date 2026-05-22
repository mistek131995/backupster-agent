namespace BackupsterAgent.Services.Backup;

public enum PgBaseDumpFormat
{
    LegacySingleTarGz,
    Container,
}

public static class PgBaseFormatDetector
{
    public const string ContainerExtension = ".pgbase.tar";

    private const int SniffBufferSize = 265;
    private const int UstarOffset = 257;

    public static PgBaseDumpFormat DetectByName(string objectKeyOrPath)
    {
        if (string.IsNullOrWhiteSpace(objectKeyOrPath))
            return PgBaseDumpFormat.LegacySingleTarGz;

        var name = objectKeyOrPath;
        if (name.EndsWith(".enc", StringComparison.OrdinalIgnoreCase))
            name = name[..^4];

        return name.EndsWith(ContainerExtension, StringComparison.OrdinalIgnoreCase)
            ? PgBaseDumpFormat.Container
            : PgBaseDumpFormat.LegacySingleTarGz;
    }

    public static async Task<PgBaseDumpFormat> DetectByContentAsync(string path, CancellationToken ct)
    {
        await using var fs = new FileStream(
            path, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: SniffBufferSize, useAsync: true);

        var buffer = new byte[SniffBufferSize];
        var totalRead = 0;
        while (totalRead < buffer.Length)
        {
            var n = await fs.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead), ct);
            if (n == 0) break;
            totalRead += n;
        }

        return Detect(buffer.AsSpan(0, totalRead));
    }

    internal static PgBaseDumpFormat Detect(ReadOnlySpan<byte> head)
    {
        if (head.Length >= 2 && head[0] == 0x1F && head[1] == 0x8B)
            return PgBaseDumpFormat.LegacySingleTarGz;

        if (head.Length >= UstarOffset + 5
            && head[UstarOffset + 0] == (byte)'u'
            && head[UstarOffset + 1] == (byte)'s'
            && head[UstarOffset + 2] == (byte)'t'
            && head[UstarOffset + 3] == (byte)'a'
            && head[UstarOffset + 4] == (byte)'r')
            return PgBaseDumpFormat.Container;

        return PgBaseDumpFormat.LegacySingleTarGz;
    }
}
