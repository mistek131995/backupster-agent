using System.Text;

namespace BackupsterAgent.Providers.Restore;

internal static class PlainTextSqlValidator
{
    private const int HeaderProbeBytes = 4096;

    public static async Task ValidateAsync(string filePath, string[] expectedHeaders, string toolName, CancellationToken ct)
    {
        if (!File.Exists(filePath))
            throw new InvalidOperationException(
                $"Файл бэкапа '{filePath}' не найден после распаковки. Файл повреждён или хранилище недоступно.");

        var info = new FileInfo(filePath);
        if (info.Length == 0)
            throw new InvalidOperationException(
                $"Файл бэкапа '{filePath}' пустой. Создайте новый бэкап и повторите восстановление.");

        var probeSize = (int)Math.Min(HeaderProbeBytes, info.Length);
        var buffer = new byte[probeSize];

        await using var stream = new FileStream(
            filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: HeaderProbeBytes, useAsync: true);

        var read = 0;
        while (read < probeSize)
        {
            var n = await stream.ReadAsync(buffer.AsMemory(read, probeSize - read), ct);
            if (n == 0) break;
            read += n;
        }

        for (var i = 0; i < read; i++)
        {
            if (buffer[i] == 0)
                throw new InvalidOperationException(
                    $"Файл бэкапа '{filePath}' содержит бинарные данные в начале файла — это не похоже на SQL-дамп от {toolName}. " +
                    "Файл повреждён либо создан другой утилитой.");
        }

        var header = Encoding.UTF8.GetString(buffer, 0, read);
        var matched = expectedHeaders.Any(h => header.Contains(h, StringComparison.Ordinal));

        if (!matched)
            throw new InvalidOperationException(
                $"Файл '{filePath}' не содержит ожидаемую шапку дампа {toolName}. " +
                "Возможно, файл повреждён, обрезан или создан другой утилитой.");
    }
}
