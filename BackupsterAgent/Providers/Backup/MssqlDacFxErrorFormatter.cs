using Microsoft.SqlServer.Dac;

namespace BackupsterAgent.Providers.Backup;

internal static class MssqlDacFxErrorFormatter
{
    private const int MaxMessages = 3;
    private const int MaxMessageChars = 500;

    public static string BuildExportFailureMessage(
        string database,
        DacServicesException exception,
        IEnumerable<DacMessage> fallbackMessages) =>
        BuildExportFailureMessage(database, exception.Messages, fallbackMessages, exception.Message);

    internal static string BuildExportFailureMessage(
        string database,
        IEnumerable<DacMessage>? exceptionMessages,
        IEnumerable<DacMessage>? fallbackMessages,
        string? exceptionMessage)
    {
        var details = ExtractErrorMessages(exceptionMessages, fallbackMessages, exceptionMessage);
        if (details.Count == 0)
        {
            return $"Ошибка экспорта MSSQL logical-бэкапа БД '{database}' через DacFx. " +
                   "Подробности смотрите в логах агента.";
        }

        return $"Ошибка экспорта MSSQL logical-бэкапа БД '{database}' через DacFx: " +
               string.Join(" ", details);
    }

    internal static IReadOnlyList<string> ExtractErrorMessages(
        IEnumerable<DacMessage>? exceptionMessages,
        IEnumerable<DacMessage>? fallbackMessages,
        string? exceptionMessage = null)
    {
        var fromException = ExtractFromDacMessages(exceptionMessages);
        if (fromException.Count > 0)
            return fromException;

        var fromFallback = ExtractFromDacMessages(fallbackMessages);
        if (fromFallback.Count > 0)
            return fromFallback;

        var normalized = NormalizeText(exceptionMessage);
        return string.IsNullOrWhiteSpace(normalized) ? [] : [normalized];
    }

    private static IReadOnlyList<string> ExtractFromDacMessages(IEnumerable<DacMessage>? messages)
    {
        if (messages is null)
            return [];

        var source = messages.ToArray();
        var errors = source.Where(m => m.MessageType == DacMessageType.Error).ToArray();
        var selected = errors.Length > 0 ? errors : source;

        return selected
            .Select(FormatMessage)
            .Where(message => !string.IsNullOrWhiteSpace(message))
            .Distinct(StringComparer.Ordinal)
            .Take(MaxMessages)
            .ToArray();
    }

    private static string FormatMessage(DacMessage message)
    {
        var text = NormalizeText(message.Message);
        if (string.IsNullOrWhiteSpace(text))
            return string.Empty;

        return message.Number == 0 ? text : $"DacFx {message.Number}: {text}";
    }

    private static string NormalizeText(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return string.Empty;

        var normalized = string.Join(" ", text.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
        return normalized.Length <= MaxMessageChars
            ? normalized
            : normalized[..MaxMessageChars] + "...";
    }
}
