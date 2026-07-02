using System.Text.Json;
using BackupsterAgent.Exceptions;

namespace BackupsterAgent.Providers.Secrets;

internal static class SecretJsonValueExtractor
{
    public static string ExtractJsonKeyIfConfigured(string value, string? jsonKey, string settingPath)
    {
        if (string.IsNullOrWhiteSpace(jsonKey))
            return value;

        try
        {
            using var document = JsonDocument.Parse(value);
            if (document.RootElement.ValueKind != JsonValueKind.Object ||
                !document.RootElement.TryGetProperty(jsonKey, out var element))
            {
                throw new SecretResolutionException(
                    $"В секрете для '{settingPath}' не найден JSON-ключ '{jsonKey}'.");
            }

            if (element.ValueKind != JsonValueKind.String)
                throw new SecretResolutionException(
                    $"JSON-ключ '{jsonKey}' в секрете для '{settingPath}' должен быть строкой.");

            return element.GetString() ?? string.Empty;
        }
        catch (SecretResolutionException)
        {
            throw;
        }
        catch (JsonException ex)
        {
            throw new SecretResolutionException(
                $"Секрет для '{settingPath}' не удалось разобрать как JSON.",
                ex);
        }
    }
}
