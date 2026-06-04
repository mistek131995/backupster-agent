using System.Security.Cryptography;
using System.Text;

namespace BackupsterAgent.Services.Common.Resolvers;

public static class DatabaseNameValidator
{
    public const int MaxLength = 128;

    public static bool IsValid(string? name, out string? reason)
    {
        if (string.IsNullOrEmpty(name))
        {
            reason = "имя пустое";
            return false;
        }

        if (name.Length > MaxLength)
        {
            reason = $"длина {name.Length} символов больше лимита {MaxLength}";
            return false;
        }

        if (name.Contains(".."))
        {
            reason = "имя содержит подряд две точки";
            return false;
        }

        foreach (var ch in name)
        {
            if (!IsAllowedChar(ch))
            {
                reason = $"имя содержит недопустимый символ '{ch}' (U+{(int)ch:X4})";
                return false;
            }
        }

        reason = null;
        return true;
    }

    public static string ToSafePathSegment(string? name)
    {
        if (IsValid(name, out _)) return name!;

        if (string.IsNullOrEmpty(name))
            throw new InvalidOperationException("Имя БД из конфигурации пустое.");

        var bytes = Encoding.UTF8.GetBytes(name);
        var encoded = Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
        var segment = $"db-{encoded}";

        return segment.Length <= MaxLength
            ? segment
            : $"db-{Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant()}";
    }

    private static bool IsAllowedChar(char ch) =>
        (ch >= 'a' && ch <= 'z') ||
        (ch >= 'A' && ch <= 'Z') ||
        (ch >= '0' && ch <= '9') ||
        ch == '_' || ch == '-' || ch == '.';
}
