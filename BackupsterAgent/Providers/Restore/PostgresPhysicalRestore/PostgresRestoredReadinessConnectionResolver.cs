using BackupsterAgent.Configuration;

namespace BackupsterAgent.Providers.Restore.PostgresPhysicalRestore;

internal static class PostgresRestoredReadinessConnectionResolver
{
    public static ConnectionConfig Resolve(ConnectionConfig source, string pgDataPath)
    {
        var configuredPort = TryReadConfiguredPort(pgDataPath);
        if (configuredPort is null || configuredPort == source.Port)
            return source;

        return new ConnectionConfig
        {
            Name = source.Name,
            DatabaseType = source.DatabaseType,
            ConnectionUri = source.ConnectionUri,
            Host = source.Host,
            Port = configuredPort.Value,
            Username = source.Username,
            Password = source.Password,
            BinPath = source.BinPath,
        };
    }

    internal static int? TryReadConfiguredPort(string pgDataPath)
    {
        int? port = null;
        foreach (var fileName in new[] { "postgresql.conf", "postgresql.auto.conf" })
        {
            var filePort = TryReadConfiguredPortFromFile(Path.Combine(pgDataPath, fileName));
            if (filePort is not null)
                port = filePort;
        }

        return port;
    }

    private static int? TryReadConfiguredPortFromFile(string path)
    {
        if (!File.Exists(path))
            return null;

        int? port = null;
        try
        {
            foreach (var rawLine in File.ReadLines(path))
            {
                var line = StripComment(rawLine).Trim();
                if (line.Length == 0)
                    continue;

                var separatorIndex = line.IndexOf('=');
                if (separatorIndex < 0)
                    continue;

                var name = line[..separatorIndex].Trim();
                if (!string.Equals(name, "port", StringComparison.OrdinalIgnoreCase))
                    continue;

                var value = Unquote(line[(separatorIndex + 1)..].Trim());
                if (int.TryParse(value, out var parsed) && parsed is > 0 and <= 65535)
                    port = parsed;
            }
        }
        catch
        {
            return null;
        }

        return port;
    }

    private static string StripComment(string value)
    {
        var inSingleQuote = false;
        var inDoubleQuote = false;

        for (var i = 0; i < value.Length; i++)
        {
            var current = value[i];

            if (current == '\'' && !inDoubleQuote)
            {
                if (inSingleQuote && i + 1 < value.Length && value[i + 1] == '\'')
                {
                    i++;
                    continue;
                }

                inSingleQuote = !inSingleQuote;
                continue;
            }

            if (current == '"' && !inSingleQuote)
            {
                inDoubleQuote = !inDoubleQuote;
                continue;
            }

            if (current == '#' && !inSingleQuote && !inDoubleQuote)
                return value[..i];
        }

        return value;
    }

    private static string Unquote(string value)
    {
        if (value.Length >= 2 &&
            ((value[0] == '\'' && value[^1] == '\'') ||
             (value[0] == '"' && value[^1] == '"')))
            return value[1..^1].Trim();

        return value;
    }
}
