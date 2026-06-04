using System.Text;
using System.Text.RegularExpressions;
using BackupsterAgent.Configuration;
using MongoDB.Driver;

namespace BackupsterAgent.Services.Common.Resolvers;

internal sealed record MongoTopologyEndpoint(string Host, int Port);

internal static partial class MongoConnectionFactory
{
    public static bool HasConnectionUri(ConnectionConfig connection) =>
        !string.IsNullOrWhiteSpace(connection.ConnectionUri);

    public static MongoClientSettings BuildClientSettings(ConnectionConfig connection)
    {
        if (HasConnectionUri(connection))
            return MongoClientSettings.FromConnectionString(RequireConnectionUri(connection));

        var settings = new MongoClientSettings
        {
            Server = new MongoServerAddress(connection.Host, connection.Port),
            ConnectTimeout = TimeSpan.FromSeconds(10),
            ServerSelectionTimeout = TimeSpan.FromSeconds(10),
        };

        if (!string.IsNullOrEmpty(connection.Username))
        {
            settings.Credential = MongoCredential.CreateCredential(
                "admin", connection.Username, connection.Password);
        }

        return settings;
    }

    public static string BuildToolUri(ConnectionConfig connection)
    {
        if (HasConnectionUri(connection))
            return RequireConnectionUri(connection);

        var host = $"{connection.Host}:{connection.Port}";
        if (string.IsNullOrEmpty(connection.Username))
            return $"mongodb://{host}/?authSource=admin";

        var user = Uri.EscapeDataString(connection.Username);
        var pass = Uri.EscapeDataString(connection.Password);
        return $"mongodb://{user}:{pass}@{host}/?authSource=admin";
    }

    public static async Task<string> WriteToolConfigAsync(
        ConnectionConfig connection,
        string dir,
        CancellationToken ct)
    {
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, "config.yaml");
        var escaped = BuildToolUri(connection).Replace("'", "''");
        await File.WriteAllTextAsync(path, $"uri: '{escaped}'\n", Encoding.UTF8, ct);

        if (!OperatingSystem.IsWindows())
            File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);

        return path;
    }

    public static MongoTopologyEndpoint? BuildTopologyEndpoint(ConnectionConfig connection)
    {
        if (!HasConnectionUri(connection))
        {
            if (string.IsNullOrWhiteSpace(connection.Host))
                return null;

            return new MongoTopologyEndpoint(connection.Host, connection.Port);
        }

        _ = RequireConnectionUri(connection);
        var url = ParseConnectionUri(connection);
        var server = url.Servers.FirstOrDefault();
        if (server is null || string.IsNullOrWhiteSpace(server.Host))
            return null;

        return new MongoTopologyEndpoint(server.Host, server.Port);
    }

    public static string Redact(string value)
    {
        if (string.IsNullOrEmpty(value))
            return value;

        return MongoUriRegex().Replace(value, match => RedactUri(match.Value));
    }

    public static string DescribeUser(ConnectionConfig connection) =>
        HasConnectionUri(connection)
            ? "пользователь, заданный в ConnectionUri"
            : $"пользователь '{connection.Username}'";

    private static string RequireConnectionUri(ConnectionConfig connection)
    {
        if (string.IsNullOrWhiteSpace(connection.ConnectionUri))
            throw new InvalidOperationException(
                $"Для MongoDB-подключения '{connection.Name}' не задан ConnectionUri.");

        if (!string.IsNullOrWhiteSpace(connection.Host) ||
            !string.IsNullOrWhiteSpace(connection.Username) ||
            !string.IsNullOrWhiteSpace(connection.Password))
        {
            throw new InvalidOperationException(
                $"Для MongoDB-подключения '{connection.Name}' укажите либо ConnectionUri, либо Host/Username/Password.");
        }

        _ = ParseConnectionUri(connection);
        return connection.ConnectionUri.Trim();
    }

    private static MongoUrl ParseConnectionUri(ConnectionConfig connection)
    {
        try
        {
            var raw = connection.ConnectionUri!.TrimStart();
            if (!raw.StartsWith("mongodb://", StringComparison.OrdinalIgnoreCase) &&
                !raw.StartsWith("mongodb+srv://", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Схема ConnectionUri для MongoDB-подключения '{connection.Name}' не поддерживается. Используйте mongodb:// или mongodb+srv://.");
            }

            return MongoUrl.Create(raw);
        }
        catch (InvalidOperationException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"ConnectionUri для MongoDB-подключения '{connection.Name}' задан некорректно.", ex);
        }
    }

    private static string RedactUri(string raw)
    {
        var schemeMatch = Regex.Match(raw, @"^(mongodb(?:\+srv)?://)(.*)$", RegexOptions.IgnoreCase);
        if (!schemeMatch.Success)
            return "<redacted-mongodb-uri>";

        var scheme = schemeMatch.Groups[1].Value;
        var rest = schemeMatch.Groups[2].Value;
        var queryIndex = rest.IndexOfAny(['?', '#']);
        if (queryIndex >= 0)
            rest = rest[..queryIndex];

        var at = rest.LastIndexOf('@');
        if (at >= 0)
            rest = "<redacted>@" + rest[(at + 1)..];

        return scheme + rest;
    }

    [GeneratedRegex(@"mongodb(?:\+srv)?://[^\s'""<>]+", RegexOptions.IgnoreCase)]
    private static partial Regex MongoUriRegex();
}
