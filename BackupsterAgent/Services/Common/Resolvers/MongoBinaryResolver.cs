using System.Collections.Concurrent;
using BackupsterAgent.Configuration;

namespace BackupsterAgent.Services.Common.Resolvers;

public sealed class MongoBinaryResolver
{
    private readonly ILogger<MongoBinaryResolver> _logger;
    private readonly ConcurrentDictionary<string, string?> _binDirCache = new();

    public MongoBinaryResolver(ILogger<MongoBinaryResolver> logger)
    {
        _logger = logger;
    }

    public string Resolve(ConnectionConfig connection, string binaryName)
    {
        if (string.IsNullOrWhiteSpace(binaryName))
            throw new ArgumentException("Binary name is required", nameof(binaryName));

        var key = connection.BinPath ?? string.Empty;
        var dir = _binDirCache.GetOrAdd(key, _ => ResolveDirectory(connection));
        return ComposePath(dir, binaryName);
    }

    private string? ResolveDirectory(ConnectionConfig connection)
    {
        if (!string.IsNullOrWhiteSpace(connection.BinPath))
        {
            var overridePath = connection.BinPath!;
            if (!Directory.Exists(overridePath))
                throw new InvalidOperationException(
                    $"Configured BinPath '{overridePath}' for connection '{connection.Name}' does not exist.");

            _logger.LogInformation(
                "Using configured BinPath '{Dir}' for connection '{Name}'",
                overridePath, connection.Name);
            return overridePath;
        }

        var candidate = FindBinDir();
        if (candidate is not null)
        {
            _logger.LogInformation(
                "Resolved MongoDB bin directory for connection '{Name}': '{Dir}'",
                connection.Name, candidate);
            return candidate;
        }

        _logger.LogInformation(
            "No MongoDB Tools install found on agent host for connection '{Name}', falling back to PATH",
            connection.Name);
        return null;
    }

    private static string? FindBinDir()
    {
        if (OperatingSystem.IsWindows())
        {
            var pf = Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles);
            if (string.IsNullOrEmpty(pf)) return null;

            var toolsRoot = Path.Combine(pf, "MongoDB", "Tools");
            if (Directory.Exists(toolsRoot))
            {
                var best = Directory.EnumerateDirectories(toolsRoot)
                    .Where(d => Directory.Exists(Path.Combine(d, "bin")))
                    .Select(d => new { Dir = d, Version = ParseVersion(Path.GetFileName(d)) })
                    .OrderByDescending(x => x.Version)
                    .FirstOrDefault();

                if (best is not null)
                    return Path.Combine(best.Dir, "bin");
            }

            var serverRoot = Path.Combine(pf, "MongoDB", "Server");
            if (Directory.Exists(serverRoot))
            {
                var best = Directory.EnumerateDirectories(serverRoot)
                    .Where(d => Directory.Exists(Path.Combine(d, "bin")))
                    .Select(d => new { Dir = d, Version = ParseVersion(Path.GetFileName(d)) })
                    .OrderByDescending(x => x.Version)
                    .FirstOrDefault();

                if (best is not null)
                    return Path.Combine(best.Dir, "bin");
            }

            return null;
        }

        if (File.Exists("/usr/bin/mongodump")) return "/usr/bin";
        if (File.Exists("/usr/local/bin/mongodump")) return "/usr/local/bin";

        return null;
    }

    private static Version ParseVersion(string name) =>
        Version.TryParse(name, out var v) ? v : new Version(0, 0);

    private static string ComposePath(string? dir, string binaryName)
    {
        if (dir is null) return binaryName;

        var withExt = OperatingSystem.IsWindows() && !binaryName.EndsWith(".exe", StringComparison.OrdinalIgnoreCase)
            ? binaryName + ".exe"
            : binaryName;

        return Path.Combine(dir, withExt);
    }
}
