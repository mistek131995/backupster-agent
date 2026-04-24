using System.Collections.Concurrent;
using BackupsterAgent.Configuration;

namespace BackupsterAgent.Services.Common.Resolvers;

public sealed class MysqlBinaryResolver
{
    private readonly ILogger<MysqlBinaryResolver> _logger;
    private readonly ConcurrentDictionary<string, string?> _binDirCache = new();

    public MysqlBinaryResolver(ILogger<MysqlBinaryResolver> logger)
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
                "Resolved MySQL bin directory for connection '{Name}': '{Dir}'",
                connection.Name, candidate);
            return candidate;
        }

        _logger.LogInformation(
            "No MySQL install found on agent host for connection '{Name}', falling back to PATH",
            connection.Name);
        return null;
    }

    private static string? FindBinDir()
    {
        if (OperatingSystem.IsWindows())
        {
            foreach (var root in EnumerateWindowsRoots())
            {
                var mysqlRoot = Path.Combine(root, "MySQL");
                if (!Directory.Exists(mysqlRoot)) continue;

                var best = Directory.EnumerateDirectories(mysqlRoot, "MySQL Server *")
                    .Select(d => new { Dir = d, Version = ParseWindowsVersion(d) })
                    .Where(x => Directory.Exists(Path.Combine(x.Dir, "bin")))
                    .OrderByDescending(x => x.Version)
                    .FirstOrDefault();

                if (best is not null)
                    return Path.Combine(best.Dir, "bin");
            }

            return null;
        }

        const string tarball = "/usr/local/mysql/bin";
        if (Directory.Exists(tarball)) return tarball;

        return null;
    }

    private static IEnumerable<string> EnumerateWindowsRoots()
    {
        var pf = Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles);
        if (!string.IsNullOrEmpty(pf)) yield return pf;

        var pf86 = Environment.GetFolderPath(Environment.SpecialFolder.ProgramFilesX86);
        if (!string.IsNullOrEmpty(pf86) && !string.Equals(pf86, pf, StringComparison.OrdinalIgnoreCase))
            yield return pf86;
    }

    private static Version ParseWindowsVersion(string dirPath)
    {
        var name = Path.GetFileName(dirPath);
        var prefix = "MySQL Server ";
        if (!name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return new Version(0, 0);

        var raw = name.Substring(prefix.Length).Trim();
        return Version.TryParse(raw, out var v) ? v : new Version(0, 0);
    }

    private static string ComposePath(string? dir, string binaryName)
    {
        if (dir is null) return binaryName;

        var withExt = OperatingSystem.IsWindows() && !binaryName.EndsWith(".exe", StringComparison.OrdinalIgnoreCase)
            ? binaryName + ".exe"
            : binaryName;

        return Path.Combine(dir, withExt);
    }
}
