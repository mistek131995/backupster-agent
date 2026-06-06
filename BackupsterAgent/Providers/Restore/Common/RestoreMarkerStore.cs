using System.Globalization;

namespace BackupsterAgent.Providers.Restore.Common;

public sealed class RestoreMarkerStore
{
    public const string MarkerFileName = ".backupster-marker";

    private readonly ILogger<RestoreMarkerStore> _logger;

    public RestoreMarkerStore(ILogger<RestoreMarkerStore> logger)
    {
        _logger = logger;
    }

    public static void WriteMarkerFile(string dir)
    {
        var path = Path.Combine(dir, MarkerFileName);
        File.WriteAllText(path, DateTime.UtcNow.ToString("o"));
    }

    public void CleanupOrphanStagingDirs(string parent, string leaf, IReadOnlyCollection<string> suffixes, TimeSpan gracePeriod)
    {
        var threshold = DateTime.UtcNow - gracePeriod;

        foreach (var suffix in suffixes)
        {
            IEnumerable<string> matches;
            try
            {
                matches = Directory.EnumerateDirectories(parent, $"{leaf}.{suffix}-*");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Orphan cleanup: failed to enumerate '{Parent}' for pattern '{Leaf}.{Suffix}-*'",
                    parent, leaf, suffix);
                continue;
            }

            foreach (var dir in matches)
                ProcessOrphanDirectory(dir, threshold, gracePeriod);
        }
    }

    private void ProcessOrphanDirectory(string dir, DateTime threshold, TimeSpan gracePeriod)
    {
        try
        {
            var marker = Path.Combine(dir, MarkerFileName);
            if (!File.Exists(marker))
            {
                _logger.LogDebug(
                    "Orphan cleanup: '{Dir}' has no '{Marker}' marker, leaving alone",
                    dir, MarkerFileName);
                return;
            }

            DateTime createdAt;
            try
            {
                var content = File.ReadAllText(marker).Trim();
                if (!DateTime.TryParse(content, null,
                        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                        out createdAt))
                {
                    _logger.LogDebug(
                        "Orphan cleanup: '{Dir}' marker has unparseable timestamp '{Content}', leaving alone",
                        dir, content);
                    return;
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex,
                    "Orphan cleanup: '{Dir}' marker unreadable, leaving alone", dir);
                return;
            }

            if (createdAt > threshold)
            {
                _logger.LogDebug(
                    "Orphan cleanup: '{Dir}' marker created {CreatedAt:o}, younger than {Hours}h, leaving alone",
                    dir, createdAt, gracePeriod.TotalHours);
                return;
            }

            _logger.LogWarning(
                "Orphan cleanup: deleting stale staging dir '{Dir}' (marker created {CreatedAt:o}, age > {Hours}h)",
                dir, createdAt, gracePeriod.TotalHours);
            Directory.Delete(dir, recursive: true);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Orphan cleanup: failed to process '{Dir}'", dir);
        }
    }
}
