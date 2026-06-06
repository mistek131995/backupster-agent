namespace BackupsterAgent.Providers.Restore.Common;

public sealed class RestorePathResolver
{
    private readonly ILogger<RestorePathResolver> _logger;

    public RestorePathResolver(ILogger<RestorePathResolver> logger)
    {
        _logger = logger;
    }

    public string ResolveRealPath(string path, string subject)
    {
        var fullPath = Path.GetFullPath(path);
        try
        {
            var info = new DirectoryInfo(fullPath);
            var target = info.ResolveLinkTarget(returnFinalTarget: true);
            return target?.FullName is { Length: > 0 } realPath ? realPath : fullPath;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to resolve symlinks for {Subject} '{Path}'. Falling back to original path.",
                subject, fullPath);
            return fullPath;
        }
    }

    public static (string parent, string leaf) SplitPath(string path, string subject)
    {
        var trimmed = path.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var parent = Path.GetDirectoryName(trimmed);
        var leaf = Path.GetFileName(trimmed);
        if (string.IsNullOrEmpty(parent) || string.IsNullOrEmpty(leaf))
            throw new InvalidOperationException(
                $"Не удалось разобрать путь {subject} '{path}' на родительский каталог и имя.");

        return (parent, leaf);
    }
}
