namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

internal static class MysqldArgsSanitizer
{
    public static readonly IReadOnlySet<string> SensitiveMysqldKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "--password",
        "--admin-password",
        "--init-file",
        "--init-connect",
        "--ssl-key",
        "--ssl-cert",
        "--ssl-ca",
        "--tls-key",
        "--tls-cert",
        "--plugin-load",
        "--plugin-load-add",
        "--early-plugin-load",
        "--skip-grant-tables",
    };

    public static IReadOnlyList<string> FilterOriginalArgs(IReadOnlyList<string> args, ISet<string> keysToRemove)
    {
        var normalizedRemove = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var key in keysToRemove)
            normalizedRemove.Add(NormalizeMysqldKey(key));

        var result = new List<string>(args.Count);
        for (var i = 0; i < args.Count; i++)
        {
            var arg = args[i];
            if (!arg.StartsWith("--", StringComparison.Ordinal))
            {
                result.Add(arg);
                continue;
            }

            var eqIndex = arg.IndexOf('=');
            var rawKey = eqIndex >= 0 ? arg[..eqIndex] : arg;
            var normalizedKey = NormalizeMysqldKey(rawKey);

            if (!normalizedRemove.Contains(normalizedKey))
            {
                result.Add(arg);
                continue;
            }

            if (eqIndex < 0 && i + 1 < args.Count && !args[i + 1].StartsWith("-", StringComparison.Ordinal))
                i++;
        }
        return result;
    }

    private static string NormalizeMysqldKey(string key) => key.Replace('_', '-');
}
