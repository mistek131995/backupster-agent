using System.Text;

namespace BackupsterAgent.Providers.Backup.MssqlLogicalBackup;

internal sealed class MssqlLogicalPreflightResult
{
    private const int MaxDetailsPerCode = 3;

    public MssqlLogicalPreflightResult(IEnumerable<Finding> findings)
    {
        Findings = findings.ToArray();
    }

    public IReadOnlyList<Finding> Findings { get; }

    public IReadOnlyList<Finding> BlockingFindings =>
        Findings.Where(f => f.Severity == FindingSeverity.Blocking).ToArray();

    public IReadOnlyList<Finding> WarningFindings =>
        Findings.Where(f => f.Severity == FindingSeverity.Warning).ToArray();

    public bool HasBlockingFindings => Findings.Any(f => f.Severity == FindingSeverity.Blocking);

    public bool HasWarningFindings => Findings.Any(f => f.Severity == FindingSeverity.Warning);

    public string BuildBlockingUserMessage(string database)
    {
        var blocking = BlockingFindings;
        if (blocking.Count == 0)
            throw new InvalidOperationException("No blocking MSSQL logical preflight findings.");

        var builder = new StringBuilder();
        builder.Append($"MSSQL logical-бэкап БД '{database}' не может быть создан через DacFx: ");
        builder.Append("база содержит объекты, неподдерживаемые форматом bacpac. Найдено: ");
        builder.Append(string.Join("; ", BuildSummaries(blocking, GetRussianTitle)));
        builder.Append(". Используйте MSSQL physical backup или устраните эти зависимости перед logical-бэкапом.");
        return builder.ToString();
    }

    public string BuildWarningLogMessage(string database)
    {
        var warnings = WarningFindings;
        if (warnings.Count == 0)
            return $"MSSQL logical preflight found no warnings for database '{database}'.";

        return $"MSSQL logical preflight warnings for database '{database}': " +
               string.Join("; ", BuildSummaries(warnings, GetEnglishTitle));
    }

    public static Finding CreateFinding(string code, string? detail)
    {
        if (!Enum.TryParse<FindingCode>(code, ignoreCase: true, out var parsed))
            throw new InvalidOperationException($"Unknown MSSQL logical preflight finding code '{code}'.");

        return CreateFinding(parsed, detail);
    }

    public static Finding CreateFinding(FindingCode code, string? detail) =>
        new(code, GetSeverity(code), NormalizeDetail(detail));

    private static IEnumerable<string> BuildSummaries(
        IReadOnlyList<Finding> findings,
        Func<FindingCode, string> titleFactory)
    {
        foreach (var group in findings.GroupBy(f => f.Code).OrderBy(g => g.Key.ToString(), StringComparer.Ordinal))
        {
            var distinctDetails = group
                .Select(f => f.Detail)
                .Where(detail => !string.IsNullOrWhiteSpace(detail))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();

            var shownDetails = distinctDetails.Take(MaxDetailsPerCode).ToArray();
            var suffix = distinctDetails.Length > shownDetails.Length
                ? $" (+{distinctDetails.Length - shownDetails.Length})"
                : string.Empty;
            var title = titleFactory(group.Key);

            yield return shownDetails.Length == 0
                ? title
                : $"{title}: {string.Join(", ", shownDetails)}{suffix}";
        }
    }

    private static FindingSeverity GetSeverity(FindingCode code) => code switch
    {
        FindingCode.CrossDatabaseReference => FindingSeverity.Blocking,
        FindingCode.LinkedServerReference => FindingSeverity.Blocking,
        FindingCode.CdcEnabled => FindingSeverity.Blocking,
        FindingCode.ReplicationEnabled => FindingSeverity.Blocking,
        FindingCode.FileStream => FindingSeverity.Blocking,
        FindingCode.OrphanedUser => FindingSeverity.Warning,
        FindingCode.ClrUnsafeOrExternal => FindingSeverity.Warning,
        FindingCode.TdeEnabled => FindingSeverity.Warning,
        _ => throw new ArgumentOutOfRangeException(nameof(code), code, null),
    };

    private static string GetRussianTitle(FindingCode code) => code switch
    {
        FindingCode.CrossDatabaseReference => "ссылки на другие БД",
        FindingCode.LinkedServerReference => "ссылки на linked servers",
        FindingCode.CdcEnabled => "CDC",
        FindingCode.ReplicationEnabled => "репликация",
        FindingCode.FileStream => "FileStream/FileTable",
        FindingCode.OrphanedUser => "осиротевшие пользователи",
        FindingCode.ClrUnsafeOrExternal => "CLR UNSAFE/EXTERNAL_ACCESS",
        FindingCode.TdeEnabled => "TDE",
        _ => throw new ArgumentOutOfRangeException(nameof(code), code, null),
    };

    private static string GetEnglishTitle(FindingCode code) => code switch
    {
        FindingCode.CrossDatabaseReference => "cross-database references",
        FindingCode.LinkedServerReference => "linked server references",
        FindingCode.CdcEnabled => "CDC",
        FindingCode.ReplicationEnabled => "replication",
        FindingCode.FileStream => "FileStream/FileTable",
        FindingCode.OrphanedUser => "orphaned users",
        FindingCode.ClrUnsafeOrExternal => "CLR UNSAFE/EXTERNAL_ACCESS assemblies",
        FindingCode.TdeEnabled => "TDE",
        _ => throw new ArgumentOutOfRangeException(nameof(code), code, null),
    };

    private static string NormalizeDetail(string? detail)
    {
        if (string.IsNullOrWhiteSpace(detail))
            return string.Empty;

        return string.Join(" ", detail.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
    }

    public sealed record Finding(FindingCode Code, FindingSeverity Severity, string Detail);

    public enum FindingSeverity
    {
        Blocking,
        Warning,
    }

    public enum FindingCode
    {
        CrossDatabaseReference,
        LinkedServerReference,
        CdcEnabled,
        ReplicationEnabled,
        FileStream,
        OrphanedUser,
        ClrUnsafeOrExternal,
        TdeEnabled,
    }
}
