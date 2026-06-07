using System.Diagnostics;

namespace BackupsterAgent.IntegrationTests.Backup;

internal static class PostgresIntegrationProcessRunner
{
    private const string PostgresUser = "postgres";

    public static bool CanUsePostgresUser()
    {
        if (!IsLinuxRootProcess())
            return true;

        return CommandSucceeds("id", ["-u", PostgresUser]) &&
               CommandSucceeds("runuser", ["-u", PostgresUser, "--", "true"]);
    }

    public static ProcessStartInfo CreatePostgresProcessStartInfo(string fileName, IEnumerable<string> arguments)
    {
        var psi = new ProcessStartInfo
        {
            FileName = IsLinuxRootProcess() ? "runuser" : fileName,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        if (IsLinuxRootProcess())
        {
            psi.ArgumentList.Add("-u");
            psi.ArgumentList.Add(PostgresUser);
            psi.ArgumentList.Add("--");
            psi.ArgumentList.Add(fileName);
        }

        foreach (var argument in arguments)
            psi.ArgumentList.Add(argument);

        psi.Environment["LC_MESSAGES"] = "C";
        psi.Environment["LANG"] = "C";
        return psi;
    }

    public static async Task PreparePgCtlLogAsync(IReadOnlyList<string> arguments, CancellationToken ct)
    {
        if (!IsLinuxRootProcess())
            return;

        for (var i = 0; i < arguments.Count - 1; i++)
        {
            if (!string.Equals(arguments[i], "-l", StringComparison.Ordinal))
                continue;

            var path = arguments[i + 1];
            var parent = Path.GetDirectoryName(path);
            if (!string.IsNullOrWhiteSpace(parent))
                Directory.CreateDirectory(parent);

            File.WriteAllText(path, string.Empty);
            await ChownPathToPostgresAsync(path, recursive: false, ct);
            return;
        }
    }

    public static async Task ChownPathToPostgresAsync(string path, bool recursive, CancellationToken ct)
    {
        if (!IsLinuxRootProcess())
            return;

        var psi = new ProcessStartInfo
        {
            FileName = "chown",
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        if (recursive)
            psi.ArgumentList.Add("-R");

        psi.ArgumentList.Add(PostgresUser);
        psi.ArgumentList.Add(path);

        using var process = new Process { StartInfo = psi };
        process.Start();
        await process.WaitForExitAsync(ct);

        if (process.ExitCode != 0)
            throw new InvalidOperationException($"chown failed for '{path}' with exit code {process.ExitCode}");
    }

    private static bool CommandSucceeds(string fileName, IEnumerable<string> arguments)
    {
        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = fileName,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            foreach (var argument in arguments)
                psi.ArgumentList.Add(argument);

            using var process = new Process { StartInfo = psi };
            process.Start();
            process.WaitForExit(5000);
            return process.HasExited && process.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }

    private static bool IsLinuxRootProcess() =>
        OperatingSystem.IsLinux() && string.Equals(Environment.UserName, "root", StringComparison.Ordinal);
}
