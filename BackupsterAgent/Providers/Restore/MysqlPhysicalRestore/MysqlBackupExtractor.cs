using System.IO.Compression;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Processes;

namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

public sealed class MysqlBackupExtractor
{
    private readonly ILogger<MysqlBackupExtractor> _logger;
    private readonly IExternalProcessRunner _processRunner;

    public MysqlBackupExtractor(
        ILogger<MysqlBackupExtractor> logger,
        IExternalProcessRunner processRunner)
    {
        _logger = logger;
        _processRunner = processRunner;
    }

    public async Task ExtractXbstreamAsync(string xbstream, string archivePath, string targetDir, CancellationToken ct)
    {
        var request = new ExternalProcessRequest
        {
            FileName = xbstream,
            Arguments = new[] { "-x", "-C", targetDir },
            RedirectStandardInput = true,
        };

        var result = await _processRunner.RunAsync(
            request,
            handleStdout: null,
            handleStdin: async (stdin, innerCt) =>
            {
                await using var fileStream = new FileStream(archivePath, FileMode.Open, FileAccess.Read,
                    FileShare.Read, bufferSize: 65536, useAsync: true);
                await using var gzipStream = new GZipStream(fileStream, CompressionMode.Decompress);
                await gzipStream.CopyToAsync(stdin.BaseStream, innerCt);
            },
            ct);

        if (result.ExitCode != 0)
        {
            var stderr = result.Stderr.Trim();
            throw new InvalidOperationException(
                $"xbstream завершился с ошибкой (код {result.ExitCode}): {stderr}");
        }
    }

    public async Task PrepareBackupAsync(string xtrabackup, string targetDir, CancellationToken ct)
    {
        var request = new ExternalProcessRequest
        {
            FileName = xtrabackup,
            Arguments = new[] { "--prepare", "--target-dir=" + targetDir },
        };

        var result = await _processRunner.RunAsync(request, handleStdout: null, handleStdin: null, ct);

        if (result.ExitCode != 0)
        {
            var stderr = result.Stderr.Trim();
            throw new InvalidOperationException(
                $"xtrabackup --prepare завершился с ошибкой (код {result.ExitCode}): {stderr}");
        }

        _logger.LogInformation("xtrabackup --prepare completed successfully");
    }

    public async Task EnsureBinaryAvailableAsync(string binary, string name, CancellationToken ct)
    {
        var request = new ExternalProcessRequest
        {
            FileName = binary,
            Arguments = new[] { "--version" },
        };

        ExternalProcessResult result;
        try
        {
            result = await _processRunner.RunAsync(request, handleStdout: null, handleStdin: null, ct);
        }
        catch (Exception ex)
        {
            throw new RestorePermissionException(
                $"Бинарник {name} недоступен на хосте агента ({ex.Message}). " +
                "Установите пакет percona-xtrabackup и убедитесь, что он находится в PATH " +
                "(или задайте ConnectionConfig.BinPath).");
        }

        if (result.ExitCode != 0)
            throw new RestorePermissionException(
                $"{binary} --version вернул код {result.ExitCode}. " +
                "Убедитесь, что percona-xtrabackup установлен и " + name + " находится в PATH " +
                "(или задайте ConnectionConfig.BinPath).");
    }
}
