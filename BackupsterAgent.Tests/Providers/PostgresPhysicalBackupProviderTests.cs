using BackupsterAgent.Configuration;
using BackupsterAgent.Enums;
using BackupsterAgent.Providers.Backup;
using BackupsterAgent.Services.Backup;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Extensions.Logging.Abstractions;

namespace BackupsterAgent.Tests.Providers;

public sealed class PostgresPhysicalBackupProviderTests
{
    [Test]
    public async Task BackupAsync_PostWriteFailure_DeletesWrittenContainer()
    {
        var root = Path.Combine(Path.GetTempPath(), $"pg-physical-cleanup-{Guid.NewGuid():N}");
        var outputPath = Path.Combine(root, "out");
        var binPath = Path.Combine(root, "bin");

        Directory.CreateDirectory(outputPath);
        Directory.CreateDirectory(binPath);

        try
        {
            var database = "cleanupdb";
            var provider = new PostgresPhysicalBackupProvider(
                NullLogger<PostgresPhysicalBackupProvider>.Instance,
                new PostgresBinaryResolver(NullLogger<PostgresBinaryResolver>.Instance),
                new ManifestCollisionProcessRunner(database));

            var config = new DatabaseConfig
            {
                Database = database,
                OutputPath = outputPath,
            };

            var connection = new ConnectionConfig
            {
                Name = "pg-cleanup",
                DatabaseType = DatabaseType.Postgres,
                Host = "localhost",
                Port = -1,
                Username = "postgres",
                Password = string.Empty,
                BinPath = binPath,
            };

            var ex = Assert.CatchAsync<Exception>(
                () => provider.BackupAsync(config, connection, CancellationToken.None));
            Assert.That(ex, Is.TypeOf<IOException>().Or.TypeOf<UnauthorizedAccessException>());

            Assert.That(Directory.GetFiles(outputPath, $"*{PgBaseFormatDetector.ContainerExtension}"), Is.Empty);
            Assert.That(Directory.GetFiles(outputPath, "*.backup_manifest"), Is.Empty);
        }
        finally
        {
            try { if (Directory.Exists(root)) Directory.Delete(root, recursive: true); }
            catch { }
        }
    }

    [Test]
    public async Task BackupAsync_AdditionalPgBasebackupTarArchive_ThrowsBeforeCreatingContainer()
    {
        var root = Path.Combine(Path.GetTempPath(), $"pg-physical-tablespace-{Guid.NewGuid():N}");
        var outputPath = Path.Combine(root, "out");
        var binPath = Path.Combine(root, "bin");

        Directory.CreateDirectory(outputPath);
        Directory.CreateDirectory(binPath);

        try
        {
            var provider = new PostgresPhysicalBackupProvider(
                NullLogger<PostgresPhysicalBackupProvider>.Instance,
                new PostgresBinaryResolver(NullLogger<PostgresBinaryResolver>.Instance),
                new ExtraArchiveProcessRunner());

            var config = new DatabaseConfig
            {
                Database = "tablespacedb",
                OutputPath = outputPath,
            };

            var connection = new ConnectionConfig
            {
                Name = "pg-tablespace",
                DatabaseType = DatabaseType.Postgres,
                Host = "localhost",
                Port = -1,
                Username = "postgres",
                Password = string.Empty,
                BinPath = binPath,
            };

            var ex = Assert.ThrowsAsync<InvalidOperationException>(
                () => provider.BackupAsync(config, connection, CancellationToken.None));

            Assert.That(ex!.Message, Does.Contain("16384.tar.gz"));
            Assert.That(ex.Message, Does.Contain("tablespaces"));
            Assert.That(Directory.GetFiles(outputPath, $"*{PgBaseFormatDetector.ContainerExtension}"), Is.Empty);
            Assert.That(Directory.GetFiles(outputPath, "*.backup_manifest"), Is.Empty);
        }
        finally
        {
            try { if (Directory.Exists(root)) Directory.Delete(root, recursive: true); }
            catch { }
        }
    }

    [Test]
    public void WarnIfInsufficientWalSendersAsync_CancelledToken_PropagatesCancellation()
    {
        var provider = new PostgresPhysicalBackupProvider(
            NullLogger<PostgresPhysicalBackupProvider>.Instance,
            new PostgresBinaryResolver(NullLogger<PostgresBinaryResolver>.Instance),
            new StubProcessRunner());

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var connection = new ConnectionConfig
        {
            Name = "pg-cancelled",
            DatabaseType = DatabaseType.Postgres,
            Host = "localhost",
            Port = 5432,
            Username = "postgres",
            Password = string.Empty,
        };

        Assert.ThrowsAsync<OperationCanceledException>(
            () => provider.WarnIfInsufficientWalSendersAsync(connection, cts.Token));
    }

    private sealed class ManifestCollisionProcessRunner : IExternalProcessRunner
    {
        private readonly string _database;

        public ManifestCollisionProcessRunner(string database)
        {
            _database = database;
        }

        public async Task<ExternalProcessResult> RunAsync(
            ExternalProcessRequest request,
            Func<Stream, CancellationToken, Task>? handleStdout,
            Func<StreamWriter, CancellationToken, Task>? handleStdin,
            CancellationToken ct)
        {
            var tempDir = GetTempDir(request);
            var outputDir = Path.GetDirectoryName(tempDir)!;

            Directory.CreateDirectory(tempDir);

            await File.WriteAllBytesAsync(Path.Combine(tempDir, "base.tar.gz"), new byte[] { 1 }, ct);
            await File.WriteAllBytesAsync(Path.Combine(tempDir, "pg_wal.tar.gz"), new byte[] { 2 }, ct);
            await File.WriteAllBytesAsync(Path.Combine(tempDir, "backup_manifest"), new byte[] { 3 }, ct);

            for (var offset = -5; offset <= 5; offset++)
            {
                var timestamp = DateTime.UtcNow.AddSeconds(offset).ToString("yyyyMMdd_HHmmss");
                Directory.CreateDirectory(Path.Combine(outputDir, $"{_database}_{timestamp}.backup_manifest"));
            }

            return new ExternalProcessResult
            {
                ExitCode = 0,
                Stdout = string.Empty,
                Stderr = string.Empty,
            };
        }

        private static string GetTempDir(ExternalProcessRequest request)
        {
            for (var i = 0; i < request.Arguments.Count - 1; i++)
            {
                if (request.Arguments[i] == "-D") return request.Arguments[i + 1];
            }

            throw new InvalidOperationException("Missing pg_basebackup output directory argument.");
        }
    }

    private sealed class ExtraArchiveProcessRunner : IExternalProcessRunner
    {
        public async Task<ExternalProcessResult> RunAsync(
            ExternalProcessRequest request,
            Func<Stream, CancellationToken, Task>? handleStdout,
            Func<StreamWriter, CancellationToken, Task>? handleStdin,
            CancellationToken ct)
        {
            var tempDir = GetTempDir(request);
            Directory.CreateDirectory(tempDir);

            await File.WriteAllBytesAsync(Path.Combine(tempDir, "base.tar.gz"), new byte[] { 1 }, ct);
            await File.WriteAllBytesAsync(Path.Combine(tempDir, "pg_wal.tar.gz"), new byte[] { 2 }, ct);
            await File.WriteAllBytesAsync(Path.Combine(tempDir, "16384.tar.gz"), new byte[] { 3 }, ct);
            await File.WriteAllBytesAsync(Path.Combine(tempDir, "backup_manifest"), new byte[] { 4 }, ct);

            return new ExternalProcessResult
            {
                ExitCode = 0,
                Stdout = string.Empty,
                Stderr = string.Empty,
            };
        }

        private static string GetTempDir(ExternalProcessRequest request)
        {
            for (var i = 0; i < request.Arguments.Count - 1; i++)
            {
                if (request.Arguments[i] == "-D") return request.Arguments[i + 1];
            }

            throw new InvalidOperationException("Missing pg_basebackup output directory argument.");
        }
    }

    private sealed class StubProcessRunner : IExternalProcessRunner
    {
        public Task<ExternalProcessResult> RunAsync(
            ExternalProcessRequest request,
            Func<Stream, CancellationToken, Task>? handleStdout,
            Func<StreamWriter, CancellationToken, Task>? handleStdin,
            CancellationToken ct) =>
            Task.FromResult(new ExternalProcessResult
            {
                ExitCode = 0,
                Stdout = string.Empty,
                Stderr = string.Empty,
            });
    }
}
