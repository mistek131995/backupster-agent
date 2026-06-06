using BackupsterAgent.Configuration;
using BackupsterAgent.Enums;
using BackupsterAgent.Providers.Backup;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Extensions.Logging.Abstractions;

namespace BackupsterAgent.Tests.Providers;

public sealed class MongoLogicalBackupProviderTests
{
    [Test]
    public async Task BackupAsync_WhenMongodumpFails_DeletesPartialArchiveAndToolConfig()
    {
        var root = Path.Combine(Path.GetTempPath(), $"backupster-mongo-provider-test-{Guid.NewGuid():N}");
        var outputPath = Path.Combine(root, "out");
        var binPath = Path.Combine(root, "bin");

        Directory.CreateDirectory(outputPath);
        Directory.CreateDirectory(binPath);

        try
        {
            var runner = new FailingMongodumpRunner();
            var provider = new MongoLogicalBackupProvider(
                NullLogger<MongoLogicalBackupProvider>.Instance,
                new MongoBinaryResolver(NullLogger<MongoBinaryResolver>.Instance),
                runner);

            var config = new DatabaseConfig
            {
                ConnectionName = "mongo-main",
                StorageName = "n/a",
                Database = "cleanupdb",
                OutputPath = outputPath,
            };

            var connection = new ConnectionConfig
            {
                Name = "mongo-main",
                DatabaseType = DatabaseType.MongoDb,
                Host = "127.0.0.1",
                Port = 27017,
                Username = "backup_user",
                Password = "password",
                BinPath = binPath,
            };

            var ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
                provider.BackupAsync(config, connection, CancellationToken.None));

            Assert.Multiple(() =>
            {
                Assert.That(ex!.Message, Does.Contain("mongodump"));
                Assert.That(ex.Message, Does.Contain("simulated failure"));
                Assert.That(Directory.GetFiles(outputPath, "*.archive.gz"), Is.Empty);
                Assert.That(runner.ConfigPath, Is.Not.Null);
                Assert.That(Directory.Exists(Path.GetDirectoryName(runner.ConfigPath!)), Is.False);
            });
        }
        finally
        {
            try { if (Directory.Exists(root)) Directory.Delete(root, recursive: true); }
            catch { }
        }
    }

    private sealed class FailingMongodumpRunner : IExternalProcessRunner
    {
        public string? ConfigPath { get; private set; }

        public async Task<ExternalProcessResult> RunAsync(
            ExternalProcessRequest request,
            Func<Stream, CancellationToken, Task>? handleStdout,
            Func<StreamWriter, CancellationToken, Task>? handleStdin,
            CancellationToken ct)
        {
            ConfigPath = request.Arguments
                .Single(arg => arg.StartsWith("--config=", StringComparison.Ordinal))
                ["--config=".Length..];

            Assert.That(request.Arguments, Does.Contain("--db=cleanupdb"));
            Assert.That(request.Arguments, Does.Contain("--archive"));
            Assert.That(File.Exists(ConfigPath), Is.True);

            if (handleStdout is not null)
            {
                using var stdout = new MemoryStream(new byte[] { 1, 2, 3, 4 });
                await handleStdout(stdout, ct);
            }

            return new ExternalProcessResult
            {
                ExitCode = 2,
                Stdout = string.Empty,
                Stderr = "simulated failure",
            };
        }
    }
}
