using BackupsterAgent.Configuration;
using BackupsterAgent.Enums;
using BackupsterAgent.Providers.Upload;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Services.Common.Secrets;
using Microsoft.Extensions.Logging.Abstractions;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class UploadProviderFactoryTests
{
    private string _tempRoot = null!;

    [SetUp]
    public void SetUp()
    {
        _tempRoot = Path.Combine(Path.GetTempPath(), $"backupster-upload-factory-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempRoot);
    }

    [TearDown]
    public void TearDown()
    {
        try { Directory.Delete(_tempRoot, recursive: true); }
        catch { }
    }

    [Test]
    public async Task GetProviderAsync_FileSecretChange_ReturnsCachedProviderUntilRestart()
    {
        var secretPath = Path.Combine(_tempRoot, "s3-secret");
        await File.WriteAllTextAsync(secretPath, "secret-v1");

        var storage = new StorageConfig
        {
            Name = "s3-main",
            Provider = UploadProvider.S3,
            S3 = new S3Settings
            {
                EndpointUrl = "https://s3.example.test",
                AccessKey = "access",
                SecretKeySecret = new SecretRef { Provider = "file", Path = secretPath },
                BucketName = "backups",
                Region = "us-east-1",
            },
        };

        await using var factory = new UploadProviderFactory(
            new StorageResolver([storage]),
            new SecretResolver(NullLogger<SecretResolver>.Instance),
            NullLoggerFactory.Instance);

        var first = await factory.GetProviderAsync("s3-main", CancellationToken.None);

        await File.WriteAllTextAsync(secretPath, "secret-v2");

        var second = await factory.GetProviderAsync("s3-main", CancellationToken.None);

        Assert.That(second, Is.SameAs(first));
    }
}
