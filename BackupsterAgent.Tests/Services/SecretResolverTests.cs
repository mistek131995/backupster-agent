using BackupsterAgent.Configuration;
using BackupsterAgent.Enums;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Secrets;
using Microsoft.Extensions.Logging.Abstractions;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class SecretResolverTests
{
    private string _tempRoot = null!;
    private SecretResolver _resolver = null!;

    [SetUp]
    public void SetUp()
    {
        _tempRoot = Path.Combine(Path.GetTempPath(), $"backupster-secrets-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempRoot);
        _resolver = new SecretResolver(NullLogger<SecretResolver>.Instance);
    }

    [TearDown]
    public void TearDown()
    {
        try { Directory.Delete(_tempRoot, recursive: true); }
        catch { }
    }

    [Test]
    public async Task ResolveStringAsync_FileSecretOverridesPlainAndTrimsTrailingNewline()
    {
        var path = Path.Combine(_tempRoot, "password");
        await File.WriteAllTextAsync(path, "from-file\r\n");

        var value = await _resolver.ResolveStringAsync(
            new SecretRef { Provider = "file", Path = path },
            "plain-value",
            "Connections['pg'].Password",
            CancellationToken.None);

        Assert.That(value, Is.EqualTo("from-file"));
    }

    [Test]
    public async Task ResolveStringAsync_NoSecretUsesPlainValue()
    {
        var value = await _resolver.ResolveStringAsync(
            secret: null,
            plainValue: "plain-value",
            settingPath: "Connections['pg'].Password",
            CancellationToken.None);

        Assert.That(value, Is.EqualTo("plain-value"));
    }

    [Test]
    public void ResolveStringAsync_EmptyFileThrows()
    {
        var path = Path.Combine(_tempRoot, "empty");
        File.WriteAllText(path, "\r\n");

        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => _resolver.ResolveStringAsync(
                new SecretRef { Provider = "file", Path = path },
                "plain-value",
                "Connections['pg'].Password",
                CancellationToken.None));

        Assert.That(ex!.Message, Is.Not.Empty);
    }

    [Test]
    public void ResolveStringAsync_MissingFileThrows()
    {
        var path = Path.Combine(_tempRoot, "missing");

        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => _resolver.ResolveStringAsync(
                new SecretRef { Provider = "file", Path = path },
                "plain-value",
                "Connections['pg'].Password",
                CancellationToken.None));

        Assert.That(ex!.Message, Is.Not.Empty);
    }

    [Test]
    public async Task ResolveConnectionAsync_ResolvesConfiguredSecretFieldsOnly()
    {
        var passwordPath = Path.Combine(_tempRoot, "password");
        var uriPath = Path.Combine(_tempRoot, "uri");
        await File.WriteAllTextAsync(passwordPath, "secret-password\n");
        await File.WriteAllTextAsync(uriPath, "mongodb://u:p@mongo.example.net:27017");

        var resolved = await _resolver.ResolveConnectionAsync(
            new ConnectionConfig
            {
                Name = "mongo",
                ConnectionUri = "plain-uri",
                ConnectionUriSecret = new SecretRef { Provider = "file", Path = uriPath },
                Username = "plain-user",
                Password = "plain-password",
                PasswordSecret = new SecretRef { Provider = "file", Path = passwordPath },
            },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(resolved.ConnectionUri, Is.EqualTo("mongodb://u:p@mongo.example.net:27017"));
            Assert.That(resolved.Username, Is.EqualTo("plain-user"));
            Assert.That(resolved.Password, Is.EqualTo("secret-password"));
        });
    }

    [Test]
    public async Task ResolveStorageAsync_ResolvesS3Keys()
    {
        var accessPath = Path.Combine(_tempRoot, "access");
        var secretPath = Path.Combine(_tempRoot, "secret");
        await File.WriteAllTextAsync(accessPath, "access-from-file\n");
        await File.WriteAllTextAsync(secretPath, "secret-from-file\n");

        var resolved = await _resolver.ResolveStorageAsync(
            new StorageConfig
            {
                Name = "s3-main",
                S3 = new S3Settings
                {
                    EndpointUrl = "https://s3.example.net",
                    AccessKey = "plain-access",
                    AccessKeySecret = new SecretRef { Provider = "file", Path = accessPath },
                    SecretKey = "plain-secret",
                    SecretKeySecret = new SecretRef { Provider = "file", Path = secretPath },
                    BucketName = "backups",
                },
            },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(resolved.S3, Is.Not.Null);
            Assert.That(resolved.S3!.AccessKey, Is.EqualTo("access-from-file"));
            Assert.That(resolved.S3.SecretKey, Is.EqualTo("secret-from-file"));
            Assert.That(resolved.S3.EndpointUrl, Is.EqualTo("https://s3.example.net"));
            Assert.That(resolved.S3.BucketName, Is.EqualTo("backups"));
        });
    }

    [Test]
    public async Task ResolveStorageAsync_AzureConnectionStringSecretDoesNotReadAccountKeySecret()
    {
        var connectionStringPath = Path.Combine(_tempRoot, "azure-connection-string");
        var missingAccountKeyPath = Path.Combine(_tempRoot, "missing-account-key");
        await File.WriteAllTextAsync(
            connectionStringPath,
            "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=key;EndpointSuffix=core.windows.net\n");

        var resolved = await _resolver.ResolveStorageAsync(
            new StorageConfig
            {
                Name = "azure-main",
                Provider = UploadProvider.AzureBlob,
                AzureBlob = new AzureBlobSettings
                {
                    ConnectionStringSecret = new SecretRef { Provider = "file", Path = connectionStringPath },
                    AccountName = "acct",
                    AccountKeySecret = new SecretRef { Provider = "file", Path = missingAccountKeyPath },
                    ServiceUri = "https://acct.blob.core.windows.net",
                    ContainerName = "backups",
                },
            },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(resolved.AzureBlob, Is.Not.Null);
            Assert.That(resolved.AzureBlob!.ConnectionString, Does.Contain("AccountName=acct"));
            Assert.That(resolved.AzureBlob.AccountKey, Is.Null);
        });
    }
}
