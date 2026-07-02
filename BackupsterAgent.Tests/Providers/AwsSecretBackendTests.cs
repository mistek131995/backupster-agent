using System.Reflection;
using System.Text;
using Amazon.Runtime;
using Amazon.SecretsManager;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;

namespace BackupsterAgent.Tests.Providers;

[TestFixture]
public sealed class AwsSecretBackendTests
{
    [Test]
    public void ApplyAwsClientConfig_KeepsServiceUrlWhenRegionIsConfigured()
    {
        const string serviceUrl = "http://localhost:4566";
        const string region = "us-east-1";
        var config = new AmazonSecretsManagerConfig();
        var secret = new SecretRef
        {
            Provider = "aws-secrets-manager",
            Name = "prod/db/password",
            ServiceUrl = serviceUrl,
            Region = region,
        };

        ApplyAwsClientConfig(config, secret);

        Assert.Multiple(() =>
        {
            Assert.That(config.ServiceURL, Does.StartWith(serviceUrl));
            Assert.That(config.AuthenticationRegion, Is.EqualTo(region));
        });
    }

    [Test]
    public async Task ReadBinarySecretAsync_DecodesValidUtf8SecretBinary()
    {
        var value = await ReadBinarySecretAsync(Encoding.UTF8.GetBytes("from-binary"));

        Assert.That(value, Is.EqualTo("from-binary"));
    }

    [Test]
    public void ReadBinarySecretAsync_InvalidUtf8ThrowsSecretResolutionException()
    {
        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => ReadBinarySecretAsync([0x66, 0x6F, 0x80, 0x6F]));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Is.Not.Empty);
            Assert.That(ex.InnerException, Is.TypeOf<DecoderFallbackException>());
        });
    }

    [Test]
    public void BuildClientCacheKey_NormalizesRegionCaseAndIgnoresProvider()
    {
        var first = BuildClientCacheKey(new SecretRef
        {
            Provider = "aws-secrets-manager",
            Region = " us-east-1 ",
            ServiceUrl = " http://localhost:4566 ",
        });
        var second = BuildClientCacheKey(new SecretRef
        {
            Provider = "aws-ssm-parameter",
            Region = "US-EAST-1",
            ServiceUrl = "http://localhost:4566",
        });

        Assert.That(second, Is.EqualTo(first));
    }

    private static void ApplyAwsClientConfig(ClientConfig config, SecretRef secret)
    {
        var method = GetBackendType().GetMethod(
            "ApplyAwsClientConfig",
            BindingFlags.NonPublic | BindingFlags.Static);

        Assert.That(method, Is.Not.Null);
        method!.Invoke(null, [config, secret]);
    }

    private static async Task<string> ReadBinarySecretAsync(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        var method = GetBackendType().GetMethod(
            "ReadBinarySecretAsync",
            BindingFlags.NonPublic | BindingFlags.Static);

        Assert.That(method, Is.Not.Null);
        var result = method!.Invoke(null, [stream, "Connections['pg'].Password", CancellationToken.None]);
        Assert.That(result, Is.InstanceOf<Task<string>>());
        return await (Task<string>)result!;
    }

    private static string BuildClientCacheKey(SecretRef secret)
    {
        var method = GetBackendType().GetMethod(
            "BuildClientCacheKey",
            BindingFlags.NonPublic | BindingFlags.Static);

        Assert.That(method, Is.Not.Null);
        var result = method!.Invoke(null, [secret]);
        Assert.That(result, Is.InstanceOf<string>());
        return (string)result!;
    }

    private static Type GetBackendType() =>
        typeof(SecretRef).Assembly.GetType(
            "BackupsterAgent.Providers.Secrets.AwsSecretBackend",
            throwOnError: true)!;
}
