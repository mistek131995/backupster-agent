using BackupsterAgent.Providers.Secrets;
using Google.Cloud.SecretManager.V1;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class GoogleSecretManagerSecretBackendTests
{
    [Test]
    public void BuildEndpoint_EmptyLocationUsesDefaultEndpoint()
    {
        Assert.That(GoogleSecretManagerSecretBackend.BuildEndpoint(null), Is.Null);
        Assert.That(GoogleSecretManagerSecretBackend.BuildEndpoint(" "), Is.Null);
    }

    [Test]
    public void BuildEndpoint_ConfiguredLocationUsesRegionalEndpoint()
    {
        Assert.That(
            GoogleSecretManagerSecretBackend.BuildEndpoint(" europe-west1 "),
            Is.EqualTo("secretmanager.europe-west1.rep.googleapis.com"));
    }

    [Test]
    public void GetClientAsync_FailedClientCreationIsNotCached()
    {
        var calls = 0;
        var backend = new GoogleSecretManagerSecretBackend(_ =>
        {
            calls++;
            return Task.FromException<SecretManagerServiceClient>(
                new InvalidOperationException($"boom {calls}"));
        });

        var first = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await backend.GetClientAsync(" europe-west1 ", CancellationToken.None));
        var second = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await backend.GetClientAsync("europe-west1", CancellationToken.None));

        Assert.That(first?.Message, Is.EqualTo("boom 1"));
        Assert.That(second?.Message, Is.EqualTo("boom 2"));
        Assert.That(calls, Is.EqualTo(2));
    }

    [Test]
    public void GetClientAsync_SynchronousClientFactoryFailureIsNotCached()
    {
        var calls = 0;
        var backend = new GoogleSecretManagerSecretBackend(_ =>
        {
            calls++;
            throw new InvalidOperationException($"boom {calls}");
        });

        var first = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await backend.GetClientAsync(null, CancellationToken.None));
        var second = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await backend.GetClientAsync(" ", CancellationToken.None));

        Assert.That(first?.Message, Is.EqualTo("boom 1"));
        Assert.That(second?.Message, Is.EqualTo("boom 2"));
        Assert.That(calls, Is.EqualTo(2));
    }
}
