using System.Security.Cryptography;
using BackupsterAgent.Services.Upload;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class SftpUploadServiceTests
{
    [Test]
    public void ComputeFingerprint_MatchesOpensshFormat()
    {
        var hostKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPublicKeyBytesExampleHere"u8.ToArray();
        var expected = "SHA256:" + Convert.ToBase64String(SHA256.HashData(hostKey)).TrimEnd('=');

        var actual = SftpUploadService.ComputeFingerprint(hostKey);

        Assert.That(actual, Is.EqualTo(expected));
        Assert.That(actual, Does.StartWith("SHA256:"));
        Assert.That(actual, Does.Not.EndWith("="), "openssh-style fingerprint has no base64 padding");
    }

    [Test]
    public void ComputeFingerprint_DifferentKeys_ProduceDifferentFingerprints()
    {
        var a = SftpUploadService.ComputeFingerprint([1, 2, 3]);
        var b = SftpUploadService.ComputeFingerprint([1, 2, 4]);
        Assert.That(a, Is.Not.EqualTo(b));
    }
}
