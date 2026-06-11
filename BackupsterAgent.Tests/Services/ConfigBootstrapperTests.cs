using System.Text.Json;
using BackupsterAgent.Services.Common;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class ConfigBootstrapperTests
{
    private string _configDir = null!;

    [SetUp]
    public void SetUp()
    {
        _configDir = Path.Combine(Path.GetTempPath(), $"config-bootstrapper-test-{Guid.NewGuid():N}");
    }

    [TearDown]
    public void TearDown()
    {
        try { Directory.Delete(_configDir, recursive: true); }
        catch { }
    }

    [Test]
    public void EnsureTemplate_NoConfig_CreatesTemplateWithGeneratedKey()
    {
        var result = ConfigBootstrapper.EnsureTemplate(_configDir);

        Assert.That(result.TemplateCreated, Is.True);
        Assert.That(result.Failure, Is.Null);
        var key = ReadEncryptionKey();
        Assert.That(key, Is.Not.Null.And.Not.Empty);
        Assert.That(Convert.FromBase64String(key!), Has.Length.EqualTo(32));
    }

    [Test]
    public void EnsureTemplate_CalledForDifferentDirs_GeneratesDistinctKeys()
    {
        var otherDir = Path.Combine(Path.GetTempPath(), $"config-bootstrapper-test-{Guid.NewGuid():N}");

        try
        {
            ConfigBootstrapper.EnsureTemplate(_configDir);
            ConfigBootstrapper.EnsureTemplate(otherDir);

            var first = ReadEncryptionKey();
            var second = ReadEncryptionKey(otherDir);
            Assert.That(first, Is.Not.EqualTo(second));
        }
        finally
        {
            try { Directory.Delete(otherDir, recursive: true); }
            catch { }
        }
    }

    [Test]
    public void EnsureTemplate_OnUnix_RestrictsAccessToOwner()
    {
        if (OperatingSystem.IsWindows())
        {
            Assert.Ignore("Unix file modes are not applicable on Windows.");
            return;
        }

        var result = ConfigBootstrapper.EnsureTemplate(_configDir);

        Assert.That(result.TemplateCreated, Is.True);
        var fileMode = File.GetUnixFileMode(result.FilePath);
        var dirMode = File.GetUnixFileMode(_configDir);
        Assert.Multiple(() =>
        {
            Assert.That(fileMode, Is.EqualTo(UnixFileMode.UserRead | UnixFileMode.UserWrite));
            Assert.That(dirMode, Is.EqualTo(UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute));
        });
    }

    [Test]
    public void EnsureTemplate_ExistingConfig_DoesNotOverwrite()
    {
        Directory.CreateDirectory(_configDir);
        var filePath = Path.Combine(_configDir, "appsettings.json");
        var existing = """{"EncryptionSettings":{"Key":"user-key"}}""";
        File.WriteAllText(filePath, existing);

        var result = ConfigBootstrapper.EnsureTemplate(_configDir);

        Assert.That(result.TemplateCreated, Is.False);
        Assert.That(result.Failure, Is.Null);
        Assert.That(File.ReadAllText(filePath), Is.EqualTo(existing));
    }

    private string? ReadEncryptionKey(string? configDir = null)
    {
        var filePath = Path.Combine(configDir ?? _configDir, "appsettings.json");
        using var doc = JsonDocument.Parse(File.ReadAllText(filePath));
        return doc.RootElement.GetProperty("EncryptionSettings").GetProperty("Key").GetString();
    }
}
