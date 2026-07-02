using BackupsterAgent.Configuration;
using BackupsterAgent.Enums;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Providers.Secrets;
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
    public async Task ResolveStringAsync_EnvSecretOverridesPlainAndTrimsTrailingNewline()
    {
        var name = NewEnvName();
        Environment.SetEnvironmentVariable(name, "from-env\r\n");

        try
        {
            var value = await _resolver.ResolveStringAsync(
                new SecretRef { Provider = "env", Name = name },
                "plain-value",
                "Connections['pg'].Password",
                CancellationToken.None);

            Assert.That(value, Is.EqualTo("from-env"));
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    [Test]
    public void ResolveString_EnvProviderIsCaseInsensitive()
    {
        var name = NewEnvName();
        Environment.SetEnvironmentVariable(name, "from-env");

        try
        {
            var value = _resolver.ResolveString(
                new SecretRef { Provider = "ENV", Name = name },
                "plain-value",
                "Connections['pg'].Password");

            Assert.That(value, Is.EqualTo("from-env"));
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    [Test]
    public async Task ResolveStringAsync_EnvSecretNameTrimsOuterWhitespace()
    {
        var name = NewEnvName();
        Environment.SetEnvironmentVariable(name, "from-env");

        try
        {
            var value = await _resolver.ResolveStringAsync(
                new SecretRef { Provider = "env", Name = $" {name} " },
                "plain-value",
                "Connections['pg'].Password",
                CancellationToken.None);

            Assert.That(value, Is.EqualTo("from-env"));
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    [Test]
    public void ResolveStringAsync_EnvUnsupportedSecretRefFieldsThrow()
    {
        var name = NewEnvName();
        Environment.SetEnvironmentVariable(name, "{\"password\":\"from-json\"}");

        try
        {
            var ex = Assert.ThrowsAsync<SecretResolutionException>(
                () => _resolver.ResolveStringAsync(
                    new SecretRef
                    {
                        Provider = "env",
                        Name = name,
                        Path = "unused",
                        Region = "eu-central-1",
                        ServiceUrl = "https://example.invalid",
                        JsonKey = "password",
                        VersionStage = "AWSCURRENT",
                        VersionId = "1",
                        WithDecryption = true,
                    },
                    "plain-value",
                    "Connections['pg'].Password",
                    CancellationToken.None));

            Assert.Multiple(() =>
            {
                Assert.That(ex!.Message, Does.Contain(nameof(SecretRef.JsonKey)));
                Assert.That(ex.Message, Does.Contain(nameof(SecretRef.Region)));
                Assert.That(ex.Message, Does.Contain(nameof(SecretRef.VersionStage)));
            });
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    [Test]
    public async Task ResolveStringAsync_AwsSecretOverridesPlainAndTrimsTrailingNewline()
    {
        var aws = new FakeSecretProvider("aws-secrets-manager", supportsSynchronousReads: false, "from-aws\n");
        var resolver = new SecretResolver(new SecretProviderFactory([aws]));

        var value = await resolver.ResolveStringAsync(
            new SecretRef { Provider = "aws-secrets-manager", Name = "prod/db/password" },
            "plain-value",
            "Connections['pg'].Password",
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo("from-aws"));
            Assert.That(aws.AsyncCalls, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ResolveStringAsync_AwsReaderUsesFactoryNormalizedProvider()
    {
        var reader = new AwsSecretReader(
            new FakeAwsSecretBackend(
                (_, _, _) => Task.FromResult("from-secrets-manager"),
                (_, _, _) => throw new InvalidOperationException("SSM should not be called")),
            NullLogger<AwsSecretReader>.Instance);
        var resolver = new SecretResolver(new SecretProviderFactory([reader]));

        var value = await resolver.ResolveStringAsync(
            new SecretRef { Provider = " AWS-SECRETS-MANAGER ", Name = "prod/db/password" },
            "plain-value",
            "Connections['pg'].Password",
            CancellationToken.None);

        Assert.That(value, Is.EqualTo("from-secrets-manager"));
    }

    [Test]
    public async Task ResolveStringAsync_CustomProviderFromFactoryOverridesPlainAndTrimsTrailingNewline()
    {
        var provider = new FakeSecretProvider("external-vault", supportsSynchronousReads: false, "from-external\n");
        var resolver = new SecretResolver(new SecretProviderFactory([provider]));

        var value = await resolver.ResolveStringAsync(
            new SecretRef { Provider = "external-vault", Name = "prod/db/password" },
            "plain-value",
            "Connections['pg'].Password",
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo("from-external"));
            Assert.That(provider.AsyncCalls, Is.EqualTo(1));
        });
    }

    [Test]
    public void RequiresAsyncResolution_UsesProviderCapability()
    {
        var asyncProvider = new FakeSecretProvider("external-vault", supportsSynchronousReads: false, "from-external");
        var syncProvider = new FakeSecretProvider("local-vault", supportsSynchronousReads: true, "from-local");
        var resolver = new SecretResolver(new SecretProviderFactory([asyncProvider, syncProvider]));

        Assert.Multiple(() =>
        {
            Assert.That(
                resolver.RequiresAsyncResolution(new SecretRef { Provider = "external-vault", Name = "secret" }),
                Is.True);
            Assert.That(
                resolver.RequiresAsyncResolution(new SecretRef { Provider = "local-vault", Name = "secret" }),
                Is.False);
            Assert.That(
                resolver.RequiresAsyncResolution(new SecretRef { Provider = "unknown-vault", Name = "secret" }),
                Is.False);
        });
    }

    [Test]
    public async Task AwsSecretReader_SecretsManagerJsonKeyReadsCurrentValueEachTime()
    {
        var calls = 0;
        SecretRef? observed = null;
        var reader = new AwsSecretReader(
            new FakeAwsSecretBackend(
                (secret, _, _) =>
                {
                    calls++;
                    observed = secret;
                    return Task.FromResult($"{{\"password\":\"from-json-{calls}\"}}");
                },
                (_, _, _) => throw new InvalidOperationException("SSM should not be called")),
            NullLogger<AwsSecretReader>.Instance);

        var secret = new SecretRef
        {
            Provider = "aws-secrets-manager",
            Name = "prod/db",
            Region = "eu-central-1",
            JsonKey = "password",
            VersionStage = "AWSCURRENT",
        };

        var first = await reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None);
        var second = await reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo("from-json-1"));
            Assert.That(second, Is.EqualTo("from-json-2"));
            Assert.That(calls, Is.EqualTo(2));
            Assert.That(observed, Is.SameAs(secret));
        });
    }

    [Test]
    public void AwsSecretReader_SecretsManagerMissingJsonKeyThrows()
    {
        var reader = new AwsSecretReader(
            new FakeAwsSecretBackend(
                (_, _, _) => Task.FromResult("{\"username\":\"backup\"}"),
                (_, _, _) => throw new InvalidOperationException("SSM should not be called")),
            NullLogger<AwsSecretReader>.Instance);

        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => reader.ReadAsync(
                new SecretRef
                {
                    Provider = "aws-secrets-manager",
                    Name = "prod/db",
                    JsonKey = "password",
                },
                "Connections['pg'].Password",
                CancellationToken.None));

        Assert.That(ex!.Message, Is.Not.Empty);
    }

    [Test]
    public async Task AwsSecretReader_SsmParameterReadsCurrentValueEachTime()
    {
        var calls = 0;
        bool? observedWithDecryption = null;
        var reader = new AwsSecretReader(
            new FakeAwsSecretBackend(
                (_, _, _) => throw new InvalidOperationException("Secrets Manager should not be called"),
                (secret, _, _) =>
                {
                    calls++;
                    observedWithDecryption = secret.WithDecryption;
                    return Task.FromResult($"from-ssm-{calls}");
                }),
            NullLogger<AwsSecretReader>.Instance);

        var secret = new SecretRef
        {
            Provider = "aws-ssm-parameter",
            Name = "/backupster/prod/db/password",
            Region = "eu-central-1",
            WithDecryption = true,
        };

        var first = await reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None);
        var second = await reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo("from-ssm-1"));
            Assert.That(second, Is.EqualTo("from-ssm-2"));
            Assert.That(calls, Is.EqualTo(2));
            Assert.That(observedWithDecryption, Is.True);
        });
    }

    [Test]
    public async Task AwsSecretReader_SsmParameterJsonKeyReadsConfiguredValue()
    {
        var reader = new AwsSecretReader(
            new FakeAwsSecretBackend(
                (_, _, _) => throw new InvalidOperationException("Secrets Manager should not be called"),
                (_, _, _) => Task.FromResult("{\"password\":\"from-ssm-json\"}")),
            NullLogger<AwsSecretReader>.Instance);

        var value = await reader.ReadAsync(
            new SecretRef
            {
                Provider = "aws-ssm-parameter",
                Name = "/backupster/prod/db/password",
                JsonKey = "password",
            },
            "Connections['pg'].Password",
            CancellationToken.None);

        Assert.That(value, Is.EqualTo("from-ssm-json"));
    }

    [Test]
    public async Task ResolveStringAsync_AzureReaderUsesFactoryNormalizedProvider()
    {
        var reader = new AzureSecretReader(
            new FakeAzureSecretBackend((_, _, _, _, _) => Task.FromResult("from-key-vault\n")),
            NullLogger<AzureSecretReader>.Instance);
        var resolver = new SecretResolver(new SecretProviderFactory([reader]));

        var value = await resolver.ResolveStringAsync(
            new SecretRef
            {
                Provider = " AZURE-KEY-VAULT ",
                ServiceUrl = "https://prod-vault.vault.azure.net",
                Name = "db-password",
            },
            "plain-value",
            "Connections['pg'].Password",
            CancellationToken.None);

        Assert.That(value, Is.EqualTo("from-key-vault"));
    }

    [Test]
    public void RequiresAsyncResolution_AzureReaderRequiresAsync()
    {
        var reader = new AzureSecretReader(
            new FakeAzureSecretBackend((_, _, _, _, _) => Task.FromResult("unused")),
            NullLogger<AzureSecretReader>.Instance);
        var resolver = new SecretResolver(new SecretProviderFactory([reader]));

        Assert.That(
            resolver.RequiresAsyncResolution(new SecretRef { Provider = "azure-key-vault", Name = "secret" }),
            Is.True);
    }

    [Test]
    public void ResolveString_AzureReaderRejectsSynchronousRead()
    {
        var reader = new AzureSecretReader(
            new FakeAzureSecretBackend((_, _, _, _, _) => Task.FromResult("unused")),
            NullLogger<AzureSecretReader>.Instance);
        var resolver = new SecretResolver(new SecretProviderFactory([reader]));

        var ex = Assert.Throws<SecretResolutionException>(
            () => resolver.ResolveString(
                new SecretRef
                {
                    Provider = "azure-key-vault",
                    ServiceUrl = "https://prod-vault.vault.azure.net",
                    Name = "db-password",
                },
                "plain-value",
                "Connections['pg'].Password"));

        Assert.That(ex!.Message, Is.Not.Empty);
    }

    [Test]
    public async Task AzureSecretReader_ReadsCurrentValueEachTime()
    {
        var calls = 0;
        Uri? observedVaultUri = null;
        string? observedName = null;
        string? observedVersion = null;
        var reader = new AzureSecretReader(
            new FakeAzureSecretBackend((vaultUri, name, version, _, _) =>
            {
                calls++;
                observedVaultUri = vaultUri;
                observedName = name;
                observedVersion = version;
                return Task.FromResult($"from-key-vault-{calls}");
            }),
            NullLogger<AzureSecretReader>.Instance);

        var secret = new SecretRef
        {
            Provider = "azure-key-vault",
            ServiceUrl = "https://prod-vault.vault.azure.net",
            Name = " db-password ",
            VersionId = "abc123",
        };

        var first = await reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None);
        var second = await reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo("from-key-vault-1"));
            Assert.That(second, Is.EqualTo("from-key-vault-2"));
            Assert.That(calls, Is.EqualTo(2));
            Assert.That(observedVaultUri, Is.EqualTo(new Uri("https://prod-vault.vault.azure.net")));
            Assert.That(observedName, Is.EqualTo("db-password"));
            Assert.That(observedVersion, Is.EqualTo("abc123"));
        });
    }

    [Test]
    public async Task AzureSecretReader_JsonKeyReadsConfiguredValue()
    {
        var reader = new AzureSecretReader(
            new FakeAzureSecretBackend((_, _, _, _, _) => Task.FromResult("{\"password\":\"from-json\"}")),
            NullLogger<AzureSecretReader>.Instance);

        var value = await reader.ReadAsync(
            new SecretRef
            {
                Provider = "azure-key-vault",
                ServiceUrl = "https://prod-vault.vault.azure.net",
                Name = "db-credentials",
                JsonKey = "password",
            },
            "Connections['pg'].Password",
            CancellationToken.None);

        Assert.That(value, Is.EqualTo("from-json"));
    }

    [Test]
    public void AzureSecretReader_MissingJsonKeyThrows()
    {
        var reader = new AzureSecretReader(
            new FakeAzureSecretBackend((_, _, _, _, _) => Task.FromResult("{\"username\":\"backup\"}")),
            NullLogger<AzureSecretReader>.Instance);

        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => reader.ReadAsync(
                new SecretRef
                {
                    Provider = "azure-key-vault",
                    ServiceUrl = "https://prod-vault.vault.azure.net",
                    Name = "db-credentials",
                    JsonKey = "password",
                },
                "Connections['pg'].Password",
                CancellationToken.None));

        Assert.That(ex!.Message, Is.Not.Empty);
    }

    [Test]
    public void AzureSecretReader_MissingServiceUrlThrows()
    {
        var reader = new AzureSecretReader(
            new FakeAzureSecretBackend((_, _, _, _, _) =>
                throw new InvalidOperationException("Backend should not be called")),
            NullLogger<AzureSecretReader>.Instance);

        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => reader.ReadAsync(
                new SecretRef { Provider = "azure-key-vault", Name = "db-password" },
                "Connections['pg'].Password",
                CancellationToken.None));

        Assert.That(ex!.Message, Is.Not.Empty);
    }

    [Test]
    public void AzureSecretReader_InvalidServiceUrlThrows()
    {
        var reader = new AzureSecretReader(
            new FakeAzureSecretBackend((_, _, _, _, _) =>
                throw new InvalidOperationException("Backend should not be called")),
            NullLogger<AzureSecretReader>.Instance);

        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => reader.ReadAsync(
                new SecretRef
                {
                    Provider = "azure-key-vault",
                    ServiceUrl = "prod-vault.vault.azure.net",
                    Name = "db-password",
                },
                "Connections['pg'].Password",
                CancellationToken.None));

        Assert.That(ex!.Message, Is.Not.Empty);
    }

    [Test]
    public void AzureSecretReader_MissingNameThrows()
    {
        var reader = new AzureSecretReader(
            new FakeAzureSecretBackend((_, _, _, _, _) =>
                throw new InvalidOperationException("Backend should not be called")),
            NullLogger<AzureSecretReader>.Instance);

        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => reader.ReadAsync(
                new SecretRef
                {
                    Provider = "azure-key-vault",
                    ServiceUrl = "https://prod-vault.vault.azure.net",
                },
                "Connections['pg'].Password",
                CancellationToken.None));

        Assert.That(ex!.Message, Is.Not.Empty);
    }

    [Test]
    public void AzureSecretReader_BackendFailureWrapsIntoSecretResolutionException()
    {
        var reader = new AzureSecretReader(
            new FakeAzureSecretBackend((_, _, _, _, _) =>
                throw new Azure.RequestFailedException(403, "Forbidden")),
            NullLogger<AzureSecretReader>.Instance);

        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => reader.ReadAsync(
                new SecretRef
                {
                    Provider = "azure-key-vault",
                    ServiceUrl = "https://prod-vault.vault.azure.net",
                    Name = "db-password",
                },
                "Connections['pg'].Password",
                CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Is.Not.Empty);
            Assert.That(ex.InnerException, Is.InstanceOf<Azure.RequestFailedException>());
        });
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
    public void ResolveStringAsync_EmptyProviderThrows()
    {
        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => _resolver.ResolveStringAsync(
                new SecretRef { Provider = "", Path = "unused" },
                "plain-value",
                "Connections['pg'].Password",
                CancellationToken.None));

        Assert.That(ex!.Message, Is.Not.Empty);
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
    public void ResolveStringAsync_MissingEnvNameThrows()
    {
        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => _resolver.ResolveStringAsync(
                new SecretRef { Provider = "env" },
                "plain-value",
                "Connections['pg'].Password",
                CancellationToken.None));

        Assert.That(ex!.Message, Is.Not.Empty);
    }

    [Test]
    public void ResolveStringAsync_MissingEnvThrows()
    {
        var name = NewEnvName();

        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => _resolver.ResolveStringAsync(
                new SecretRef { Provider = "env", Name = name },
                "plain-value",
                "Connections['pg'].Password",
                CancellationToken.None));

        Assert.That(ex!.Message, Is.Not.Empty);
    }

    [Test]
    public void ResolveStringAsync_EmptyEnvThrows()
    {
        var name = NewEnvName();
        Environment.SetEnvironmentVariable(name, "\r\n");

        try
        {
            var ex = Assert.ThrowsAsync<SecretResolutionException>(
                () => _resolver.ResolveStringAsync(
                    new SecretRef { Provider = "env", Name = name },
                    "plain-value",
                    "Connections['pg'].Password",
                    CancellationToken.None));

            Assert.That(ex!.Message, Does.Contain("имеет пустое значение"));
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
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
    public async Task ResolveConnectionAsync_ResolvesEnvSecrets()
    {
        var passwordName = NewEnvName();
        var uriName = NewEnvName();
        Environment.SetEnvironmentVariable(passwordName, "secret-password");
        Environment.SetEnvironmentVariable(uriName, "mongodb://u:p@mongo.example.net:27017");

        try
        {
            var resolved = await _resolver.ResolveConnectionAsync(
                new ConnectionConfig
                {
                    Name = "mongo",
                    ConnectionUri = "plain-uri",
                    ConnectionUriSecret = new SecretRef { Provider = "env", Name = uriName },
                    Username = "plain-user",
                    Password = "plain-password",
                    PasswordSecret = new SecretRef { Provider = "env", Name = passwordName },
                },
                CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(resolved.ConnectionUri, Is.EqualTo("mongodb://u:p@mongo.example.net:27017"));
                Assert.That(resolved.Username, Is.EqualTo("plain-user"));
                Assert.That(resolved.Password, Is.EqualTo("secret-password"));
            });
        }
        finally
        {
            Environment.SetEnvironmentVariable(passwordName, null);
            Environment.SetEnvironmentVariable(uriName, null);
        }
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
    public async Task ResolveStorageAsync_ResolvesS3EnvKeys()
    {
        var accessName = NewEnvName();
        var secretName = NewEnvName();
        Environment.SetEnvironmentVariable(accessName, "access-from-env");
        Environment.SetEnvironmentVariable(secretName, "secret-from-env");

        try
        {
            var resolved = await _resolver.ResolveStorageAsync(
                new StorageConfig
                {
                    Name = "s3-main",
                    S3 = new S3Settings
                    {
                        EndpointUrl = "https://s3.example.net",
                        AccessKey = "plain-access",
                        AccessKeySecret = new SecretRef { Provider = "env", Name = accessName },
                        SecretKey = "plain-secret",
                        SecretKeySecret = new SecretRef { Provider = "env", Name = secretName },
                        BucketName = "backups",
                    },
                },
                CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(resolved.S3, Is.Not.Null);
                Assert.That(resolved.S3!.AccessKey, Is.EqualTo("access-from-env"));
                Assert.That(resolved.S3.SecretKey, Is.EqualTo("secret-from-env"));
            });
        }
        finally
        {
            Environment.SetEnvironmentVariable(accessName, null);
            Environment.SetEnvironmentVariable(secretName, null);
        }
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

    private static string NewEnvName() =>
        $"BACKUPSTER_TEST_SECRET_{Guid.NewGuid():N}".ToUpperInvariant();

    private sealed class FakeAwsSecretBackend : IAwsSecretBackend
    {
        private readonly Func<SecretRef, string, CancellationToken, Task<string>> _secretsManagerValueReader;
        private readonly Func<SecretRef, string, CancellationToken, Task<string>> _ssmParameterValueReader;

        public FakeAwsSecretBackend(
            Func<SecretRef, string, CancellationToken, Task<string>> secretsManagerValueReader,
            Func<SecretRef, string, CancellationToken, Task<string>> ssmParameterValueReader)
        {
            _secretsManagerValueReader = secretsManagerValueReader;
            _ssmParameterValueReader = ssmParameterValueReader;
        }

        public Task<string> ReadSecretsManagerValueAsync(
            SecretRef secret,
            string settingPath,
            CancellationToken ct) =>
            _secretsManagerValueReader(secret, settingPath, ct);

        public Task<string> ReadSsmParameterValueAsync(
            SecretRef secret,
            string settingPath,
            CancellationToken ct) =>
            _ssmParameterValueReader(secret, settingPath, ct);
    }

    private sealed class FakeAzureSecretBackend : IAzureSecretBackend
    {
        private readonly Func<Uri, string, string?, string, CancellationToken, Task<string>> _secretValueReader;

        public FakeAzureSecretBackend(
            Func<Uri, string, string?, string, CancellationToken, Task<string>> secretValueReader)
        {
            _secretValueReader = secretValueReader;
        }

        public Task<string> ReadSecretValueAsync(
            Uri vaultUri,
            string secretName,
            string? version,
            string settingPath,
            CancellationToken ct) =>
            _secretValueReader(vaultUri, secretName, version, settingPath, ct);
    }

    private sealed class FakeSecretProvider : ISecretProvider
    {
        private readonly string _provider;
        private readonly string _value;

        public FakeSecretProvider(string provider, bool supportsSynchronousReads, string value)
        {
            _provider = provider;
            SupportsSynchronousReads = supportsSynchronousReads;
            _value = value;
        }

        public int AsyncCalls { get; private set; }
        public string EmptyValueSourceName => "Fake secret";
        public bool SupportsSynchronousReads { get; }

        public bool CanRead(string provider) =>
            provider.Equals(_provider, StringComparison.Ordinal);

        public Task<string> ReadAsync(SecretRef secret, string settingPath, CancellationToken ct)
        {
            AsyncCalls++;
            return Task.FromResult(_value);
        }

        public string Read(SecretRef secret, string settingPath) =>
            _value;
    }
}
