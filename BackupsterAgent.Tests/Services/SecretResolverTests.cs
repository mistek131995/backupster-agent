using System.Net;
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
    public async Task ResolveStringAsync_GoogleSecretManagerReaderUsesFactoryNormalizedProvider()
    {
        string? observedName = null;
        string? observedLocation = null;
        var reader = new GoogleSecretManagerSecretReader(
            new FakeGoogleSecretManagerSecretBackend((name, location, _, _) =>
            {
                observedName = name;
                observedLocation = location;
                return Task.FromResult("{\"password\":\"from-google\\n\"}");
            }),
            NullLogger<GoogleSecretManagerSecretReader>.Instance);
        var resolver = new SecretResolver(new SecretProviderFactory([reader]));

        var value = await resolver.ResolveStringAsync(
            new SecretRef
            {
                Provider = " GOOGLE-SECRET-MANAGER ",
                ProjectId = "prod-project",
                Location = "europe-west1",
                Name = " db-password ",
                VersionId = "5",
                JsonKey = "password",
            },
            "plain-value",
            "Connections['pg'].Password",
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo("from-google"));
            Assert.That(
                observedName,
                Is.EqualTo("projects/prod-project/locations/europe-west1/secrets/db-password/versions/5"));
            Assert.That(observedLocation, Is.EqualTo("europe-west1"));
        });
    }

    [Test]
    public void RequiresAsyncResolution_GoogleSecretManagerReaderRequiresAsync()
    {
        var reader = new GoogleSecretManagerSecretReader(
            new FakeGoogleSecretManagerSecretBackend((_, _, _, _) => Task.FromResult("unused")),
            NullLogger<GoogleSecretManagerSecretReader>.Instance);
        var resolver = new SecretResolver(new SecretProviderFactory([reader]));

        Assert.That(
            resolver.RequiresAsyncResolution(new SecretRef { Provider = "google-secret-manager", Name = "secret" }),
            Is.True);
    }

    [Test]
    public void ResolveString_GoogleSecretManagerReaderRejectsSynchronousRead()
    {
        var reader = new GoogleSecretManagerSecretReader(
            new FakeGoogleSecretManagerSecretBackend((_, _, _, _) => Task.FromResult("unused")),
            NullLogger<GoogleSecretManagerSecretReader>.Instance);
        var resolver = new SecretResolver(new SecretProviderFactory([reader]));

        var ex = Assert.Throws<SecretResolutionException>(
            () => resolver.ResolveString(
                new SecretRef
                {
                    Provider = "google-secret-manager",
                    ProjectId = "prod-project",
                    Name = "db-password",
                },
                "plain-value",
                "Connections['pg'].Password"));

        Assert.That(ex!.Message, Is.Not.Empty);
    }

    [Test]
    public async Task GoogleSecretManagerReader_ReadsCurrentValueEachTime()
    {
        var calls = 0;
        var observedNames = new List<string>();
        var reader = new GoogleSecretManagerSecretReader(
            new FakeGoogleSecretManagerSecretBackend((name, _, _, _) =>
            {
                calls++;
                observedNames.Add(name);
                return Task.FromResult($"from-google-{calls}");
            }),
            NullLogger<GoogleSecretManagerSecretReader>.Instance);

        var secret = new SecretRef
        {
            Provider = "google-secret-manager",
            ProjectId = "prod-project",
            Name = "db-password",
        };

        var first = await reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None);
        var second = await reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo("from-google-1"));
            Assert.That(second, Is.EqualTo("from-google-2"));
            Assert.That(calls, Is.EqualTo(2));
            Assert.That(
                observedNames,
                Is.EqualTo(new[]
                {
                    "projects/prod-project/secrets/db-password/versions/latest",
                    "projects/prod-project/secrets/db-password/versions/latest",
                }));
        });
    }

    [Test]
    public async Task GoogleSecretManagerReader_FullVersionNameUsesConfiguredName()
    {
        string? observedName = null;
        string? observedLocation = null;
        var reader = new GoogleSecretManagerSecretReader(
            new FakeGoogleSecretManagerSecretBackend((name, location, _, _) =>
            {
                observedName = name;
                observedLocation = location;
                return Task.FromResult("from-google");
            }),
            NullLogger<GoogleSecretManagerSecretReader>.Instance);

        var value = await reader.ReadAsync(
            new SecretRef
            {
                Provider = "google-secret-manager",
                Name = "projects/prod-project/locations/europe-west1/secrets/db-password/versions/7",
            },
            "Connections['pg'].Password",
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo("from-google"));
            Assert.That(
                observedName,
                Is.EqualTo("projects/prod-project/locations/europe-west1/secrets/db-password/versions/7"));
            Assert.That(observedLocation, Is.EqualTo("europe-west1"));
        });
    }

    [Test]
    public void GoogleSecretManagerReader_MissingProjectIdThrows()
    {
        var reader = new GoogleSecretManagerSecretReader(
            new FakeGoogleSecretManagerSecretBackend((_, _, _, _) =>
                throw new InvalidOperationException("Backend should not be called")),
            NullLogger<GoogleSecretManagerSecretReader>.Instance);

        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => reader.ReadAsync(
                new SecretRef
                {
                    Provider = "google-secret-manager",
                    Name = "db-password",
                },
                "Connections['pg'].Password",
                CancellationToken.None));

        Assert.That(ex!.Message, Is.Not.Empty);
    }

    [Test]
    public void GoogleSecretManagerReader_MissingNameThrows()
    {
        var reader = new GoogleSecretManagerSecretReader(
            new FakeGoogleSecretManagerSecretBackend((_, _, _, _) =>
                throw new InvalidOperationException("Backend should not be called")),
            NullLogger<GoogleSecretManagerSecretReader>.Instance);

        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => reader.ReadAsync(
                new SecretRef
                {
                    Provider = "google-secret-manager",
                    ProjectId = "prod-project",
                },
                "Connections['pg'].Password",
                CancellationToken.None));

        Assert.That(ex!.Message, Is.Not.Empty);
    }

    [Test]
    public void GoogleSecretManagerReader_BackendFailureWrapsIntoSecretResolutionException()
    {
        var reader = new GoogleSecretManagerSecretReader(
            new FakeGoogleSecretManagerSecretBackend((_, _, _, _) =>
                throw new InvalidOperationException("Backend failed")),
            NullLogger<GoogleSecretManagerSecretReader>.Instance);

        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => reader.ReadAsync(
                new SecretRef
                {
                    Provider = "google-secret-manager",
                    ProjectId = "prod-project",
                    Name = "db-password",
                },
                "Connections['pg'].Password",
                CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Is.Not.Empty);
            Assert.That(ex.InnerException, Is.InstanceOf<InvalidOperationException>());
        });
    }

    [Test]
    public async Task ResolveStringAsync_HashicorpVaultReaderUsesFactoryNormalizedProvider()
    {
        var backend = new FakeHashicorpVaultSecretBackend
        {
            KvValue = "{\"password\":\"from-vault\\n\"}",
        };
        var reader = new HashicorpVaultSecretReader(
            [
                new VaultSecretProviderConfig
                {
                    Name = "prod-hcp",
                    Address = "https://vault.example.net",
                    Namespace = "admin",
                    Auth = new VaultAuthConfig
                    {
                        Method = "Token",
                        Token = "vault-token",
                    },
                },
            ],
            backend);
        var resolver = new SecretResolver(new SecretProviderFactory([reader]));

        var value = await resolver.ResolveStringAsync(
            new SecretRef
            {
                Provider = " HASHICORP-VAULT ",
                Name = "prod-hcp",
                MountPath = "kv",
                Path = "backupster/prod/main-pg",
                Namespace = "team-a",
                JsonKey = "password",
                VersionId = "2",
            },
            "plain-value",
            "Connections['pg'].Password",
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo("from-vault"));
            Assert.That(backend.ReadCalls, Is.EqualTo(1));
            Assert.That(backend.LastReadAddress, Is.EqualTo(new Uri("https://vault.example.net")));
            Assert.That(backend.LastReadNamespace, Is.EqualTo("team-a"));
            Assert.That(backend.LastReadToken, Is.EqualTo("vault-token"));
            Assert.That(backend.LastReadMountPath, Is.EqualTo("kv"));
            Assert.That(backend.LastReadSecretPath, Is.EqualTo("backupster/prod/main-pg"));
            Assert.That(backend.LastReadVersion, Is.EqualTo("2"));
        });
    }

    [Test]
    public void RequiresAsyncResolution_HashicorpVaultReaderRequiresAsync()
    {
        var reader = new HashicorpVaultSecretReader(
            [new VaultSecretProviderConfig { Name = "prod", Address = "https://vault.example.net" }],
            new FakeHashicorpVaultSecretBackend());
        var resolver = new SecretResolver(new SecretProviderFactory([reader]));

        Assert.That(
            resolver.RequiresAsyncResolution(new SecretRef { Provider = "hashicorp-vault", Name = "prod" }),
            Is.True);
    }

    [Test]
    public void ResolveString_HashicorpVaultReaderRejectsSynchronousRead()
    {
        var reader = new HashicorpVaultSecretReader(
            [new VaultSecretProviderConfig { Name = "prod", Address = "https://vault.example.net" }],
            new FakeHashicorpVaultSecretBackend());
        var resolver = new SecretResolver(new SecretProviderFactory([reader]));

        var ex = Assert.Throws<SecretResolutionException>(
            () => resolver.ResolveString(
                new SecretRef { Provider = "hashicorp-vault", Name = "prod", Path = "backupster/db" },
                "plain-value",
                "Connections['pg'].Password"));

        Assert.That(ex!.Message, Is.Not.Empty);
    }

    [Test]
    public async Task HashicorpVaultSecretReader_AppRoleAuthenticatesOnceAndCachesToken()
    {
        var backend = new FakeHashicorpVaultSecretBackend
        {
            KvValue = "{\"value\":\"from-vault\"}",
            LoginResult = new VaultAppRoleLoginResult("login-token", 3600, true),
        };
        var reader = new HashicorpVaultSecretReader(
            [
                new VaultSecretProviderConfig
                {
                    Name = "prod",
                    Address = "https://vault.example.net",
                    Namespace = "admin",
                    Auth = new VaultAuthConfig
                    {
                        Method = "AppRole",
                        MountPath = "auth/custom-approle",
                        RoleId = "role-id",
                        SecretId = "secret-id",
                    },
                },
            ],
            backend);
        var secret = new SecretRef
        {
            Provider = "hashicorp-vault",
            Name = "prod",
            MountPath = "secret",
            Path = "backupster/db",
        };

        var first = await reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None);
        var second = await reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo("from-vault"));
            Assert.That(second, Is.EqualTo("from-vault"));
            Assert.That(backend.LoginCalls, Is.EqualTo(1));
            Assert.That(backend.ReadCalls, Is.EqualTo(2));
            Assert.That(backend.LastLoginNamespace, Is.EqualTo("admin"));
            Assert.That(backend.LastLoginAuthMountPath, Is.EqualTo("auth/custom-approle"));
            Assert.That(backend.LastLoginRoleId, Is.EqualTo("role-id"));
            Assert.That(backend.LastLoginSecretId, Is.EqualTo("secret-id"));
            Assert.That(backend.ReadTokens, Is.EqualTo(new[] { "login-token", "login-token" }));
        });
    }

    [Test]
    public async Task HashicorpVaultSecretReader_RenewsTokenBeforeLeaseExpires()
    {
        var time = new ManualTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var backend = new FakeHashicorpVaultSecretBackend
        {
            LoginResult = new VaultAppRoleLoginResult("login-token", 100, true),
            RenewResult = new VaultAppRoleLoginResult("renewed-token", 200, true),
        };
        var reader = new HashicorpVaultSecretReader(
            [
                new VaultSecretProviderConfig
                {
                    Name = "prod",
                    Address = "https://vault.example.net",
                    Auth = new VaultAuthConfig
                    {
                        Method = "AppRole",
                        RoleId = "role-id",
                        SecretId = "secret-id",
                    },
                },
            ],
            backend,
            time);
        var secret = new SecretRef
        {
            Provider = "hashicorp-vault",
            Name = "prod",
            Path = "backupster/db",
        };

        await reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None);
        time.Advance(TimeSpan.FromSeconds(91));
        await reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(backend.LoginCalls, Is.EqualTo(1));
            Assert.That(backend.RenewCalls, Is.EqualTo(1));
            Assert.That(backend.LastRenewToken, Is.EqualTo("login-token"));
            Assert.That(backend.ReadTokens, Is.EqualTo(new[] { "login-token", "renewed-token" }));
        });
    }

    [Test]
    public async Task HashicorpVaultSecretReader_RenewsTokenWithoutAnotherSecretRead()
    {
        var backend = new FakeHashicorpVaultSecretBackend
        {
            LoginResult = new VaultAppRoleLoginResult("login-token", 2, true),
            RenewResult = new VaultAppRoleLoginResult("renewed-token", 3600, true),
        };
        await using var reader = new HashicorpVaultSecretReader(
            [
                new VaultSecretProviderConfig
                {
                    Name = "prod",
                    Address = "https://vault.example.net",
                    Auth = new VaultAuthConfig
                    {
                        Method = "AppRole",
                        RoleId = "role-id",
                        SecretId = "secret-id",
                    },
                },
            ],
            backend);

        await reader.ReadAsync(
            new SecretRef { Provider = "hashicorp-vault", Name = "prod", Path = "backupster/db" },
            "Connections['pg'].Password",
            CancellationToken.None);
        await backend.RenewalObserved.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(backend.LoginCalls, Is.EqualTo(1));
            Assert.That(backend.RenewCalls, Is.EqualTo(1));
            Assert.That(backend.ReadCalls, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task HashicorpVaultSecretReader_ReauthenticatesWhenRenewalIsRejected()
    {
        var time = new ManualTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var backend = new FakeHashicorpVaultSecretBackend
        {
            LoginResults =
            {
                new VaultAppRoleLoginResult("first-token", 100, true),
                new VaultAppRoleLoginResult("second-token", 100, true),
            },
            RenewException = new SecretResolutionException(
                "Renewal rejected",
                new VaultApiException(
                    "Forbidden",
                    HttpStatusCode.Forbidden,
                    ["invalid token", "permission denied"])),
        };
        var reader = new HashicorpVaultSecretReader(
            [
                new VaultSecretProviderConfig
                {
                    Name = "prod",
                    Address = "https://vault.example.net",
                    Auth = new VaultAuthConfig
                    {
                        Method = "AppRole",
                        RoleId = "role-id",
                        SecretId = "secret-id",
                    },
                },
            ],
            backend,
            time);
        var secret = new SecretRef
        {
            Provider = "hashicorp-vault",
            Name = "prod",
            Path = "backupster/db",
        };

        await reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None);
        time.Advance(TimeSpan.FromSeconds(91));
        await reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(backend.RenewCalls, Is.EqualTo(1));
            Assert.That(backend.LoginCalls, Is.EqualTo(2));
            Assert.That(backend.ReadTokens, Is.EqualTo(new[] { "first-token", "second-token" }));
        });
    }

    [Test]
    public async Task HashicorpVaultSecretReader_AppRoleWithoutSecretIdOmitsIt()
    {
        var backend = new FakeHashicorpVaultSecretBackend();
        var reader = new HashicorpVaultSecretReader(
            [
                new VaultSecretProviderConfig
                {
                    Name = "prod",
                    Address = "https://vault.example.net",
                    Auth = new VaultAuthConfig
                    {
                        Method = "AppRole",
                        RoleId = "role-id",
                    },
                },
            ],
            backend);

        await reader.ReadAsync(
            new SecretRef { Provider = "hashicorp-vault", Name = "prod", Path = "backupster/db" },
            "Connections['pg'].Password",
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(backend.LoginCalls, Is.EqualTo(1));
            Assert.That(backend.LastLoginSecretId, Is.Null);
        });
    }

    [Test]
    public async Task HashicorpVaultSecretReader_ResponseWrappingTokenIsUnwrappedBeforeLogin()
    {
        var backend = new FakeHashicorpVaultSecretBackend
        {
            UnwrappedSecretId = "unwrapped-secret-id",
        };
        var reader = new HashicorpVaultSecretReader(
            [
                new VaultSecretProviderConfig
                {
                    Name = "prod",
                    Address = "https://vault.example.net",
                    Namespace = "admin",
                    Auth = new VaultAuthConfig
                    {
                        Method = "AppRole",
                        RoleId = "role-id",
                        SecretId = "wrapping-token",
                        SecretIdMode = "ResponseWrappingToken",
                        SecretIdWrappingExpectedCreationPath = "auth/approle/role/backupster/secret-id",
                    },
                },
            ],
            backend);

        await reader.ReadAsync(
            new SecretRef { Provider = "hashicorp-vault", Name = "prod", Path = "backupster/db" },
            "Connections['pg'].Password",
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(backend.UnwrapCalls, Is.EqualTo(1));
            Assert.That(backend.LastWrappingToken, Is.EqualTo("wrapping-token"));
            Assert.That(backend.LastExpectedCreationPath, Is.EqualTo("auth/approle/role/backupster/secret-id"));
            Assert.That(backend.LastUnwrapNamespace, Is.EqualTo("admin"));
            Assert.That(backend.LastLoginSecretId, Is.EqualTo("unwrapped-secret-id"));
        });
    }

    [Test]
    public async Task HashicorpVaultSecretReader_AppRoleZeroLeaseDurationCachesToken()
    {
        var backend = new FakeHashicorpVaultSecretBackend
        {
            KvValue = "{\"value\":\"from-vault\"}",
            LoginResult = new VaultAppRoleLoginResult("non-expiring-token", 0, false),
        };
        var reader = new HashicorpVaultSecretReader(
            [
                new VaultSecretProviderConfig
                {
                    Name = "prod",
                    Address = "https://vault.example.net",
                    Auth = new VaultAuthConfig
                    {
                        Method = "AppRole",
                        RoleId = "role-id",
                        SecretId = "secret-id",
                    },
                },
            ],
            backend);
        var secret = new SecretRef
        {
            Provider = "hashicorp-vault",
            Name = "prod",
            MountPath = "secret",
            Path = "backupster/db",
        };

        var first = await reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None);
        var second = await reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo("from-vault"));
            Assert.That(second, Is.EqualTo("from-vault"));
            Assert.That(backend.LoginCalls, Is.EqualTo(1));
            Assert.That(backend.ReadCalls, Is.EqualTo(2));
            Assert.That(backend.ReadTokens, Is.EqualTo(new[] { "non-expiring-token", "non-expiring-token" }));
        });
    }

    [Test]
    public async Task HashicorpVaultSecretReader_InvalidTokenReadReauthenticatesAndRetriesOnce()
    {
        var backend = new FakeHashicorpVaultSecretBackend
        {
            LoginResults =
            {
                new VaultAppRoleLoginResult("stale-token", 3600, true),
                new VaultAppRoleLoginResult("fresh-token", 3600, true),
            },
            ReadResultFactory = (call, _) =>
            {
                if (call == 1)
                    throw new SecretResolutionException(
                        "Vault forbidden",
                        new VaultApiException(
                            "Forbidden",
                            HttpStatusCode.Forbidden,
                            ["invalid token", "permission denied"]));

                return "{\"value\":\"from-vault\"}";
            },
        };
        var reader = new HashicorpVaultSecretReader(
            [
                new VaultSecretProviderConfig
                {
                    Name = "prod",
                    Address = "https://vault.example.net",
                    Auth = new VaultAuthConfig
                    {
                        Method = "AppRole",
                        RoleId = "role-id",
                        SecretId = "secret-id",
                    },
                },
            ],
            backend);

        var value = await reader.ReadAsync(
            new SecretRef
            {
                Provider = "hashicorp-vault",
                Name = "prod",
                MountPath = "secret",
                Path = "backupster/db",
            },
            "Connections['pg'].Password",
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo("from-vault"));
            Assert.That(backend.LoginCalls, Is.EqualTo(2));
            Assert.That(backend.ReadCalls, Is.EqualTo(2));
            Assert.That(backend.ReadTokens, Is.EqualTo(new[] { "stale-token", "fresh-token" }));
        });
    }

    [Test]
    public async Task HashicorpVaultSecretReader_PermissionDeniedKeepsCachedToken()
    {
        var backend = new FakeHashicorpVaultSecretBackend
        {
            LoginResult = new VaultAppRoleLoginResult("login-token", 3600, true),
            ReadResultFactory = (call, _) =>
            {
                if (call == 1)
                    throw new SecretResolutionException(
                        "Vault forbidden",
                        new VaultApiException(
                            "Forbidden",
                            HttpStatusCode.Forbidden,
                            ["permission denied"]));

                return "{\"value\":\"from-vault\"}";
            },
        };
        var reader = new HashicorpVaultSecretReader(
            [
                new VaultSecretProviderConfig
                {
                    Name = "prod",
                    Address = "https://vault.example.net",
                    Auth = new VaultAuthConfig
                    {
                        Method = "AppRole",
                        RoleId = "role-id",
                        SecretId = "secret-id",
                    },
                },
            ],
            backend);
        var secret = new SecretRef
        {
            Provider = "hashicorp-vault",
            Name = "prod",
            Path = "backupster/db",
        };

        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => reader.ReadAsync(secret, "Connections['pg'].Password", CancellationToken.None));
        var value = await reader.ReadAsync(
            secret,
            "Connections['pg'].Password",
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Is.Not.Empty);
            Assert.That(value, Is.EqualTo("from-vault"));
            Assert.That(backend.LoginCalls, Is.EqualTo(1));
            Assert.That(backend.ReadCalls, Is.EqualTo(2));
            Assert.That(backend.ReadTokens, Is.EqualTo(new[] { "login-token", "login-token" }));
        });
    }

    [Test]
    public async Task HashicorpVaultSecretReader_TokenAuthCanUseEnvBootstrapSecret()
    {
        var tokenName = NewEnvName();
        Environment.SetEnvironmentVariable(tokenName, "env-vault-token");

        try
        {
            var backend = new FakeHashicorpVaultSecretBackend
            {
                KvValue = "{\"value\":\"from-vault\"}",
            };
            var reader = new HashicorpVaultSecretReader(
                [
                    new VaultSecretProviderConfig
                    {
                        Name = "prod",
                        Address = "https://vault.example.net",
                        Auth = new VaultAuthConfig
                        {
                            Method = "Token",
                            TokenSecret = new SecretRef { Provider = "env", Name = tokenName },
                        },
                    },
                ],
                backend);

            var value = await reader.ReadAsync(
                new SecretRef
                {
                    Provider = "hashicorp-vault",
                    Name = "prod",
                    Path = "backupster/db",
                },
                "Connections['pg'].Password",
                CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(value, Is.EqualTo("from-vault"));
                Assert.That(backend.LastReadToken, Is.EqualTo("env-vault-token"));
            });
        }
        finally
        {
            Environment.SetEnvironmentVariable(tokenName, null);
        }
    }

    [Test]
    public void HashicorpVaultSecretReader_MultipleFieldsWithoutJsonKeyThrows()
    {
        var backend = new FakeHashicorpVaultSecretBackend
        {
            KvValue = "{\"username\":\"backup\",\"password\":\"secret\"}",
        };
        var reader = new HashicorpVaultSecretReader(
            [
                new VaultSecretProviderConfig
                {
                    Name = "prod",
                    Address = "https://vault.example.net",
                    Auth = new VaultAuthConfig { Token = "vault-token" },
                },
            ],
            backend);

        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => reader.ReadAsync(
                new SecretRef
                {
                    Provider = "hashicorp-vault",
                    Name = "prod",
                    Path = "backupster/db",
                },
                "Connections['pg'].Password",
                CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("JsonKey"));
    }

    [Test]
    public void HashicorpVaultSecretReader_MissingProfileThrows()
    {
        var reader = new HashicorpVaultSecretReader(
            [new VaultSecretProviderConfig { Name = "prod", Address = "https://vault.example.net" }],
            new FakeHashicorpVaultSecretBackend());

        var ex = Assert.ThrowsAsync<SecretResolutionException>(
            () => reader.ReadAsync(
                new SecretRef
                {
                    Provider = "hashicorp-vault",
                    Name = "missing",
                    Path = "backupster/db",
                },
                "Connections['pg'].Password",
                CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("missing"));
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

    private sealed class FakeGoogleSecretManagerSecretBackend : IGoogleSecretManagerSecretBackend
    {
        private readonly Func<string, string?, string, CancellationToken, Task<string>> _secretVersionReader;

        public FakeGoogleSecretManagerSecretBackend(
            Func<string, string?, string, CancellationToken, Task<string>> secretVersionReader)
        {
            _secretVersionReader = secretVersionReader;
        }

        public Task<string> ReadSecretVersionAsync(
            string secretVersionName,
            string? location,
            string settingPath,
            CancellationToken ct) =>
            _secretVersionReader(secretVersionName, location, settingPath, ct);
    }

    private sealed class FakeHashicorpVaultSecretBackend : IHashicorpVaultSecretBackend
    {
        public string KvValue { get; init; } = "{\"value\":\"from-vault\"}";
        public VaultAppRoleLoginResult LoginResult { get; init; } = new("login-token", 3600, true);
        public VaultAppRoleLoginResult RenewResult { get; init; } = new("renewed-token", 3600, true);
        public SecretResolutionException? RenewException { get; init; }
        public List<VaultAppRoleLoginResult> LoginResults { get; } = [];
        public string UnwrappedSecretId { get; init; } = "unwrapped-secret-id";
        public Func<int, string, string>? ReadResultFactory { get; init; }
        public int ReadCalls { get; private set; }
        public int LoginCalls { get; private set; }
        public int RenewCalls { get; private set; }
        public int UnwrapCalls { get; private set; }
        public Uri? LastReadAddress { get; private set; }
        public string? LastReadNamespace { get; private set; }
        public string? LastReadToken { get; private set; }
        public string? LastReadMountPath { get; private set; }
        public string? LastReadSecretPath { get; private set; }
        public string? LastReadVersion { get; private set; }
        public string? LastLoginNamespace { get; private set; }
        public string? LastLoginAuthMountPath { get; private set; }
        public string? LastLoginRoleId { get; private set; }
        public string? LastLoginSecretId { get; private set; }
        public string? LastRenewToken { get; private set; }
        public string? LastWrappingToken { get; private set; }
        public string? LastExpectedCreationPath { get; private set; }
        public string? LastUnwrapNamespace { get; private set; }
        public List<string> ReadTokens { get; } = [];
        public TaskCompletionSource<bool> RenewalObserved { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<string> ReadKvV2Async(
            Uri address,
            string? vaultNamespace,
            string token,
            string mountPath,
            string secretPath,
            string? version,
            string settingPath,
            CancellationToken ct)
        {
            ReadCalls++;
            LastReadAddress = address;
            LastReadNamespace = vaultNamespace;
            LastReadToken = token;
            LastReadMountPath = mountPath;
            LastReadSecretPath = secretPath;
            LastReadVersion = version;
            ReadTokens.Add(token);
            if (ReadResultFactory is not null)
                return Task.FromResult(ReadResultFactory(ReadCalls, token));

            return Task.FromResult(KvValue);
        }

        public Task<VaultAppRoleLoginResult> LoginAppRoleAsync(
            Uri address,
            string? vaultNamespace,
            string authMountPath,
            string roleId,
            string? secretId,
            string settingPath,
            CancellationToken ct)
        {
            LoginCalls++;
            LastLoginNamespace = vaultNamespace;
            LastLoginAuthMountPath = authMountPath;
            LastLoginRoleId = roleId;
            LastLoginSecretId = secretId;
            var result = LoginResults.Count >= LoginCalls
                ? LoginResults[LoginCalls - 1]
                : LoginResult;
            return Task.FromResult(result);
        }

        public Task<VaultAppRoleLoginResult> RenewTokenAsync(
            Uri address,
            string? vaultNamespace,
            string token,
            string settingPath,
            CancellationToken ct)
        {
            RenewCalls++;
            LastRenewToken = token;
            RenewalObserved.TrySetResult(true);
            if (RenewException is not null)
                throw RenewException;

            return Task.FromResult(RenewResult);
        }

        public Task<string> UnwrapAppRoleSecretIdAsync(
            Uri address,
            string? vaultNamespace,
            string wrappingToken,
            string expectedCreationPath,
            string settingPath,
            CancellationToken ct)
        {
            UnwrapCalls++;
            LastWrappingToken = wrappingToken;
            LastExpectedCreationPath = expectedCreationPath;
            LastUnwrapNamespace = vaultNamespace;
            return Task.FromResult(UnwrappedSecretId);
        }
    }

    private sealed class ManualTimeProvider : TimeProvider
    {
        private DateTimeOffset _utcNow;

        public ManualTimeProvider(DateTimeOffset utcNow)
        {
            _utcNow = utcNow;
        }

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void Advance(TimeSpan value)
        {
            _utcNow += value;
        }
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
