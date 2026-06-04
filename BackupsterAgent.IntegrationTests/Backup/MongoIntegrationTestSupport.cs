using BackupsterAgent.Configuration;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;

namespace BackupsterAgent.IntegrationTests.Backup;

internal static class MongoIntegrationTestSupport
{
    public const string TestDbPrefix = "bp_itest_mongo_";

    private static readonly JsonWriterSettings CanonicalJsonSettings = new()
    {
        OutputMode = JsonOutputMode.CanonicalExtendedJson,
    };

    public static async Task AssumeMongoToolsAvailableAsync(
        ConnectionConfig connection,
        MongoBinaryResolver resolver,
        ExternalProcessRunner runner,
        CancellationToken ct)
    {
        await AssumeToolAvailableAsync(connection, resolver, runner, "mongodump", ct);
        await AssumeToolAvailableAsync(connection, resolver, runner, "mongorestore", ct);
    }

    public static MongoClient CreateClient(ConnectionConfig connection) =>
        new(MongoConnectionFactory.BuildClientSettings(connection));

    public static async Task CreateSourceDatabaseAsync(
        ConnectionConfig connection,
        string dbName,
        CancellationToken ct)
    {
        var db = CreateClient(connection).GetDatabase(dbName);

        await db.GetCollection<BsonDocument>("items").InsertManyAsync(
            BuildItems(),
            cancellationToken: ct);

        await db.GetCollection<BsonDocument>("audit_events").InsertManyAsync(
            BuildAuditEvents(),
            cancellationToken: ct);
    }

    public static async Task<IReadOnlyDictionary<string, IReadOnlyList<string>>> ReadSnapshotAsync(
        ConnectionConfig connection,
        string dbName,
        CancellationToken ct)
    {
        var db = CreateClient(connection).GetDatabase(dbName);
        using var cursor = await db.ListCollectionNamesAsync(cancellationToken: ct);
        var collectionNames = (await cursor.ToListAsync(ct))
            .Where(name => !name.StartsWith("system.", StringComparison.Ordinal))
            .Order(StringComparer.Ordinal)
            .ToArray();

        var snapshot = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
        foreach (var collectionName in collectionNames)
        {
            var collection = db.GetCollection<BsonDocument>(collectionName);
            var docs = await collection
                .Find(FilterDefinition<BsonDocument>.Empty)
                .Sort(Builders<BsonDocument>.Sort.Ascending("_id"))
                .ToListAsync(ct);

            snapshot[collectionName] = docs
                .Select(doc => doc.ToJson(CanonicalJsonSettings))
                .ToArray();
        }

        return snapshot;
    }

    public static async Task DropDatabaseIfExistsAsync(
        ConnectionConfig connection,
        string dbName,
        CancellationToken ct)
    {
        await CreateClient(connection).DropDatabaseAsync(dbName, ct);
    }

    public static async Task DropLeftoverTestDatabasesAsync(
        ConnectionConfig connection,
        CancellationToken ct)
    {
        try
        {
            var client = CreateClient(connection);
            using var cursor = await client.ListDatabaseNamesAsync(cancellationToken: ct);
            var leftovers = (await cursor.ToListAsync(ct))
                .Where(name => name.StartsWith(TestDbPrefix, StringComparison.Ordinal))
                .ToArray();

            foreach (var name in leftovers)
            {
                try { await client.DropDatabaseAsync(name, ct); }
                catch (Exception ex)
                {
                    TestContext.Progress.WriteLine($"Leftover DB '{name}' cleanup failed: {ex.Message}");
                }
            }
        }
        catch (MongoCommandException ex)
        {
            TestContext.Progress.WriteLine($"Leftover MongoDB cleanup skipped: {ex.Message}");
        }
    }

    public static async Task DecompressGzAsync(string gzPath, string outputPath, CancellationToken ct)
    {
        await using var input = File.OpenRead(gzPath);
        await using var gz = new System.IO.Compression.GZipStream(
            input,
            System.IO.Compression.CompressionMode.Decompress);
        await using var output = File.Create(outputPath);
        await gz.CopyToAsync(output, ct);
    }

    private static async Task AssumeToolAvailableAsync(
        ConnectionConfig connection,
        MongoBinaryResolver resolver,
        ExternalProcessRunner runner,
        string toolName,
        CancellationToken ct)
    {
        var binary = resolver.Resolve(connection, toolName);
        try
        {
            var result = await runner.RunAsync(
                new ExternalProcessRequest
                {
                    FileName = binary,
                    Arguments = new[] { "--version" },
                },
                handleStdout: null,
                handleStdin: null,
                ct);

            Assume.That(
                result.ExitCode,
                Is.Zero,
                $"{toolName} --version returned {result.ExitCode}; set Mongo:BinPath or install MongoDB Database Tools.");
        }
        catch (Exception ex)
        {
            Assume.That(
                false,
                $"{toolName} is not available; set Mongo:BinPath or install MongoDB Database Tools. {ex.Message}");
        }
    }

    private static IReadOnlyList<BsonDocument> BuildItems() =>
    [
        new BsonDocument
        {
            ["_id"] = 1,
            ["name"] = "alpha",
            ["enabled"] = true,
            ["tags"] = new BsonArray { "hot", "daily" },
            ["meta"] = new BsonDocument
            {
                ["priority"] = 10,
                ["owner"] = "ops",
            },
        },
        new BsonDocument
        {
            ["_id"] = 2,
            ["name"] = "beta",
            ["enabled"] = false,
            ["tags"] = new BsonArray { "cold" },
            ["meta"] = new BsonDocument
            {
                ["priority"] = 20,
                ["owner"] = "dev",
            },
        },
        new BsonDocument
        {
            ["_id"] = 3,
            ["name"] = "gamma",
            ["enabled"] = true,
            ["tags"] = new BsonArray(),
            ["meta"] = new BsonDocument
            {
                ["priority"] = 30,
                ["owner"] = BsonNull.Value,
            },
        },
    ];

    private static IReadOnlyList<BsonDocument> BuildAuditEvents() =>
    [
        new BsonDocument
        {
            ["_id"] = 1,
            ["event"] = "created",
            ["itemId"] = 1,
            ["atUtc"] = new BsonDateTime(new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc)),
            ["traceId"] = ObjectId.Parse("64f7a1a00000000000000001"),
        },
        new BsonDocument
        {
            ["_id"] = 2,
            ["event"] = "updated",
            ["itemId"] = 2,
            ["atUtc"] = new BsonDateTime(new DateTime(2026, 1, 2, 4, 5, 6, DateTimeKind.Utc)),
            ["traceId"] = ObjectId.Parse("64f7a1a00000000000000002"),
        },
    ];
}
