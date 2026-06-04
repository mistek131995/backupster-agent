using BackupsterAgent.Configuration;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Common.Resolvers;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class DatabaseNameValidatorTests
{
    [TestCase("mydb")]
    [TestCase("my_db")]
    [TestCase("my-db")]
    [TestCase("my.db")]
    [TestCase("MyDb123")]
    [TestCase("a")]
    [TestCase("Users_2026-04-19")]
    public void IsValid_AcceptsRegularNames(string name)
    {
        Assert.That(DatabaseNameValidator.IsValid(name, out var reason), Is.True);
        Assert.That(reason, Is.Null);
    }

    [TestCase(null)]
    [TestCase("")]
    public void IsValid_RejectsNullOrEmpty(string? name)
    {
        Assert.That(DatabaseNameValidator.IsValid(name, out var reason), Is.False);
        Assert.That(reason, Does.Contain("пустое"));
    }

    [Test]
    public void IsValid_RejectsTooLong()
    {
        var name = new string('a', DatabaseNameValidator.MaxLength + 1);
        Assert.That(DatabaseNameValidator.IsValid(name, out var reason), Is.False);
        Assert.That(reason, Does.Contain("длина"));
    }

    [TestCase("foo..bar")]
    [TestCase("..evil")]
    [TestCase("trail..")]
    public void IsValid_RejectsDoubleDot(string name)
    {
        Assert.That(DatabaseNameValidator.IsValid(name, out var reason), Is.False);
        Assert.That(reason, Does.Contain("две точки"));
    }

    [TestCase("foo bar")]
    [TestCase("foo\nbar")]
    [TestCase("foo\tbar")]
    [TestCase("foo'; DROP")]
    [TestCase("foo\"bar")]
    [TestCase("foo/bar")]
    [TestCase("foo\\bar")]
    [TestCase("foo\0bar")]
    [TestCase("привет")]
    [TestCase("foo;bar")]
    public void IsValid_RejectsDisallowedChars(string name)
    {
        Assert.That(DatabaseNameValidator.IsValid(name, out var reason), Is.False);
        Assert.That(reason, Does.Contain("недопустимый символ"));
    }

    [Test]
    public void ToSafePathSegment_SafeName_ReturnsName()
    {
        Assert.That(DatabaseNameValidator.ToSafePathSegment("customers_2026"), Is.EqualTo("customers_2026"));
    }

    [Test]
    public void ToSafePathSegment_UnsafeName_ReturnsBase64UrlSegment()
    {
        var segment = DatabaseNameValidator.ToSafePathSegment("../prod");

        Assert.That(segment, Is.EqualTo("db-Li4vcHJvZA"));
        Assert.That(segment, Does.Not.Contain("/"));
        Assert.That(segment, Does.Not.Contain("\\"));
        Assert.That(segment, Does.Not.Contain(".."));
    }

    [Test]
    public void ToSafePathSegment_LongUnsafeName_ReturnsHashSegment()
    {
        var segment = DatabaseNameValidator.ToSafePathSegment(new string('Ж', 200));

        Assert.That(segment, Does.StartWith("db-"));
        Assert.That(segment.Length, Is.EqualTo(67));
    }

    [Test]
    public void ToSafePathSegment_EmptyName_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => DatabaseNameValidator.ToSafePathSegment(string.Empty));
    }

    [Test]
    public void DatabasePathSegment_SafeDatabaseName_ReturnsDatabaseName()
    {
        var config = new DatabaseConfig { Database = "customers" };

        Assert.That(config.DatabasePathSegment, Is.EqualTo("customers"));
    }

    [Test]
    public void DatabasePathSegment_UnsafeDatabaseName_ReturnsGeneratedSegment()
    {
        var config = new DatabaseConfig { Database = "../prod" };

        Assert.That(config.DatabasePathSegment, Is.EqualTo("db-Li4vcHJvZA"));
    }
}
