using BackupsterAgent.Providers.Restore;

namespace BackupsterAgent.Tests.Providers;

public sealed class MysqlArgFilterTests
{
    private static ISet<string> StripDefaults() =>
        new HashSet<string>(MysqlPhysicalRestoreProvider.SensitiveMysqldKeys, StringComparer.OrdinalIgnoreCase)
        {
            "--datadir",
            "--port",
            "--user",
        };

    [Test]
    public void Strips_password_inline()
    {
        var input = new[] { "--datadir=/var/lib/mysql", "--password=secret", "--port=3306" };
        var result = MysqlPhysicalRestoreProvider.FilterOriginalArgs(input, StripDefaults());

        Assert.That(result, Is.Empty);
    }

    [Test]
    public void Strips_password_separated_value()
    {
        var input = new[] { "--password", "secret", "--max-connections=100" };
        var result = MysqlPhysicalRestoreProvider.FilterOriginalArgs(input, StripDefaults());

        Assert.That(result, Is.EqualTo(new[] { "--max-connections=100" }));
    }

    [Test]
    public void Strips_init_file_and_ssl_key_and_skip_grant_tables()
    {
        var input = new[]
        {
            "--init-file=/root/init.sql",
            "--ssl-key=/etc/mysql/server.key",
            "--skip-grant-tables",
            "--server-id=42",
        };
        var result = MysqlPhysicalRestoreProvider.FilterOriginalArgs(input, StripDefaults());

        Assert.That(result, Is.EqualTo(new[] { "--server-id=42" }));
    }

    [Test]
    public void Keeps_user_config_when_stripping_user()
    {
        var input = new[] { "--user=mysql", "--user-config=/etc/my.cnf.d/extra" };
        var result = MysqlPhysicalRestoreProvider.FilterOriginalArgs(input, StripDefaults());

        Assert.That(result, Is.EqualTo(new[] { "--user-config=/etc/my.cnf.d/extra" }));
    }

    [Test]
    public void Strips_with_underscore_form_when_dash_form_listed()
    {
        var input = new[] { "--skip_grant_tables", "--plugin_load_add=ldap.so", "--server-id=7" };
        var result = MysqlPhysicalRestoreProvider.FilterOriginalArgs(input, StripDefaults());

        Assert.That(result, Is.EqualTo(new[] { "--server-id=7" }));
    }

    [Test]
    public void Keeps_innodb_buffer_pool_size_and_other_tuning()
    {
        var input = new[]
        {
            "--innodb-buffer-pool-size=8G",
            "--max-connections=2000",
            "--bind-address=0.0.0.0",
            "--plugin-dir=/usr/lib/mysql/plugin",
        };
        var result = MysqlPhysicalRestoreProvider.FilterOriginalArgs(input, StripDefaults());

        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public void Drops_flag_form_without_consuming_next_dash_arg()
    {
        var input = new[] { "--skip-grant-tables", "--server-id=99" };
        var result = MysqlPhysicalRestoreProvider.FilterOriginalArgs(input, StripDefaults());

        Assert.That(result, Is.EqualTo(new[] { "--server-id=99" }));
    }

    [Test]
    public void Empty_input_returns_empty()
    {
        var result = MysqlPhysicalRestoreProvider.FilterOriginalArgs(Array.Empty<string>(), StripDefaults());

        Assert.That(result, Is.Empty);
    }

    [Test]
    public void Strip_set_can_use_underscore_key_form()
    {
        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "--max_connections" };
        var input = new[] { "--max-connections=1000", "--server-id=1" };
        var result = MysqlPhysicalRestoreProvider.FilterOriginalArgs(input, keys);

        Assert.That(result, Is.EqualTo(new[] { "--server-id=1" }));
    }
}
