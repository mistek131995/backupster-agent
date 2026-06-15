using BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

namespace BackupsterAgent.Tests.Providers;

public sealed class MysqlServerProbeTests
{
    [Test]
    public void ReadinessSql_UsesSqlLevelProbe()
    {
        Assert.That(MysqlServerProbe.ReadinessSql, Is.EqualTo("SELECT 1"));
    }

    [Test]
    public void ShutdownPrivilegeSql_RequiresShutdownPrivilegeOnly()
    {
        Assert.That(MysqlServerProbe.ShutdownPrivilegeSql, Does.Contain("privilege_type = 'SHUTDOWN'"));
        Assert.That(MysqlServerProbe.ShutdownPrivilegeSql, Does.Not.Contain("SUPER"));
    }
}
