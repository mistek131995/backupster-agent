using System.Security.Cryptography;

namespace BackupsterAgent.Services.Common;

public static class ConfigBootstrapper
{
    public sealed record Result(bool TemplateCreated, string FilePath, Exception? Failure);

    public static Result EnsureTemplate(string configDir)
    {
        var filePath = Path.Combine(configDir, "appsettings.json");

        if (File.Exists(filePath))
            return new Result(false, filePath, null);

        try
        {
            var encryptionKey = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
            var template = BuildTemplate(encryptionKey);

            if (OperatingSystem.IsWindows())
            {
                Directory.CreateDirectory(configDir);
                File.WriteAllText(filePath, template);
            }
            else
            {
                const UnixFileMode ownerOnlyDirectory = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute;
                Directory.CreateDirectory(configDir, ownerOnlyDirectory);
                File.SetUnixFileMode(configDir, ownerOnlyDirectory);

                var fileOptions = new FileStreamOptions
                {
                    Mode = FileMode.CreateNew,
                    Access = FileAccess.Write,
                    UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite
                };
                using var stream = new FileStream(filePath, fileOptions);
                using var writer = new StreamWriter(stream);
                writer.Write(template);
            }

            return new Result(true, filePath, null);
        }
        catch (Exception ex)
        {
            return new Result(false, filePath, ex);
        }
    }

    private static string BuildTemplate(string encryptionKey) => $$"""
        {
          "Connections": [
            {
              "Name": "main",
              "DatabaseType": "Postgres",
              "Host": "",
              "Port": 5432,
              "Username": "",
              "Password": ""
            }
          ],
          "Storages": [
            {
              "Name": "main",
              "Provider": "S3",
              "S3": {
                "EndpointUrl": "",
                "AccessKey": "",
                "SecretKey": "",
                "BucketName": "",
                "Region": "us-east-1"
              }
            }
          ],
          "Databases": [
            {
              "ConnectionName": "main",
              "StorageName": "main",
              "Database": "",
              "OutputPath": "/backups",
              "FilePaths": []
            }
          ],
          "FileSets": [],
          "EncryptionSettings": {
            "Key": "{{encryptionKey}}"
          }
        }
        """;
}
