# Конфигурация

Все настройки в `appsettings.json`. Любой параметр можно переопределить переменной окружения.

- [Подключения, хранилища и базы данных](#подключения-хранилища-и-базы-данных)
- [Секреты из внешних источников](#секреты-из-внешних-источников)
- [Наборы файлов (FileSets)](#наборы-файлов-filesets)
- [Шифрование](#шифрование)
- [Хранилища — настройки провайдеров](#хранилища--настройки-провайдеров)
- [Подключение к Dashboard](#подключение-к-dashboard)
- [Путь к конфигу](#путь-к-конфигу)
- [Структура файлов в хранилище](#структура-файлов-в-хранилище)

> Настройки сборщика мусора (`GcSettings`) и очистки устаревших бэкапов (`RetentionSettings`) описаны в отдельном файле — [gc-and-retention.md](gc-and-retention.md).
> Настройки автономной очереди бэкапов (`OutboxSettings`) — в разделе [Автономный режим](#автономный-режим) ниже.

---

## Подключения, хранилища и базы данных

Конфиг разбит на три списка:

- `Connections[]` — реквизиты серверов БД (хост, логин, пароль, тип) или строка подключения там, где она поддерживается.
- `Storages[]` — хранилища для бэкапов (S3, SFTP, Azure Blob, WebDAV или локальный путь); у каждого уникальное имя и собственный набор настроек.
- `Databases[]` — список баз; каждая ссылается на подключение по имени. `StorageName` остаётся как legacy-fallback для старых расписаний без `storageNames`.

Такое разделение позволяет не дублировать реквизиты сервера для нескольких БД и класть разные БД в разные бакеты/хранилища.

```json
"Connections": [
  {
    "Name": "main-pg",
    "DatabaseType": "Postgres",
    "Host": "localhost",
    "Port": 5432,
    "Username": "user",
    "Password": "secret"
  },
  {
    "Name": "app-mysql",
    "DatabaseType": "Mysql",
    "Host": "localhost",
    "Port": 3306,
    "Username": "backup",
    "Password": "secret"
  },
  {
    "Name": "reporting-mssql",
    "DatabaseType": "Mssql",
    "Host": "localhost",
    "Port": 1433,
    "Username": "sa",
    "Password": "secret"
  },
  {
    "Name": "reporting-mssql-uri",
    "DatabaseType": "Mssql",
    "ConnectionUri": "Server=tcp:sql.example.net,1433;Integrated Security=true;Encrypt=True;TrustServerCertificate=False"
  },
  {
    "Name": "local-mongo",
    "DatabaseType": "MongoDb",
    "Host": "mongo.internal",
    "Port": 27017,
    "Username": "backup",
    "Password": "secret",
    "BinPath": "/usr/bin"
  },
  {
    "Name": "atlas-mongo",
    "DatabaseType": "MongoDb",
    "ConnectionUri": "mongodb+srv://backup:<password>@cluster.example.net/?tls=true",
    "BinPath": "/usr/bin"
  }
],
"Storages": [
  {
    "Name": "prod-s3",
    "Provider": "S3",
    "S3": {
      "EndpointUrl": "https://storage.yandexcloud.net",
      "AccessKey": "...",
      "SecretKey": "...",
      "BucketName": "prod-backups",
      "Region": "us-east-1"
    }
  },
  {
    "Name": "archive-sftp",
    "Provider": "Sftp",
    "Sftp": {
      "Host": "backup.example.com",
      "Port": 22,
      "Username": "backupuser",
      "PrivateKeyPath": "/root/.ssh/id_rsa",
      "RemotePath": "/var/backups"
    }
  }
],
"Databases": [
  {
    "ConnectionName": "main-pg",
    "StorageName": "prod-s3",
    "Database": "mydb",
    "OutputPath": "/tmp/backups",
    "FilePaths": []
  },
  {
    "ConnectionName": "reporting-mssql",
    "StorageName": "archive-sftp",
    "Database": "mydb2",
    "OutputPath": "/tmp/backups",
    "FilePaths": ["/etc/myapp/config.yml", "/var/data/certs"]
  }
]
```

- `Name` подключения и `Name` хранилища должны быть уникальны в пределах своих списков.
- `ConnectionName` у БД обязан ссылаться на существующую запись — иначе эта БД будет пропущена с ошибкой в логе, остальные продолжат работать.
- `StorageName` у БД опционален и используется только как legacy-fallback, когда расписание не прислало `storageNames`. Пустое или невалидное значение само по себе БД не блокирует: расписания с явным `storageNames` всё равно будут запускаться, а fallback без валидного storage будет пропущен с warning.
- `OutputPath` — папка для временных файлов дампа. Для MSSQL physical этот же путь передаётся SQL Server в `BACKUP DATABASE ... TO DISK` / `RESTORE DATABASE ... FROM DISK`, поэтому агент и SQL Server должны видеть каталог одинаково. Файлы удаляются после загрузки или restore.
- `FilePaths` — список путей к файлам или директориям для файлового бэкапа. Директории обходятся рекурсивно. Файлы режутся на content-defined chunks (FastCDC, ~4 МиБ) и дедуплицируются внутри одного хранилища. Работает на всех провайдерах: S3, SFTP, Azure Blob, WebDAV, LocalFs. На SFTP операции идут через persistent SSH-сессию серийно; на WebDAV каждый чанк требует отдельный HTTPS round-trip. Поле необязательное.
- Для MSSQL используйте **либо** `ConnectionUri` — полную SQL Server connection string, **либо** `Host` + `Port` + `Username` + `Password`. Смешанный вариант не синхронизируется на дашборд и не используется для backup/restore. Агент программно меняет в строке только `Initial Catalog`/`Database`, чтобы открыть нужную БД или `master` для `BACKUP`/`RESTORE`.
- Для topology sync MSSQL с `ConnectionUri` агент извлекает только безопасные `host`/`port` из `Data Source` (`server`, `server,1433`, `tcp:server,1433`). Если `Data Source` нельзя надёжно представить как host/port (например, named instance, LocalDB, named pipes, административное подключение `admin:`), sync подключения пропускается с warning; backup/restore по `ConnectionUri` не блокируется. Локальные синонимы `.` и `(local)` отображаются как `localhost`.
- Для MongoDB используйте **либо** `ConnectionUri`, **либо** `Host` + `Port` + `Username` + `Password`. Смешанный вариант не синхронизируется на дашборд и не используется для backup/restore.
- `BinPath` — **для PostgreSQL, MySQL и MongoDB**, необязательное. Каталог с клиентскими бинарниками: `pg_dump`/`pg_basebackup`/`psql`/`pg_ctl` для PG, `mysqldump`/`mysql`/`xtrabackup`/`xbstream`/`mysqld` для MySQL, `mongodump`/`mongorestore` для MongoDB. Override авто-резолва. По умолчанию агент сам ищет клиент: для PG — под мажорную версию сервера (реестр Windows + стандартные каталоги установки → `PATH`); для MySQL — `C:\Program Files\MySQL\MySQL Server *\bin` (высшая версия) / `/usr/local/mysql/bin` → `PATH`; для MongoDB — стандартные каталоги MongoDB Database Tools / MongoDB Server → `PATH`. Задавайте поле только при нестандартной установке, когда авто-резолв не находит нужный каталог, либо когда `PATH` службы не содержит нужный bin-каталог. Для MSSQL поле не используется. Подробнее — [postgres.md](postgres.md), [mysql.md](mysql.md).

---

## Секреты из внешних источников

Любое поле секрета можно задать plain-строкой или заменить соседним `*Secret`-полем. Если задано `*Secret`, оно имеет приоритет над plain-значением. Конфиги без `*Secret` используют plain-значение.

Формат для файла:

```json
"PasswordSecret": {
  "Provider": "file",
  "Path": "/run/secrets/db_password"
}
```

Формат для переменной окружения:

```json
"PasswordSecret": {
  "Provider": "env",
  "Name": "BACKUPSTER_PG_PASSWORD"
}
```

Формат для AWS Secrets Manager:

```json
"PasswordSecret": {
  "Provider": "aws-secrets-manager",
  "Name": "prod/db/main-pg",
  "Region": "eu-central-1",
  "JsonKey": "password",
  "VersionStage": "AWSCURRENT"
}
```

Формат для AWS SSM Parameter Store:

```json
"PasswordSecret": {
  "Provider": "aws-ssm-parameter",
  "Name": "/backupster/prod/db/main-pg/password",
  "Region": "eu-central-1",
  "WithDecryption": true
}
```

`Provider` обязателен и поддерживает `file`, `env`, `aws-secrets-manager` или `aws-ssm-parameter`. Для `file` поле `Path` — путь к файлу на хосте агента или внутри контейнера. Файл читается как UTF-8, завершающий перевод строки (`CR/LF`) срезается, пустой файл считается ошибкой конфигурации. Для `env` поле `Name` — имя переменной окружения, доступной процессу агента; завершающий перевод строки также срезается, отсутствующая или пустая переменная считается ошибкой конфигурации.

Для `aws-secrets-manager` поле `Name` — имя или ARN секрета. Если секрет хранится JSON-объектом, `JsonKey` выбирает top-level строковое поле; без `JsonKey` используется весь `SecretString`. `VersionStage` и `VersionId` опциональны; если оба не заданы, AWS возвращает текущую версию (`AWSCURRENT`). Для `aws-ssm-parameter` поле `Name` — имя или ARN параметра; label/version указываются стандартным AWS-синтаксисом в `Name` (`name:label` или `name:version`). `WithDecryption` по умолчанию `true`.

Для AWS-провайдеров `Region` можно задать в `*Secret`; если он не задан, используется стандартная конфигурация AWS SDK для процесса агента. `ServiceUrl` опционален и нужен только для нестандартных endpoint'ов. AWS credentials в `appsettings.json` не задаются: агент использует стандартную цепочку AWS SDK (переменные окружения, web identity, `AWS_PROFILE`/shared config, container credentials, EC2 instance metadata). Минимальные IAM-права: `secretsmanager:GetSecretValue` для Secrets Manager, `ssm:GetParameter` для Parameter Store и `kms:Decrypt`, если секрет/параметр зашифрован customer-managed KMS key.

Поддерживаемые поля:

| Область | Plain-поле | Secret-поле |
|---|---|---|
| Dashboard | `AgentSettings.Token` | `AgentSettings.TokenSecret` |
| Шифрование | `EncryptionSettings.Key` | `EncryptionSettings.KeySecret` |
| Подключения | `Connections[].ConnectionUri` | `Connections[].ConnectionUriSecret` |
| Подключения | `Connections[].Username` | `Connections[].UsernameSecret` |
| Подключения | `Connections[].Password` | `Connections[].PasswordSecret` |
| S3 | `Storages[].S3.AccessKey` | `Storages[].S3.AccessKeySecret` |
| S3 | `Storages[].S3.SecretKey` | `Storages[].S3.SecretKeySecret` |
| SFTP | `Storages[].Sftp.Username` | `Storages[].Sftp.UsernameSecret` |
| SFTP | `Storages[].Sftp.Password` | `Storages[].Sftp.PasswordSecret` |
| SFTP | `Storages[].Sftp.PrivateKeyPassphrase` | `Storages[].Sftp.PrivateKeyPassphraseSecret` |
| Azure Blob | `Storages[].AzureBlob.ConnectionString` | `Storages[].AzureBlob.ConnectionStringSecret` |
| Azure Blob | `Storages[].AzureBlob.AccountKey` | `Storages[].AzureBlob.AccountKeySecret` |
| WebDAV | `Storages[].WebDav.Username` | `Storages[].WebDav.UsernameSecret` |
| WebDAV | `Storages[].WebDav.Password` | `Storages[].WebDav.PasswordSecret` |

Агент заново читает внешний источник при каждом использовании соответствующего `*Secret`-поля. Для подключений к БД это следующий backup/restore/topology-sync; для токена дашборда — следующий HTTP-вызов; для хранилищ — следующий backup/restore/delete/GC, при изменении разрешённых credentials клиент хранилища пересоздаётся. Содержимое файлов секретов и AWS Secrets Manager/SSM можно ротировать без перезапуска агента. `env`-provider также читает переменную окружения процесса при каждом обращении, но изменения переменных окружения службы/контейнера обычно попадают в процесс только после перезапуска. Изменение plain-значений в `appsettings.json` или самой `*Secret`-ссылки требует перезапуска агента.

`EncryptionSettings.KeySecret` — исключение: мастер-ключ шифрования читается один раз и не поддерживает горячую ротацию. Ключ должен оставаться тем же для всех backup/restore, иначе старые бэкапы нельзя будет расшифровать.

Пример подключения и S3-хранилища:

```json
{
  "Name": "main-pg",
  "DatabaseType": "Postgres",
  "Host": "localhost",
  "Port": 5432,
  "UsernameSecret": {
    "Provider": "file",
    "Path": "/run/secrets/pg_user"
  },
  "PasswordSecret": {
    "Provider": "file",
    "Path": "/run/secrets/pg_password"
  }
}
```

```json
{
  "Name": "prod-s3",
  "Provider": "S3",
  "S3": {
    "EndpointUrl": "https://storage.yandexcloud.net",
    "AccessKeySecret": {
      "Provider": "file",
      "Path": "/run/secrets/s3_access_key"
    },
    "SecretKeySecret": {
      "Provider": "file",
      "Path": "/run/secrets/s3_secret_key"
    },
    "BucketName": "prod-backups",
    "Region": "us-east-1"
  }
}
```

Файловый provider подходит для Docker secrets, Kubernetes Secrets, External Secrets, Vault Agent templates, systemd `LoadCredential=` и CI/CD, если они записывают секрет в файл с правами, доступными процессу агента. Env-provider подходит для self-hosted, CI/CD и контейнеров, где секрет доставляется процессу как переменная окружения. AWS-провайдеры подходят для EC2/ECS/EKS и других окружений, где агент может получить IAM-роль или стандартные AWS credentials. Значения секретов, имена env-переменных, имена/ARN AWS-секретов и сами `*Secret`-ссылки на дашборд не отправляются. Для topology sync MongoDB/MSSQL агент может прочитать `ConnectionUriSecret` локально только чтобы извлечь безопасные `host`/`port`.

---

## Наборы файлов (FileSets)

`FileSets[]` — отдельный список для бэкапа произвольных каталогов и файлов без привязки к базе данных. Подходит для загруженных пользователями файлов приложения, конфигов, сертификатов и т. п.

```json
"FileSets": [
  {
    "Name": "app-uploads",
    "StorageName": "prod-s3",
    "Paths": [
      "/var/www/uploads",
      "/etc/myapp/certs"
    ]
  }
]
```

- `Name` — уникальное имя набора. Используется как идентификатор в дашборде.
- `StorageName` — ссылается на запись в `Storages[]` по имени. Может указывать на любой поддерживаемый провайдер: S3, SFTP, Azure Blob, WebDAV или LocalFs.
- `Paths` — список путей к файлам или директориям. Директории обходятся рекурсивно.

Пайплайн: **Open → Capture → Finalize** — тот же content-defined chunking (FastCDC, ~4 МиБ) и дедупликация по sha256, что и у файлового этапа БД-бэкапа. Дамп базы не создаётся.

Расписание — индивидуальное per-file-set, задаётся через дашборд.

**Работает на всех провайдерах:** S3, SFTP, Azure Blob, WebDAV, LocalFs. На SFTP операции идут серийно через persistent SSH-сессию; на WebDAV каждый чанк требует отдельный HTTPS round-trip.

> Набор файлов регистрируется в дашборде автоматически при первом открытии записи бэкапа — отдельной настройки через UI не требуется.

---

## Шифрование

```json
"EncryptionSettings": {
  "Key": "<base64 от 32 байт>"
}
```

Ключ можно хранить в файле, переменной окружения, AWS Secrets Manager или AWS SSM Parameter Store через `KeySecret`:

```json
"EncryptionSettings": {
  "KeySecret": {
    "Provider": "file",
    "Path": "/run/secrets/backupster_encryption_key"
  }
}
```

```json
"EncryptionSettings": {
  "KeySecret": {
    "Provider": "env",
    "Name": "BACKUPSTER_ENCRYPTION_KEY"
  }
}
```

При создании шаблона `appsettings.json` агент генерирует ключ автоматически. **Сохраните его в надёжном месте** — без ключа бэкапы восстановить невозможно, а его смена ломает дешифровку уже сделанных бэкапов.

Если plain-поле `Key` пустое и `KeySecret` не задан, агент стартует, но не запускает бэкапы до настройки ключа. Если `KeySecret` задан через `file` или `env`, внешний источник должен быть доступен уже при старте агента: недоступный файл, отсутствующая env-переменная, пустое значение, не-base64 или ключ не на 32 байта считаются ошибкой конфигурации и валят старт с понятным сообщением. Если `KeySecret` задан через AWS, секрет читается при первом использовании ключа; недоступный AWS-секрет, пустое значение, не-base64 или ключ не на 32 байта блокируют backup/restore до исправления конфигурации и перезапуска агента.

Если конфиг создан вручную и ключ пустой, сгенерируйте его сами:

```bash
# Linux / macOS
openssl rand -base64 32

# PowerShell
[Convert]::ToBase64String((1..32 | ForEach-Object { Get-Random -Maximum 256 }))
```

---

## Хранилища — настройки провайдеров

Каждая запись в `Storages[]` имеет поле `Provider` (`S3`, `Sftp`, `AzureBlob`, `WebDav` или `LocalFs`) и соответствующий вложенный блок с тем же именем. Лишние блоки для выбранного провайдера игнорируются.

**S3-хранилище:**

```json
{
  "Name": "prod-s3",
  "Provider": "S3",
  "S3": {
    "EndpointUrl": "https://storage.yandexcloud.net",
    "AccessKey": "...",
    "SecretKey": "...",
    "BucketName": "my-bucket",
    "Region": "us-east-1"
  }
}
```

> Для MinIO и Yandex Object Storage включён `ForcePathStyle` — ничего дополнительно настраивать не нужно.

> **`Region`** — для AWS S3 ставьте реальный регион бакета (например, `eu-central-1`). Для S3-совместимых хранилищ (MinIO, Yandex Object Storage) значение используется только для подписи запросов и игнорируется сервером — принято указывать `us-east-1`. Для Cloudflare R2 официальное значение — `auto`.

**SFTP-хранилище:**

```json
{
  "Name": "archive-sftp",
  "Provider": "Sftp",
  "Sftp": {
    "Host": "backup.example.com",
    "Port": 22,
    "Username": "backupuser",
    "Password": "",
    "PrivateKeyPath": "/root/.ssh/id_rsa",
    "PrivateKeyPassphrase": "",
    "RemotePath": "/var/backups",
    "HostKeyFingerprint": "SHA256:abcDEF123..."
  }
}
```

Поддерживается аутентификация по паролю и по приватному ключу. Удалённые директории создаются автоматически.

> **`HostKeyFingerprint`** — опциональный отпечаток публичного ключа SFTP-сервера в формате `SHA256:<base64 без padding>` (совпадает с тем, что печатает `ssh-keyscan -t rsa host | ssh-keygen -lf -`).
> - Задан → несовпадающий ключ сервера отвергается, агент пишет error «possible MITM» и соединение обрывается.
> - Не задан → агент один раз за время жизни процесса пишет warning с актуальным отпечатком, чтобы вы могли скопировать его в конфиг. Без отпечатка защиты от MITM нет — для prod обязательно задайте.
>
> Как получить отпечаток:
> ```bash
> ssh-keyscan -t rsa backup.example.com 2>/dev/null | ssh-keygen -lf -
> # 256 SHA256:abcDEF123...   backup.example.com (RSA)
> ```

Файловый бэкап (`FilePaths`/`FileSets`), дедупликация чанков и chunk GC поддерживаются. Операции идут через одну persistent SSH-сессию серийно под семафором, поэтому на больших файловых наборах SFTP заметно медленнее S3/Azure Blob/LocalFs.

**Azure Blob-хранилище:**

```json
{
  "Name": "prod-azure",
  "Provider": "AzureBlob",
  "AzureBlob": {
    "ConnectionString": "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net",
    "ContainerName": "prod-backups"
  }
}
```

Альтернативно — без `ConnectionString`, тройкой `AccountName + AccountKey + ServiceUri`:

```json
{
  "Name": "prod-azure",
  "Provider": "AzureBlob",
  "AzureBlob": {
    "AccountName": "mystorage",
    "AccountKey": "<base64 от Azure>",
    "ServiceUri": "https://mystorage.blob.core.windows.net",
    "ContainerName": "prod-backups"
  }
}
```

- `ContainerName` обязателен. Контейнер создавать заранее не нужно — агент создаст автоматически при первой загрузке.
- Задайте либо `ConnectionString`, либо все три поля `AccountName` + `AccountKey` + `ServiceUri`. Если переданы оба варианта — приоритет у `ConnectionString`.
- `ServiceUri` обычно вида `https://<account>.blob.core.windows.net`. Для Azure Government / Stack Hub — соответствующий региональный домен.
- Поддерживается весь спектр: дамп, файловый бэкап с дедупликацией, file-set'ы, chunk GC, retention sweep.

**WebDAV-хранилище:**

```json
{
  "Name": "yandex-disk",
  "Provider": "WebDav",
  "WebDav": {
    "BaseUrl": "https://webdav.yandex.ru",
    "Username": "you@yandex.ru",
    "Password": "<пароль приложения>",
    "RemotePath": "/backups"
  }
}
```

- `BaseUrl` обязателен и должен начинаться с `http://` или `https://`. Голый `http://` — credentials и тело бэкапа уйдут в открытом виде, агент один раз пишет warning; для prod используйте `https`.
- `Username` / `Password` — basic-auth. Для Яндекс.Диска **обязательно** [пароль приложения](https://id.yandex.ru/security/app-passwords), не основной пароль аккаунта (двухфакторка ломает обычную авторизацию по WebDAV).
- `RemotePath` — базовый каталог под аккаунтом. Дефолт `/`. Промежуточные каталоги (`MKCOL`) создаются автоматически при первой загрузке.
- Покрывает Яндекс.Диск, Облако МТС и любые WebDAV-совместимые серверы (Nextcloud / ownCloud / Apache mod_dav).
- Файловый бэкап (`FilePaths`/`FileSets`), дедупликация чанков и chunk GC поддерживаются. Каждая операция чанка — отдельный HTTPS round-trip; листинг идёт через `PROPFIND Depth: 1` BFS, без `Depth: infinity`.

**Локальная папка (LocalFs):**

```json
{
  "Name": "local-disk",
  "Provider": "LocalFs",
  "LocalFs": {
    "RemotePath": "D:/backups"
  }
}
```

- `RemotePath` обязателен. Это папка на хосте агента, в которую агент кладёт зашифрованные дампы. Может быть как локальным диском (`D:/backups`, `/var/backups`), так и заранее смонтированной сетевой шарой (NFS, CIFS/SMB, iSCSI-том) — для агента это просто путь.
- Промежуточные подкаталоги создаются автоматически. Запись атомарна — файл сначала пишется во временный `.upload-tmp` рядом с целевым именем и только после успешного копирования переименовывается на финальное имя; при удалении пустые подкаталоги вычищаются до `RemotePath`.
- Защита от выхода за корень: agent резолвит итоговый путь через `Path.GetFullPath` и проверяет, что он лежит под `RemotePath`. Битый или вредоносный `objectKey` (например, с `..`) приводит к явной ошибке, не к записи в произвольное место ФС.
- Никаких credentials в этом блоке нет — на дашборд тоже ничего не уходит. Доступ к каталогу обеспечивается правами ФС: на Linux — владелец/группа процесса агента, на Windows — ACL на каталог; на сетевых шарах — права на mount.
- Файловый бэкап (`FilePaths`/`FileSets`), дедупликация чанков в общий пул `chunks/{sha256}` и chunk GC — **поддерживаются полностью**, как на S3 и Azure Blob. На сетевой шаре с миллионами чанков рекурсивный листинг каталога `chunks/` будет упираться в скорость SMB/NFS — для локального диска вопросов нет.

---

## Подключение к Dashboard

Token и DashboardUrl передаются через переменные окружения (не в `appsettings.json`):

```bash
AgentSettings__Token=<токен агента из Dashboard>
AgentSettings__DashboardUrl=http://your-server:8080
```

Токен также можно брать из файла:

```bash
AgentSettings__TokenSecret__Provider=file
AgentSettings__TokenSecret__Path=/run/secrets/backupster_agent_token
AgentSettings__DashboardUrl=http://your-server:8080
```

Или из отдельной переменной окружения через `TokenSecret`:

```bash
AgentSettings__TokenSecret__Provider=env
AgentSettings__TokenSecret__Name=BACKUPSTER_AGENT_TOKEN
BACKUPSTER_AGENT_TOKEN=<токен агента из Dashboard>
AgentSettings__DashboardUrl=http://your-server:8080
```

Токен передаётся на сервер через заголовок `X-Agent-Token`. Расписание опрашивается каждые 5 минут.

> **Сетевая прозрачность.** Полный перечень HTTP-запросов агента к дашборду со схемами тел и перечнем полей — в [`NETWORK.md`](../NETWORK.md). Учётные данные БД, ключи шифрования, S3/SFTP-секреты на дашборд **никогда** не уходят.

---

## Путь к конфигу

По умолчанию агент ищет `appsettings.json` в:
- **Linux:** `/var/lib/backupster-agent/config/` для пакетной установки, `{директория exe}/config/` для ручной установки из zip
- **Windows:** `{директория exe}\config\`

Переопределяется через переменную окружения `CONFIG_PATH`.

---

## Автономный режим

Если дашборд недоступен, бэкап выполняется штатно, а метаданные записи сохраняются в локальную очередь на диске. Поведение очереди настраивается через `OutboxSettings`:

```json
"OutboxSettings": {
  "ReplayIntervalSeconds": 60,
  "MaxEntries": 1000,
  "MaxAgeDays": 14
}
```

Блок полностью опционален и в шаблон `appsettings.json` не входит.

- **`ReplayIntervalSeconds`** — как часто воркер пытается дослать накопившиеся записи на дашборд. По умолчанию 60 секунд (минимум 10).
- **`MaxEntries`** — верхний предел длины очереди. Если записей больше — самые старые (по `QueuedAt`) уезжают в `outbox/dead/` с reason `exceeded max entries (N)`. По умолчанию 1000. Значение `0` или меньше отключает лимит.
- **`MaxAgeDays`** — записи старше N дней (по `QueuedAt`) уезжают в `outbox/dead/` с reason `exceeded max age (N days)`. По умолчанию 14. Значение `0` или меньше отключает лимит.

Обрезка по возрасту и по числу выполняется в начале каждого тика replay-воркера (до попытки дослать), порядок: сначала по возрасту, потом — если очередь всё ещё над лимитом — по количеству.

Очередь лежит в `{config}/outbox/`. Туда же попадают записи, которые не удалось дослать после 100 попыток (`exceeded 100 replay attempts`), и те, что отвергнуты дашбордом как permanent (см. `DashboardAvailabilityPolicy`). Содержимое `outbox/dead/` агентом больше не обрабатывается — оператор может разобрать руками.

---

## Структура файлов в хранилище

```
{database}/{yyyy-MM-dd_HH-mm-ss}/
  {database}_{yyyyMMdd_HHmmss}.sql.gz.enc    ← PostgreSQL / MySQL дамп (logical)
  {database}_{yyyyMMdd_HHmmss}.archive.gz.enc ← MongoDB дамп (logical, mongodump --archive)
  {database}_{yyyyMMdd_HHmmss}.xbstream.gz.enc ← MySQL physical (XtraBackup)
  {database}_{yyyyMMdd_HHmmss}.pgbase.tar.enc ← PostgreSQL physical full (pg_basebackup, tar-контейнер base.tar.gz + pg_wal.tar.gz)
  {database}_{yyyyMMdd_HHmmss}_diff.pgbase.tar.enc ← PostgreSQL physical differential
  {database}_{yyyyMMdd_HHmmss}.backup_manifest.enc ← PostgreSQL physical sidecar для future DIFF/restore chain
  {database}_{yyyyMMdd_HHmmss}.tar.gz.enc    ← legacy PostgreSQL physical (читается новым агентом, новые не пишутся)
  {database}_{yyyyMMdd_HHmmss}.bacpac.enc    ← MSSQL дамп (logical, через DacFx)
  {database}_{yyyyMMdd_HHmmss}.bak.enc       ← MSSQL дамп (physical, через BACKUP DATABASE)
  manifest.json.gz.enc                       ← манифест файлового бэкапа (если FilePaths непуст)
  manifest.json.enc                          ← легаси-формат старых бэкапов (читается новым агентом)

chunks/{sha256}                              ← общий пул дедуплицированных чанков (S3 / SFTP / Azure Blob / WebDAV / LocalFs)
```

Манифест нового формата — gzip-сжатый JSON, зашифрованный framed-GCM; reader стримит его через `PipeReader` + `Utf8JsonReader` без полного разворачивания в RAM. Это позволяет бэкапить и восстанавливать файловые хранилища с миллионами мелких файлов без всплеска памяти.
