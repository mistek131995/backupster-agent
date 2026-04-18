# DbBackupAgent

.NET 10 Worker Service для автоматического резервного копирования баз данных.

Панель управления агентом — **[backupster.io](https://backupster.io/)**. Там регистрируется агент, выдаётся токен, настраивается расписание и смотрится история бэкапов.

## Что делает

Для каждой базы данных pipeline:

```
Dump → Encrypt → Upload → Cleanup → File Backup (только S3) → Report
```

1. **Dump** — вызывает `pg_dump` или `sqlcmd`, сохраняет файл на диск.
2. **Encrypt** — AES-256-GCM, дамп режется на фреймы по 1 МиБ (каждый со своим nonce и tag для защиты от повреждений), добавляется суффикс `.enc`.
3. **Upload** — загружает зашифрованный файл в S3 или SFTP.
4. **Cleanup** — удаляет оба локальных файла (дамп + зашифрованный), всегда, даже при ошибке.
5. **File Backup** — если `FilePaths` непуст: режет каждый файл на content-defined chunks (FastCDC, ~4 МиБ), считает sha256 и грузит только новые куски в общий пул `chunks/{sha256}` (дедупликация через HEAD). Зашифрованный манифест (список файлов + хэши) кладётся рядом с дампом в `manifest.json.enc`. **Работает только с S3** — при SFTP пропускается с warning. Ошибка на этом этапе не валит отчёт о дампе.
6. **Report** — отправляет отчёт на DbBackupDashboard о статусе дампа и (если был) файлового этапа.

Расписание запусков получает из Dashboard (cron, опрос каждые 5 минут).
Проверка cron-расписания — каждые 30 секунд.

Поддерживаемые БД: **PostgreSQL**, **MSSQL**.  
Поддерживаемые хранилища: **S3-совместимые** (MinIO, Yandex Object Storage, AWS S3, Cloudflare R2), **SFTP**.

---

## Требования

- .NET 10 Runtime (или SDK для сборки из исходников)
- `pg_dump` в `PATH` — для PostgreSQL
- `sqlcmd` в `PATH` — для MSSQL
- Зарегистрированный агент на [backupster.io](https://backupster.io/) (нужен токен)

---

## Запуск

### Docker

```bash
docker run -d --name dbbackup-agent \
  -e AgentSettings__Token=<токен> \
  -e AgentSettings__DashboardUrl=<url дашборда> \
  -v /root/dbbackup-agent:/app/config \
  ghcr.io/mistek131995/db_backup_agent:latest
```

Если планируете использовать файловый бэкап (`FilePaths`), смонтируйте исходные директории в контейнер отдельными томами и пропишите их контейнерные пути в `FilePaths`:

```bash
docker run -d --name dbbackup-agent \
  -e AgentSettings__Token=<токен> \
  -e AgentSettings__DashboardUrl=<url дашборда> \
  -v /root/dbbackup-agent:/app/config \
  -v /var/app/uploads:/app/data/uploads \
  -v /etc/app:/app/data/config \
  ghcr.io/mistek131995/db_backup_agent:latest
```

В `appsettings.json`: `"FilePaths": ["/app/data/uploads", "/app/data/config"]`.

При первом запуске агент создаст шаблон `/app/config/appsettings.json`. Заполните его и перезапустите контейнер:

```bash
docker restart dbbackup-agent
```

### Linux (systemd)

```bash
sudo mkdir -p /opt/dbbackup-agent
# скопируйте опубликованные файлы в /opt/dbbackup-agent

sudo tee /etc/systemd/system/dbbackup-agent.service <<EOF
[Unit]
Description=DbBackup Agent
After=network.target

[Service]
WorkingDirectory=/opt/dbbackup-agent
ExecStart=/opt/dbbackup-agent/DbBackupAgent
Environment=AgentSettings__Token=<токен>
Environment=AgentSettings__DashboardUrl=<url дашборда>
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now dbbackup-agent
```

При первом запуске агент создаст шаблон `/app/config/appsettings.json`. Заполните его и перезапустите службу:

```bash
sudo systemctl restart dbbackup-agent
```

### Windows (служба)

```powershell
# Распакуйте опубликованные файлы в C:\Services\DbBackupAgent

sc.exe create DbBackupAgent binPath="C:\Services\DbBackupAgent\DbBackupAgent.exe"

# Задайте переменные окружения
reg add "HKLM\SYSTEM\CurrentControlSet\Services\DbBackupAgent\Environment" ^
  /v AgentSettings__Token /t REG_SZ /d "<токен>"
reg add "HKLM\SYSTEM\CurrentControlSet\Services\DbBackupAgent\Environment" ^
  /v AgentSettings__DashboardUrl /t REG_SZ /d "<url дашборда>"

sc.exe start DbBackupAgent
```

При первом запуске агент создаст шаблон `C:\Services\DbBackupAgent\config\appsettings.json`. Заполните его и перезапустите службу:

```powershell
sc.exe stop DbBackupAgent
sc.exe start DbBackupAgent
```

### Для разработки

```bash
cd DbBackupAgent
dotnet run --project DbBackupAgent/DbBackupAgent.csproj
```

---

## Поведение при пустом конфиге

Агент **не падает** если конфигурация не заполнена:

- Нет ключа шифрования — логирует warning, пропускает бэкапы
- Нет настроек S3 — логирует warning, клиент не создаётся до первого вызова
- Нет баз данных — логирует warning, пропускает бэкапы
- Token и DashboardUrl пустые — расписание не загружается, агент простаивает

Заполните `appsettings.json` и перезапустите — агент начнёт работать.

---

## Конфигурация

Все настройки в `appsettings.json`. Любой параметр можно переопределить переменной окружения.

### Подключения и базы данных

Конфиг разделён на два списка: `Connections[]` хранит данные серверов (хост, логин, пароль, тип БД), `Databases[]` — список баз, каждая ссылается на подключение по имени. Это удобно, когда на одном сервере несколько БД — реквизиты не дублируются.

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
    "Name": "reporting-mssql",
    "DatabaseType": "Mssql",
    "Host": "localhost",
    "Port": 1433,
    "Username": "sa",
    "Password": "secret"
  }
],
"Databases": [
  {
    "ConnectionName": "main-pg",
    "Database": "mydb",
    "OutputPath": "/tmp/backups",
    "FilePaths": []
  },
  {
    "ConnectionName": "reporting-mssql",
    "Database": "mydb2",
    "OutputPath": "/tmp/backups",
    "FilePaths": ["/etc/myapp/config.yml", "/var/data/certs"]
  }
]
```

- `Name` подключения должно быть уникальным в пределах `Connections[]`.
- `ConnectionName` у БД обязан ссылаться на существующее подключение — иначе эта БД будет пропущена с ошибкой в логе, остальные продолжат работать.
- `OutputPath` — папка для временных файлов дампа. Файлы удаляются после загрузки.
- `FilePaths` — список путей к файлам или директориям для файлового бэкапа. Директории обходятся рекурсивно. Файлы режутся на content-defined chunks (FastCDC, ~4 МиБ) и дедуплицируются между бэкапами. Работает только при `UploadSettings.Provider = "S3"`. Поле необязательное, по умолчанию пустое.

### Шифрование

```json
"EncryptionSettings": {
  "Key": "<base64 от 32 байт>"
}
```

Сгенерировать ключ:

```bash
# Linux / macOS
openssl rand -base64 32

# PowerShell
[Convert]::ToBase64String((1..32 | ForEach-Object { Get-Random -Maximum 256 }))
```

### Хранилище — S3

```json
"UploadSettings": { "Provider": "S3" },
"S3Settings": {
  "EndpointUrl": "https://storage.yandexcloud.net",
  "AccessKey": "...",
  "SecretKey": "...",
  "BucketName": "my-bucket",
  "Region": "us-east-1"
}
```

> Для MinIO и Yandex Object Storage включён `ForcePathStyle` — ничего дополнительно настраивать не нужно.

### Хранилище — SFTP

```json
"UploadSettings": { "Provider": "Sftp" },
"SftpSettings": {
  "Host": "backup.example.com",
  "Port": 22,
  "Username": "backupuser",
  "Password": "",
  "PrivateKeyPath": "/root/.ssh/id_rsa",
  "PrivateKeyPassphrase": "",
  "RemotePath": "/var/backups"
}
```

Поддерживается аутентификация по паролю и по приватному ключу. Удалённые директории создаются автоматически.

> **Файловый бэкап (`FilePaths`) не работает с SFTP** — у SFTP нет дешёвого `HEAD` для дедупликации кусков. При непустом `FilePaths` дамп загрузится, файлы будут пропущены с warning.

### Подключение к Dashboard

Token и DashboardUrl передаются через переменные окружения (не в `appsettings.json`):

```bash
AgentSettings__Token=<токен агента из Dashboard>
AgentSettings__DashboardUrl=http://your-server:8080
```

Токен передаётся на сервер через заголовок `X-Agent-Token`. Расписание опрашивается каждые 5 минут.

### Путь к конфигу

По умолчанию агент ищет `appsettings.json` в:
- **Docker / Linux:** `/app/config/`
- **Windows:** `{директория exe}\config\`

Переопределяется через переменную окружения `CONFIG_PATH`.

---

## Структура файлов в хранилище

```
{database}_{yyyy-MM-dd_HH-mm-ss}/
  {database}_{yyyyMMdd_HHmmss}.sql.gz.enc    ← PostgreSQL дамп
  {database}_{yyyyMMdd_HHmmss}.bak.enc       ← MSSQL дамп
  manifest.json.enc                          ← манифест файлового бэкапа (если FilePaths непуст)

chunks/{sha256}                              ← общий пул дедуплицированных чанков (S3 only)
```

---

## Restore

Восстановление из бэкапа **уже в разработке**. Манифесты и чанки копятся в S3, но штатного инструмента для обратной сборки файлового дерева или разворачивания дампа пока нет — появится в ближайших релизах.

---

## Поведение при ошибках

- Ошибка одной БД не останавливает обработку остальных.
- Ошибка отдельного файла в `FilePaths` не останавливает обработку остальных файлов.
- Ошибка на файловом этапе не валит отчёт о дампе — он уйдёт с пометкой о файловой ошибке.
- Отчёт о дампе отправляется и при успехе, и при ошибке дампа.
- Временные файлы удаляются даже если pipeline упал.
- `ReportService` и `ScheduleService` делают до 3 повторных попыток (1 с → 2 с → 4 с) при недоступности Dashboard.

---

## Heartbeat

Агент обновляет статус "в сети" на дашборде при каждом запросе расписания (каждые 5 минут). Отдельного heartbeat-эндпоинта нет — используется `GET /api/v1/agent/schedule`.

---

## Версионирование образов и релизов

Релизы публикуются по git-тегам `v*`. Суффикс тега определяет канал:

| Суффикс | Docker-теги в GHCR | GitHub Release |
|---|---|---|
| без суффикса | сам тег + `latest` | стабильный |
| `b` (бета) | сам тег + `latest` | prerelease |
| `e` (экспериментальный) | только сам тег | prerelease |

Пока стабильных релизов нет, бета-версии (`b`) публикуются под `latest` — именно их получают пользователи, запустившие образ без явного тега. Экспериментальные (`e`) ставятся только по полному тегу и не затрагивают `latest`.

Чтобы закрепиться на конкретной версии — укажите полный тег в `docker run` вместо `latest`.
