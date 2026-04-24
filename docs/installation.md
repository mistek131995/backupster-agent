# Установка и запуск

- [Требования](#требования)
- [Docker](#docker)
- [Linux (systemd)](#linux-systemd)
- [Windows (служба)](#windows-служба)
- [Для разработки](#для-разработки)
- [Поведение при пустом конфиге](#поведение-при-пустом-конфиге)

---

## Требования

- .NET 10 Runtime (или SDK для сборки из исходников)
- `pg_dump` / `psql` в `PATH` — для PostgreSQL (backup + restore)
- `mysqldump` / `mysql` в `PATH` — для MySQL/MariaDB (backup + restore)
- Для MSSQL внешние утилиты не требуются — агент работает по TDS через `Microsoft.Data.SqlClient` и `Microsoft.SqlServer.DacFx` in-process (logical `.bacpac`, physical `BACKUP DATABASE`)
- Зарегистрированный агент на [backupster.io](https://backupster.io/) (нужен токен)

---

## Docker

```bash
docker run -d --name backupster-agent \
  --restart unless-stopped \
  -e AgentSettings__Token=<токен> \
  -e AgentSettings__DashboardUrl=<url дашборда> \
  -v /root/backupster-agent:/app/config \
  ghcr.io/mistek131995/backupster-agent:latest
```

Volume `/root/backupster-agent:/app/config` сохраняет конфиг, расписание запусков (`runs/`) и очередь offline-бэкапов (`outbox/`). Без него данные пропадут при пересоздании контейнера.

Если планируете использовать файловый бэкап (`FilePaths`), смонтируйте исходные директории в контейнер отдельными томами и пропишите их контейнерные пути в `FilePaths`:

```bash
docker run -d --name backupster-agent \
  --restart unless-stopped \
  -e AgentSettings__Token=<токен> \
  -e AgentSettings__DashboardUrl=<url дашборда> \
  -v /root/backupster-agent:/app/config \
  -v /var/app/uploads:/app/data/uploads \
  -v /etc/app:/app/data/config \
  ghcr.io/mistek131995/backupster-agent:latest
```

В `appsettings.json`: `"FilePaths": ["/app/data/uploads", "/app/data/config"]`.

При первом запуске агент создаст шаблон `/app/config/appsettings.json`. Заполните его и перезапустите контейнер:

```bash
docker restart backupster-agent
```

---

## Linux (systemd)

```bash
sudo mkdir -p /opt/backupster-agent
# скопируйте опубликованные файлы в /opt/backupster-agent

sudo tee /etc/systemd/system/backupster-agent.service <<EOF
[Unit]
Description=Backupster Agent
After=network.target

[Service]
WorkingDirectory=/opt/backupster-agent
ExecStart=/opt/backupster-agent/BackupsterAgent
Environment=AgentSettings__Token=<токен>
Environment=AgentSettings__DashboardUrl=<url дашборда>
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now backupster-agent
```

При первом запуске агент создаст шаблон `/opt/backupster-agent/config/appsettings.json`. Заполните его и перезапустите службу:

```bash
sudo systemctl restart backupster-agent
```

---

## Windows (служба)

### 1. Установите .NET Runtime 10

Скачайте установщик с [dotnet.microsoft.com](https://dotnet.microsoft.com/download/dotnet/10.0) — **.NET Runtime** (не ASP.NET, не SDK) для Windows x64.

Для бэкапа БД установите клиентские утилиты под используемые СУБД:

- **PostgreSQL** — `pg_dump` / `psql` (ставятся вместе с [PostgreSQL Server](https://www.postgresql.org/download/windows/) или отдельным пакетом «Command Line Tools»).
- **MySQL / MariaDB** — `mysqldump` / `mysql` из [MySQL Community Server](https://dev.mysql.com/downloads/mysql/) или MariaDB.
- **MSSQL** — клиентские утилиты не требуются, агент работает по TDS in-process.

Убедитесь, что каталоги с `pg_dump.exe` / `mysqldump.exe` попали в системную переменную `Path`, иначе служба их не увидит.

### 2. Распакуйте агента

Скопируйте опубликованные файлы в `C:\Services\BackupsterAgent` (путь может быть любым — дальше по тексту используется именно этот):

```powershell
New-Item -ItemType Directory -Path "C:\Services\BackupsterAgent" -Force
# распакуйте архив агента в C:\Services\BackupsterAgent
```

### 3. Зарегистрируйте службу Windows

Запустите PowerShell от имени администратора и выполните:

```powershell
sc.exe create BackupsterAgent `
  binPath= "C:\Services\BackupsterAgent\BackupsterAgent.exe" `
  start= auto

[Environment]::SetEnvironmentVariable("AgentSettings__Token","<токен>","Machine")
[Environment]::SetEnvironmentVariable("AgentSettings__DashboardUrl","<url дашборда>","Machine")

Start-Service BackupsterAgent
```

> В `sc.exe` после `binPath=` и `start=` обязателен пробел — это особенность синтаксиса `sc.exe`, не опечатка.
>
> Переменные уровня `Machine` подхватываются только новыми процессами, поэтому `Start-Service` после `SetEnvironmentVariable` обязателен — иначе служба стартанёт со старым окружением.

### 4. Заполните конфиг и перезапустите службу

При первом запуске агент создаст шаблон `C:\Services\BackupsterAgent\config\appsettings.json`. Заполните его (подключения к БД, хранилище, ключ шифрования — см. [configuration.md](configuration.md)) и перезапустите службу:

```powershell
Restart-Service BackupsterAgent
```

Проверить состояние и прочитать последние записи журнала:

```powershell
Get-Service BackupsterAgent
Get-EventLog -LogName Application -Source BackupsterAgent -Newest 20
```

### Удаление службы

```powershell
Stop-Service BackupsterAgent
sc.exe delete BackupsterAgent

[Environment]::SetEnvironmentVariable("AgentSettings__Token",$null,"Machine")
[Environment]::SetEnvironmentVariable("AgentSettings__DashboardUrl",$null,"Machine")
```

---

## Для разработки

```bash
cd BackupsterAgent
dotnet run --project BackupsterAgent/BackupsterAgent.csproj
```

---

## Поведение при пустом конфиге

Агент **не падает** если конфигурация не заполнена:

- Нет ключа шифрования — логирует warning, пропускает бэкапы
- Нет настроек хранилища у storage — при первом обращении клиент упадёт с явной ошибкой; другие БД с корректным storage продолжат работать
- Нет баз данных — логирует warning, пропускает бэкапы
- Token и DashboardUrl пустые — расписание не загружается, агент простаивает

Заполните `appsettings.json` и перезапустите — агент начнёт работать.
