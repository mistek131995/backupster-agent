# Установка и запуск

- [Требования](#требования)
- [Docker](#docker)
- [Linux (.deb / .rpm)](#linux-deb--rpm)
- [Linux (ручная установка из zip)](#linux-ручная-установка-из-zip)
- [Windows (служба)](#windows-служба)
- [Для разработки](#для-разработки)
- [Поведение при пустом конфиге](#поведение-при-пустом-конфиге)

---

## Требования

- Готовые артефакты (`.deb`, `.rpm`, `linux-x64.zip`, `win-x64.zip`, Docker-образ) self-contained — .NET Runtime ставить не нужно. SDK нужен только для сборки из исходников.
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

## Linux (.deb / .rpm)

Готовые пакеты публикуются на каждом релизе в [GitHub Releases](https://github.com/mistek131995/backupster-agent/releases/latest). Postinst-скрипт сам создаёт системного юзера `backupster`, каталоги в `/var/lib/backupster-agent/{config,outbox,runs,temp}`, ставит systemd-юнит и `enable`-ит его.

### 1. Установите пакет

```bash
# Debian / Ubuntu
sudo apt install ./backupster-agent_*_amd64.deb

# RHEL / Rocky / Fedora
sudo dnf install ./backupster-agent-*.x86_64.rpm
```

Дополнительно поставьте клиент СУБД: `pg_dump` / `psql` (PostgreSQL) или `mysqldump` / `mysql` (MySQL / MariaDB). Для MSSQL внешних бинарников не требуется.

### 2. Впишите токен в env-файл

Юнит читает `EnvironmentFile=/etc/backupster-agent/env`. Файл помечен как `config|noreplace`, при апгрейде пакета не перезаписывается:

```bash
sudo tee /etc/backupster-agent/env >/dev/null <<EOF
AgentSettings__Token=<токен>
AgentSettings__DashboardUrl=<url дашборда>
EOF
```

### 3. Запустите сервис

```bash
sudo systemctl start backupster-agent
journalctl -u backupster-agent -f
```

### 4. Заполните конфиг и перезапустите

При первом запуске агент создаёт шаблон `/var/lib/backupster-agent/config/appsettings.json`. Заполните подключения к БД, ключ шифрования и хранилище (см. [configuration.md](configuration.md)), затем:

```bash
sudo systemctl restart backupster-agent
```

### Удаление

```bash
sudo apt remove backupster-agent     # Debian / Ubuntu
sudo dnf remove backupster-agent     # RHEL / Rocky / Fedora
```

Каталог `/var/lib/backupster-agent/` (с конфигом, очередью offline-бэкапов и состоянием расписания) и `/etc/backupster-agent/env` сохраняются. Удалите их вручную, если нужна полная очистка.

---

## Linux (ручная установка из zip)

Используйте, если пакетный путь не подходит — другая архитектура, кастомная директория, минималистичный образ без `systemd`. Self-contained zip публикуется в тех же [GitHub Releases](https://github.com/mistek131995/backupster-agent/releases/latest).

```bash
sudo mkdir -p /opt/backupster-agent
sudo unzip BackupsterAgent-*-linux-x64.zip -d /opt/backupster-agent
sudo chmod +x /opt/backupster-agent/BackupsterAgent

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

### 1. Скачайте и распакуйте архив

Возьмите `BackupsterAgent-vX.Y.Z-win-x64.zip` с [GitHub Releases](https://github.com/mistek131995/backupster-agent/releases/latest). Бинарь self-contained — .NET Runtime ставить не нужно.

```powershell
New-Item -ItemType Directory -Path "C:\Services\BackupsterAgent" -Force
Expand-Archive -Path "BackupsterAgent-*-win-x64.zip" -DestinationPath "C:\Services\BackupsterAgent"
```

Дополнительно установите клиент СУБД, под которую делаются бэкапы:

- **PostgreSQL** — `pg_dump` / `psql` ([PostgreSQL Server](https://www.postgresql.org/download/windows/) или Command Line Tools).
- **MySQL / MariaDB** — `mysqldump` / `mysql` из [MySQL Community Server](https://dev.mysql.com/downloads/mysql/) или MariaDB.
- **MSSQL** — клиентских утилит не нужно, агент работает по TDS in-process.

Убедитесь, что каталоги с `pg_dump.exe` / `mysqldump.exe` попали в системную переменную `Path`, иначе служба их не увидит.

### 2. Зарегистрируйте службу Windows

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

### 3. Заполните конфиг и перезапустите службу

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
