# MySQL: бэкап и восстановление

Два режима бэкапа MySQL / MariaDB:

- **Logical** — `mysqldump` → gzip → encrypt. Универсальный формат, переносится между версиями; агент должен выполняться на том же хосте, что и MySQL/MariaDB.
- **Physical** — Percona XtraBackup (`xtrabackup --backup --stream=xbstream`) → gzip → encrypt. Быстрее на больших БД, но требовательнее к инфраструктуре (см. ниже).

---

## Логический бэкап (mysqldump)

Стандартный режим. Агент вызывает `mysqldump`, стримит вывод в gzip без промежуточного `.sql`-файла, шифрует и загружает в хранилище. Для восстановления — `mysql` через stdin.

### Необходимые привилегии

Для бэкапа:

```sql
GRANT SELECT, SHOW VIEW, TRIGGER, LOCK TABLES ON <database>.* TO 'backup_user'@'%';
FLUSH PRIVILEGES;
```

Для восстановления:

```sql
GRANT ALL PRIVILEGES ON <database>.* TO 'backup_user'@'%';
FLUSH PRIVILEGES;
```

---

## Физический бэкап (XtraBackup)

### Требования к инфраструктуре

1. **Linux-only.** Percona XtraBackup поддерживается агентом только на Linux. На Windows физический MySQL-бэкап и restore отвергаются сразу с понятной ошибкой; используйте `logical`.
2. **Один хост.** Агент и MySQL должны работать на одной машине — XtraBackup читает файлы данных MySQL напрямую через `datadir`.
3. **Percona XtraBackup.** Устанавливается отдельно. Версия XtraBackup должна быть совместима с мажорной версией MySQL (например, XtraBackup 8.0 для MySQL 8.0). Агент ищет бинарники `xtrabackup` и `xbstream` по тем же правилам, что и `mysqldump` (см. раздел «Поиск бинарников» ниже).
4. **Остановка MySQL.** Агент сам останавливает MySQL перед подменой `datadir` и запускает обратно после. Поддерживаются оба варианта: service-managed (systemd на Linux) — агент останавливает и запускает через `systemctl`; unmanaged (mysqld запущен вручную) — агент останавливает через SQL-команду `SHUTDOWN` и запускает процесс напрямую. Ручная остановка пользователем не требуется.
5. **Атомарный rename.** `datadir` и его родительский каталог должны быть на одной файловой системе (агент проверяет это пробным rename).

### Необходимые привилегии

Для бэкапа:

```sql
GRANT RELOAD, PROCESS, REPLICATION CLIENT ON *.* TO 'backup_user'@'%';
-- MySQL 8.0.24+: вместо RELOAD можно BACKUP_ADMIN
FLUSH PRIVILEGES;
```

Для восстановления — дополнительно (только если MySQL запущен как unmanaged-процесс, не через systemd):

```sql
GRANT SHUTDOWN ON *.* TO 'backup_user'@'%';
FLUSH PRIVILEGES;
```

Если MySQL управляется сервисом, привилегия `SHUTDOWN` не нужна — агент останавливает MySQL через менеджер сервисов. В этом случае агент должен быть запущен с правами на управление сервисом (root на Linux).

### Как работает restore

1. Распаковка `.xbstream.gz` в staging-каталог рядом с `datadir`.
2. `xtrabackup --prepare` на staging (применение redo log).
3. Сбор информации о работающем MySQL (PID, аргументы, владелец datadir, тип управления — сервис или unmanaged).
4. Остановка MySQL: через `systemctl stop` для service-managed, через SQL `SHUTDOWN` для unmanaged.
5. Атомарная подмена: текущий `datadir` → `*.old`, staging → `datadir`.
6. `chown` (Linux) для восстановления владельца.
7. Запуск MySQL: через `systemctl start` для service-managed, прямой запуск mysqld для unmanaged.
8. Health-check: агент ждёт, пока MySQL начнёт принимать TCP-подключения на порту (без аутентификации — на случай, если бэкап содержит другие credentials).

Если на любом шаге после подмены происходит ошибка — агент останавливает MySQL (если он успел подняться), откатывает `datadir` из `*.old` и пытается поднять MySQL на исходных данных.

### Ограничения

- Differential-режим для MySQL не поддерживается (только full physical).
- XtraBackup не входит в стандартную поставку MySQL — его нужно установить отдельно.
- Windows не поддерживается для физического MySQL-бэкапа и restore через XtraBackup — используйте logical-режим.
- Агент и MySQL должны выполняться на одном хосте.

---

## Поиск бинарников

Агент ищет бинарники MySQL (`mysqldump`, `mysql`, `xtrabackup`, `xbstream`, `mysqld`) по единым правилам:

1. Если в `ConnectionConfig.BinPath` указан путь — используется он.
2. Иначе агент ищет установку на хосте:
   - Windows: `C:\Program Files\MySQL\MySQL Server *\bin` и `C:\Program Files (x86)\MySQL\MySQL Server *\bin`. Если найдено несколько — выбирается каталог с наибольшей версией.
   - Linux: `/usr/local/mysql/bin` (установка из tarball).
3. Если каталог не найден — fallback на `PATH`.

В отличие от PostgreSQL, агент не опрашивает сервер о версии: MySQL-клиент обратно совместим, и `mysqldump` подходящей или более свежей версии работает корректно.

**Когда задавать `BinPath` явно.** На Windows инсталлятор MySQL часто добавляет `bin` только в `User PATH`, но агент работает как служба (LocalSystem / отдельный аккаунт) и этот PATH не видит. Либо авто-резолв находит не ту установку. В обоих случаях задайте каталог явно:

```json
{
  "Connections": [
    {
      "Name": "mysql-main",
      "DatabaseType": "Mysql",
      "Host": "127.0.0.1",
      "Port": 3306,
      "Username": "backup_user",
      "Password": "...",
      "BinPath": "C:\\Program Files\\MySQL\\MySQL Server 8.0\\bin"
    }
  ]
}
```

Override бьёт всё остальное — ни скан стандартных каталогов, ни `PATH` не опрашиваются.
