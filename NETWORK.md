# Backupster Agent — сетевая прозрачность

Этот документ — полный перечень HTTP-запросов, которые агент (`BackupsterAgent`) делает к дашборду. Для каждого запроса указаны метод, URL, заголовки, точная схема тела и пример.

Цель документа — дать администратору возможность убедиться: **с хоста, где стоит агент, на дашборд уходит только сетевая топология (имена, хосты, порты, имена БД), но никогда — учётные данные подключений к БД, ключи шифрования, секреты хранилищ (S3, SFTP, Azure Blob, WebDAV) и пути локальных хранилищ (LocalFs)**.

Документ обязан обновляться вместе с кодом в том же PR, что и изменения сетевого поведения агента.

---

## Общие принципы

### Аутентификация

Каждый запрос к дашборду содержит заголовок:

```
X-Agent-Token: <AgentSettings.Token>
```

Токен задаётся через env var `AgentSettings__Token` (см. README агента). В теле запроса и в query-строке токен не передаётся никогда. В логах агента печатается только префикс `token[..8]`.

### Версия агента

Каждый запрос к дашборду дополнительно содержит заголовок:

```
X-Agent-Version: <agent semver, например 1.2.3>
```

Значение читается из `AssemblyInformationalVersionAttribute` ассембли агента (см. `Configuration/AgentVersion`). Header выставляется централизованно через `DefaultRequestHeaders` всех типизированных `HttpClient`'ов в `Program.cs` (`ConfigureDashboardClient`) — все 8 агентских клиентов отправляют его автоматически.

Дашборд использует значение трояко:

- **Гейт совместимости в `AgentTokenMiddleware`.** Если версия агента не входит в поддерживаемую дашбордом полосу (или заголовок вовсе отсутствует) — запрос отвергается ответом `426 Upgrade Required` с телом `{"message": "Дашборд требует агента версии X.Y.x. Обновите агент."}` (сообщение на русском). Конкретные границы полосы зашиты в коде дашборда и могут меняться от мажорной линии к мажорной — смотри release notes дашборда; обновляйте агент до версии, объявленной совместимой. **Этот ответ возможен на любом эндпоинте секций 1–14**; в их таблицах он отдельно не повторяется. На стороне агента 426 классифицируется `DashboardAvailabilityPolicy` как `PermanentSkip` (4xx) — итерация бэкапа/таска пропускается без outbox, текст из тела ответа попадает в `Warning`-лог.
- **UI и индикатор обновления.** Версия сохраняется в `Agent.Version` (в т.ч. для несовместимой версии — чтобы оператор видел в карточке агента, куда обновляться), отображается в карточке агента, сверяется с тегом последнего релиза.
- **Узкие фиче-гейты.** Например, создание нескольких расписаний одного режима требует версии ≥ 1.4.0 — это уже отказ конкретной операции с `400 Bad Request`, а не глобальный 426.

### Формат enum по сети

Все enum сериализуются в **camelCase** (`JsonStringEnumConverter(JsonNamingPolicy.CamelCase)`). Примеры: `inProgress`, `encryptingDump`, `downloadingDump`, `success`, `failed`, `partial`, `physicalDifferential`.

В БД дашборда те же enum хранятся как `.ToString()` (`"InProgress"`, `"EncryptingDump"` и т. д.) — это внутренний формат, не сетевой.

### Формат ключей объектов

Имена дампов и манифестов в хранилище формируются провайдером, не агент-фасадом. Для всех PG/MySQL/MSSQL дампов схема единая:

```
{database}/{yyyy-MM-dd_HH-mm-ss}/{database}_{yyyyMMdd_HHmmss}.{ext}.enc
```

- `.sql.gz.enc` — PostgreSQL / MySQL logical (`pg_dump` / `mysqldump` → gzip → encrypt).
- `.xbstream.gz.enc` — MySQL physical (`xtrabackup --backup --stream=xbstream` → gzip → encrypt).
- `.bacpac.enc` — MSSQL logical (`DacFx ExportBacpac` → encrypt).
- `.bak.enc` — MSSQL physical (`BACKUP DATABASE` → encrypt).
- `.pgbase.tar.enc` — PostgreSQL physical (`pg_basebackup --format=tar --wal-method=stream --gzip` → tar-контейнер из `base.tar.gz` + `pg_wal.tar.gz` → encrypt). Контейнер — обычный uncompressed tar, два внутренних `.tar.gz` уже сжаты pg_basebackup'ом. Для DIFF имя несёт суффикс `_diff` (`{database}_{yyyyMMdd_HHmmss}_diff.pgbase.tar.enc`). Расширение `.pgbase.tar.enc` — для людей и диагностики; restore-провайдер выбирает ветку «контейнер» vs «legacy single-tar» по **magic bytes расшифрованного файла** (`1F 8B` → gzip / legacy, `ustar` на offset 257 → tar-контейнер), потому что стандартный pipeline после decrypt'а кладёт файл как `dump.bin` и теряет исходное имя.
- `.tar.gz.enc` — **legacy** формат PostgreSQL physical (агент ≤ предыдущей минорной версии, `pg_basebackup --wal-method=fetch`). Один gzipped tar с базой и `pg_wal/` внутри. Новые бэкапы не создаются; старые продолжают восстанавливаться через legacy-ветку. Для DIFF — суффикс `_diff` (`{database}_{yyyyMMdd_HHmmss}_diff.tar.gz.enc`).
- `_diff.bak.enc` — MSSQL physical differential (`BACKUP DATABASE ... WITH DIFFERENTIAL` → encrypt).
- `.backup_manifest.enc` — PostgreSQL physical sidecar к каждому full/diff: зашифрованный `backup_manifest` от `pg_basebackup`. Хранится рядом с дампом отдельным объектом (НЕ внутри `.pgbase.tar.enc`), нужен следующему DIFF (`--incremental=<path>`) и финальному `pg_combinebackup` на restore.

Для file-set'ов и файлового этапа DB-бэкапа:

```
{database|fileSetName}/{yyyy-MM-dd_HH-mm-ss}/manifest.json.gz.enc   ← новый формат (gzip-стрим)
{database|fileSetName}/{yyyy-MM-dd_HH-mm-ss}/manifest.json.enc      ← легаси (читается, не пишется)
chunks/{sha256}                                                     ← общий пул дедуплицированных чанков
```

Ни одна из сторон контракта не парсит имя файла — это просто строка. Формат описан, чтобы примеры в этом документе совпадали с реальностью.

### Транспорт

Все запросы — обычный HTTP/HTTPS на `AgentSettings.DashboardUrl`. Content-Type тела — `application/json; charset=utf-8`. Ответы без тела — `204 No Content`.

### Таймауты HTTP-клиентов

Верхние лимиты на один HTTP-вызов (задаются в `Program.cs` через `AddHttpClient(c => c.Timeout = ...)`), чтобы агент не зависал на проблемах сети:

- `BackupRecordClient`, `ScheduleService`, `ConnectionSyncService`, `DatabaseSyncService`, `FileSetSyncService`, `StorageSyncService`, `RetentionClient` — **20 с** на вызов.
- `AgentTaskClient` — **60 с** на вызов (покрывает long-poll 30 с с запасом).
- Progress-вызовы (`BackupRecordClient.ReportProgressAsync`, `AgentTaskClient.ReportProgressAsync`) поверх этого дополнительно ограничены `CancellationTokenSource.CancelAfter(3 с)` — heartbeat не должен ни при каких условиях задерживать пайплайн.

Polly-ретраи (1/2/4 с) срабатывают поверх этих лимитов. Общее время на одну операцию с ретраями — `таймаут × 3 + 7 с` в худшем случае.

### Что никогда не уходит на дашборд

- `Connections[].Username`, `Connections[].Password`
- `EncryptionSettings.Key`
- `Storages[].S3.AccessKey`, `Storages[].S3.SecretKey`
- `Storages[].Sftp.Password`, `Storages[].Sftp.PrivateKeyPath`, `Storages[].Sftp.PrivateKeyPassphrase`
- `Storages[].AzureBlob.ConnectionString`, `Storages[].AzureBlob.AccountKey`
- `Storages[].WebDav.Password`
- `Storages[].LocalFs.RemotePath` (сам путь и факт его наличия)
- Содержимое дампов, чанков, файлов (весь payload бэкапа шифруется AES-256-GCM и идёт напрямую в ваше хранилище — S3/SFTP/Azure Blob/WebDAV или локальную папку — минуя дашборд)

Если вы нашли в выхлопе агента или в трафике что-то из этого списка — это баг. Пишите в репозиторий.

---

## 1. Backup — открыть запись

Первый шаг бэкап-пайплайна. Агент просит дашборд завести запись и вернуть `id`, чтобы дальше слать прогресс и финализацию по этому id.

- **Метод / URL:** `POST {DashboardUrl}/api/v1/agent/backup-record`
- **Клиент:** `Services/Dashboard/Clients/BackupRecordClient.OpenAsync`
- **Retry:** Polly, 3 попытки (1/2/4 с)
- **Ожидаемые ответы:**
  - `201 Created` с телом — запись открыта
  - `401 Unauthorized` — токен невалиден
  - `403 Forbidden` — БД или file-set деактивированы (превышен лимит тарифа на стороне дашборда)
  - `404 Not Found` — БД не зарегистрирована у агента
  - `409 Conflict` — `baseBackupRecordId` для DIFF указан невалидно (родитель не Success, другое хранилище, помечен StorageUnreachable и т. п.)

### Тело запроса (`OpenBackupRecordDto`)

Пример для БД-бэкапа:

```json
{
  "databaseName": "mydb",
  "connectionName": "main-pg",
  "storageName": "prod-s3",
  "startedAt": "2026-04-19T03:00:00.000Z"
}
```

Пример для дифференциального бэкапа (`backupMode = physicalDifferential`, ссылка на корневой полный):

```json
{
  "databaseName": "mydb",
  "connectionName": "main-pg",
  "storageName": "prod-s3",
  "startedAt": "2026-04-19T03:30:00.000Z",
  "backupMode": "physicalDifferential",
  "baseBackupRecordId": "0f4c1c1e-7b8f-4e9a-9f2b-1b3a4c5d6e7f"
}
```

Пример для file-set бэкапа (`ConnectionName` пустой, `DatabaseName` дублирует имя file-set):

```json
{
  "databaseName": "config-backups",
  "connectionName": "",
  "storageName": "prod-s3",
  "startedAt": "2026-04-19T03:00:00.000Z",
  "databaseType": "fileSet",
  "fileSetName": "config-backups"
}
```

| Поле             | Тип             | Описание                                                              |
|------------------|-----------------|-----------------------------------------------------------------------|
| `databaseName`   | string          | Имя БД из `Databases[].Database`. Для file-set — дублирует `fileSetName` |
| `connectionName` | string          | Имя подключения из `Databases[].ConnectionName` (должно быть в `Connections[]`). Для file-set — пустая строка |
| `storageName`    | string          | Имя хранилища из `Databases[].StorageName` (должно быть в `Storages[]`). Сервер сохраняет его в записи, чтобы при retention-чистке знать, какой агент-storage обслуживает этот бэкап |
| `startedAt`      | datetime (UTC)? | Опционально. Момент реального старта бэкапа по часам агента (ISO 8601). Если не передан — сервер использует `DateTime.UtcNow`. Агент отправляет это поле всегда; при replay накопленных offline-записей передаётся реальное время старта бэкапа, случившегося до того, как дашборд стал доступен, чтобы `StartedAt` в UI не сдвигался на момент replay'я |
| `databaseType`   | enum (string)?  | `postgres`, `mysql`, `mssql`, `fileSet`. Для cron-бэкапов БД не передаётся (`null`) — сервер берёт тип из ранее зарегистрированной `AgentDatabase`. Для file-set — `fileSet`, именно по этому полю сервер отличает file-set run и авто-регистрирует `AgentDatabase` с типом `FileSet` при первом открытии |
| `fileSetName`    | string?         | Имя file-set из `FileSets[].Name`. Заполняется только для file-set run, иначе `null` |
| `backupMode`     | enum (string)?  | `logical`, `physical` или `physicalDifferential`. Режим, в котором агент снимает этот бэкап. `null` для file-set (не применимо) и для старых агентов, которые не шлют поле — в этом случае сервер инференсит по `AgentDatabase.DatabaseType`: `Mssql` → `Physical`, остальные → `Logical` (backward-compat). Значение `physicalDifferential` обязательно идёт вместе с `baseBackupRecordId` |
| `baseBackupRecordId` | Guid?       | Только для `backupMode = physicalDifferential`. Идентификатор корневого полного (Physical) бэкапа, относительно которого снимается дифференциал. Дашборд валидирует: запись существует, `Status = Success`, тот же агент и БД, то же `storageName`, не помечена `StorageUnreachable`. Несоответствие → `409 Conflict`. Для других режимов поле игнорируется/не передаётся |

### Тело ответа (`OpenBackupRecordResponseDto`)

```json
{
  "id": "0f4c1c1e-7b8f-4e9a-9f2b-1b3a4c5d6e7f"
}
```

Для дифференциального бэкапа ответ дополнительно несёт ключи родительского бэкапа, чтобы агент не делал отдельных запросов перед скачиванием:

```json
{
  "id": "2c8d9e3f-4a5b-6c7d-8e9f-0a1b2c3d4e5f",
  "baseDumpObjectKey": "mydb/2026-04-19_03-00-00/mydb_20260419_030000.tar.gz.enc",
  "basePgBaseManifestKey": "mydb/2026-04-19_03-00-00/mydb_20260419_030000.backup_manifest.enc"
}
```

| Поле                    | Тип     | Описание                                                                                                              |
|-------------------------|---------|-----------------------------------------------------------------------------------------------------------------------|
| `id`                    | Guid    | Идентификатор созданной записи                                                                                        |
| `baseDumpObjectKey`     | string? | Ключ дампа родительского бэкапа из хранилища. Возвращается только для DIFF и только если запрос содержит валидный `baseBackupRecordId`. У FULL/Logical — `null` |
| `basePgBaseManifestKey` | string? | Ключ зашифрованного `backup_manifest.enc` родительского бэкапа. Заполняется только для PostgreSQL DIFF (для MSSQL DIFF не используется — chain отслеживается через msdb) |

---

## 2. Backup — прогресс

Heartbeat-отчёт о текущей стадии бэкапа. Шлётся не чаще раза в 5 секунд + немедленно при смене стадии. Ошибки swallow-ятся (не ломают бэкап).

- **Метод / URL:** `POST {DashboardUrl}/api/v1/agent/backup-record/{id}/progress`
- **Клиент:** `Services/Dashboard/Clients/BackupRecordClient.ReportProgressAsync`
- **Retry:** нет. Таймаут 3 с
- **Ожидаемые ответы:**
  - `204 No Content` — прогресс принят
  - `401 Unauthorized` — токен невалиден
  - `403 Forbidden` — запись принадлежит другому агенту
  - `404 Not Found` — запись не найдена
  - `409 Conflict` — запись уже финализирована (`Status != InProgress`)

### Тело запроса (`BackupProgressDto`)

```json
{
  "stage": "uploadingDump",
  "processed": 52428800,
  "total": 104857600,
  "unit": "bytes",
  "currentItem": null
}
```

| Поле          | Тип           | Описание                                                                              |
|---------------|---------------|---------------------------------------------------------------------------------------|
| `stage`       | enum (string) | Одно из: `dumping`, `encryptingDump`, `uploadingDump`, `capturingFiles`               |
| `processed`   | long?         | Обработано единиц (байты для дампа, файлы для `capturingFiles`)                       |
| `total`       | long?         | Всего единиц (может быть `null`, если неизвестно заранее)                             |
| `unit`        | string?       | `"bytes"` или `"files"`                                                               |
| `currentItem` | string?       | Имя текущего файла для `capturingFiles` (не используется для стадий дампа)            |

---

## 3. Backup — финализация

Закрывает запись финальным статусом. **Идемпотентно:** повторная финализация той же записи возвращает `204` без изменений в БД.

- **Метод / URL:** `PATCH {DashboardUrl}/api/v1/agent/backup-record/{id}`
- **Клиент:** `Services/Dashboard/Clients/BackupRecordClient.FinalizeAsync`
- **Retry:** Polly, 3 попытки (1/2/4 с)
- **Ожидаемые ответы:**
  - `204 No Content` — финализировано
  - `400 Bad Request` — невалидный `Status` (`inProgress` в финализации запрещён)
  - `401 Unauthorized` — токен невалиден
  - `403 Forbidden` — запись принадлежит другому агенту
  - `404 Not Found` — запись не найдена

### Тело запроса (`FinalizeBackupRecordDto`)

```json
{
  "status": "success",
  "sizeBytes": 104857600,
  "durationMs": 42318,
  "dumpObjectKey": "mydb/2026-04-19_03-00-00/mydb_20260419_030000.sql.gz.enc",
  "errorMessage": null,
  "backupAt": "2026-04-19T03:00:42.318Z",
  "manifestKey": "mydb/2026-04-19_03-00-00/manifest.json.gz.enc",
  "filesCount": 142,
  "filesTotalBytes": 78430210,
  "newChunksCount": 39,
  "fileBackupError": null
}
```

| Поле              | Тип            | Описание                                                                                                  |
|-------------------|----------------|-----------------------------------------------------------------------------------------------------------|
| `status`          | enum (string)  | `success` или `failed`                                                                                    |
| `sizeBytes`       | long?          | Размер зашифрованного дампа                                                                               |
| `durationMs`      | long?          | Длительность всего пайплайна                                                                              |
| `dumpObjectKey`   | string?        | Ключ объекта в хранилище — на случай `Failed` может быть `null`, если дамп не успел загрузиться           |
| `errorMessage`    | string?        | Человекочитаемый текст ошибки. Не `ex.Message` с внутренностями                                           |
| `backupAt`        | datetime (UTC) | Момент завершения (ISO 8601)                                                                              |
| `manifestKey`     | string?        | Ключ манифеста файлового бэкапа. `null`, если файлы не бэкапили                                           |
| `filesCount`      | int?           | Количество файлов в манифесте                                                                             |
| `filesTotalBytes` | long?          | Суммарный размер файлов                                                                                   |
| `newChunksCount`  | int?           | Сколько чанков реально загрузилось (остальные уже были в хранилище благодаря дедупу)                      |
| `fileBackupError` | string?        | Текст ошибки этапа файлов. `null`, если этап не запускался или прошёл успешно                             |
| `pgBaseManifestKey` | string?      | Ключ зашифрованного `backup_manifest.enc` от `pg_basebackup`. Заполняется только для PostgreSQL physical (full и differential) — нужен для будущих DIFF и для `pg_combinebackup` на restore. У других СУБД и режимов — `null` |

---

## 4. Schedule — получить расписание (+ heartbeat)

Агент забирает актуальное расписание каждые 5 минут. Тот же запрос работает как heartbeat — дашборд обновляет `Agent.LastSeenAt`.

- **Метод / URL:** `GET {DashboardUrl}/api/v1/agent/schedule`
- **Клиент:** `Services/Dashboard/Clients/ScheduleService`
- **Retry:** Polly, 3 попытки (1/2/4 с)
- **Тело запроса:** пустое
- **Ожидаемые ответы:** `200 OK` c телом, `401 Unauthorized`

### Тело ответа (`ScheduleDto`)

```json
{
  "overrides": [
    {
      "databaseName": "reporting",
      "cronExpression": "0 */2 * * *",
      "isActive": true,
      "backupMode": "logical",
      "storageNames": ["prod-s3", "prod-sftp"]
    },
    {
      "databaseName": "reporting",
      "cronExpression": "0 4 * * *",
      "isActive": true,
      "backupMode": "physical",
      "storageNames": ["prod-s3"]
    }
  ]
}
```

Расписания индивидуальны per-`Id`. Дашборд 1.4.0+ может прислать любое число записей на одну `(databaseName, backupMode)`-пару; дашборд старше 1.4.0 присылает максимум одну запись на пару (это enforce'ит `AgentVersionGuard` в API расписаний). Для file-set — обычно одна запись с `backupMode: "logical"`. Массив `storageNames` означает, что одну запись расписания агент разворачивает в N независимых запусков бэкапа — по одному на каждое хранилище из массива.

| Поле                     | Тип                        | Описание                                                            |
|--------------------------|----------------------------|---------------------------------------------------------------------|
| `overrides`              | `ScheduleOverrideDto[]?`   | Массив всех активных и неактивных расписаний для БД и файл-сетов этого агента |
| `overrides[].id`         | Guid?                      | Идентификатор расписания. Если отсутствует (старый дашборд), агент вычисляет детерминированный fake-id `sha256("{databaseName}|{backupMode}|{storage}")[..16]` — стабилен между тиками и перезапусками |
| `overrides[].name`       | string?                    | Имя расписания (для UI и логов). Не используется агентом, дублируется в логах |
| `overrides[].databaseName` | string                   | Имя БД или файл-сета                                                |
| `overrides[].backupMode` | enum (string)              | `"logical"`, `"physical"` или `"physicalDifferential"`. Перед запуском `physicalDifferential` агент дополнительно дёргает `GET /api/v1/agent/backup-records/last-successful` (секция 9.5) — если живой Successful Physical не нашёлся, тик пропускается с warning |
| `overrides[].cronExpression` | string                 | Cron-выражение                                                      |
| `overrides[].isActive`   | bool                       | `false` = расписание выключено, агент пропускает                    |
| `overrides[].storageNames` | string[]                 | Имена хранилищ из `Storages[].Name`, в которые надо положить бэкап (мультиплицируется внутри `ScheduleService.GetDueSchedulesAsync` на N `ScheduleEntry`). Пустой массив или отсутствие поля (старый дашборд) → агент использует legacy-fallback на `DatabaseConfig.StorageName`/`FileSetConfig.StorageName`. Пустые/пробельные элементы пропускаются молча. Элементы, которых нет в `Storages[]` агента, при tick'е пропускаются с `Warning` |

---

## 5. Connection sync

Все четыре sync-эндпоинта в разделах 5–8 — это слоты единого `Workers/TopologySyncWorker.cs`. Воркер запускает их параллельно через `Task.WhenAll`; у каждого слота независимый backoff (10 с → 5 мин), и после первого успеха конкретный слот останавливается, не блокируя остальные. Логи помечены префиксом слота (`TopologySyncWorker[connections]: …`, `[databases]`, `[filesets]`, `[storages]`).

Отправляется один раз на старте (и после успеха — останавливается).

- **Метод / URL:** `POST {DashboardUrl}/api/v1/agent/connections`
- **Слот / клиент:** `TopologySyncWorker[connections]` → `Services/Dashboard/Sync/ConnectionSyncService`
- **Retry:** Polly, 3 попытки (1/2/4 с)
- **Ожидаемые ответы:** `204 No Content`, `400 Bad Request`, `401 Unauthorized`

### Тело запроса (`ConnectionSyncRequestDto`)

```json
{
  "connections": [
    {
      "name": "main-pg",
      "databaseType": "Postgres",
      "host": "db-prod.internal",
      "port": 5432
    },
    {
      "name": "reporting-mssql",
      "databaseType": "Mssql",
      "host": "10.0.2.15",
      "port": 1433
    }
  ]
}
```

| Поле (элемента)  | Тип    | Описание                                                                 |
|------------------|--------|--------------------------------------------------------------------------|
| `name`           | string | Имя подключения из `Connections[].Name`                                  |
| `databaseType`   | string | `Postgres`, `Mysql`, `Mssql`, `MongoDb` (строкой, PascalCase — это не enum, а сырое поле `DatabaseType` как в конфиге) |
| `host`           | string | Хост из `Connections[].Host`                                             |
| `port`           | int    | Порт из `Connections[].Port`                                             |

> **Ни `Username`, ни `Password` не попадают в это тело.** Формируется в `ConnectionSyncService.BuildPayload()` — при изменениях проверяйте, что оно по-прежнему берёт только эти четыре поля.

---

## 6. Database sync

Второй слот `TopologySyncWorker`: агент сообщает дашборду свой список БД (без file-set'ов). Отправляется один раз на старте, после успеха — слот останавливается.

- **Метод / URL:** `POST {DashboardUrl}/api/v1/agent/databases`
- **Слот / клиент:** `TopologySyncWorker[databases]` → `Services/Dashboard/Sync/DatabaseSyncService`
- **Retry:** Polly, 3 попытки (1/2/4 с)
- **Ожидаемые ответы:** `204 No Content`, `400 Bad Request`, `401 Unauthorized`

### Тело запроса (`DatabaseSyncRequestDto`)

```json
{
  "databases": [
    {
      "name": "mydb",
      "databaseType": "Postgres"
    },
    {
      "name": "reporting",
      "databaseType": "Mssql"
    }
  ]
}
```

| Поле (элемента) | Тип    | Описание                                                                                                   |
|-----------------|--------|------------------------------------------------------------------------------------------------------------|
| `name`          | string | Имя БД из `Databases[].Database`                                                                           |
| `databaseType`  | string | `Postgres`, `Mysql`, `Mssql`, `MongoDb` (PascalCase — это сырой `ToString()` от enum, не camelCase-enum по сети). Резолвится из `Connections[].DatabaseType` для этой БД через `ConnectionResolver` |

Бэкенд делает upsert по `(AgentId, Name)` среди записей, у которых `DatabaseType != FileSet`. Stale-записи (удалённые из конфига) **не чистятся** — история бэкапов сохраняется. Записи с пустым `Database` или неизвестным `ConnectionName` в payload не попадают (warning в лог).

---

## 7. FileSet sync

Третий слот `TopologySyncWorker`: агент сообщает дашборду свой список file-set'ов. Отправляется один раз на старте, после успеха — слот останавливается.

- **Метод / URL:** `POST {DashboardUrl}/api/v1/agent/filesets`
- **Слот / клиент:** `TopologySyncWorker[filesets]` → `Services/Dashboard/Sync/FileSetSyncService`
- **Retry:** Polly, 3 попытки (1/2/4 с)
- **Ожидаемые ответы:** `204 No Content`, `400 Bad Request`, `401 Unauthorized`

### Тело запроса (`FileSetSyncRequestDto`)

```json
{
  "fileSets": [
    {
      "name": "config-backups",
      "storageName": "prod-s3"
    }
  ]
}
```

| Поле (элемента) | Тип    | Описание                                                                 |
|-----------------|--------|--------------------------------------------------------------------------|
| `name`          | string | Имя file-set из `FileSets[].Name`                                        |
| `storageName`   | string | Имя хранилища из `FileSets[].StorageName` (должно быть в `Storages[]`)   |

Бэкенд делает upsert по `(AgentId, Name)` среди записей с `DatabaseType = FileSet`. Stale-записи не чистятся. Коллизия: имя file-set не может совпасть с именем зарегистрированной БД на том же агенте — такой запрос отклоняется с `BadRequest`. Записи с пустым `Name` или неизвестным `StorageName` в payload не попадают (warning в лог).

---

## 8. Storage sync

Четвёртый слот `TopologySyncWorker`: агент сообщает дашборду свой список хранилищ. Отправляется один раз на старте, после успеха — слот останавливается. Дашборд использует этот список в UI (`GET /api/v1/agents/{id}/storages` для dropdown'а в `BackupNowModal`/`DatabaseScheduleModal`).

- **Метод / URL:** `POST {DashboardUrl}/api/v1/agent/storages`
- **Слот / клиент:** `TopologySyncWorker[storages]` → `Services/Dashboard/Sync/StorageSyncService`
- **Retry:** Polly, 3 попытки (1/2/4 с)
- **Ожидаемые ответы:** `204 No Content`, `400 Bad Request`, `401 Unauthorized`

### Тело запроса (`StorageSyncRequestDto`)

```json
{
  "storages": [
    {
      "name": "prod-s3",
      "provider": "S3"
    },
    {
      "name": "local-archive",
      "provider": "LocalFs"
    }
  ]
}
```

| Поле (элемента) | Тип    | Описание                                                                                       |
|-----------------|--------|------------------------------------------------------------------------------------------------|
| `name`          | string | Имя хранилища из `Storages[].Name`                                                             |
| `provider`      | string | `S3`, `Sftp`, `AzureBlob`, `WebDav`, `LocalFs` (PascalCase — `.ToString()` от `UploadProvider`) |

Бэкенд делает upsert по `(AgentId, Name)`. Stale-записи не чистятся (как с базами и file-set'ами).

> **Ни credentials, ни endpoint'ы, ни bucket'ы не попадают в это тело.** Формируется в `StorageSyncService.BuildPayload()` — при изменениях проверяйте, что оно по-прежнему берёт только эти два поля.

---

## 9. Task-канал — long-poll задачи

Агент опрашивает дашборд каждые 30 секунд (long-poll). Первый запрос сразу после старта; после выполнения задачи — сразу следующий; при 5xx/сетевых ошибках backoff 10 с → 5 мин.

Канал единый для всех типов юзерских задач: `restore`, `delete`, `backup`. Все три ветки на стороне агента активны.

- **Метод / URL:** `GET {DashboardUrl}/api/v1/agent/task`
- **Клиент:** `Services/Dashboard/Clients/AgentTaskClient.FetchTaskAsync`
- **Тело запроса:** пустое
- **Ожидаемые ответы:** `204 No Content` (задач нет), `200 OK` с телом, `401 Unauthorized`

### Тело ответа (`AgentTaskForAgentDto`)

```json
{
  "id": "7a2e1d8f-9b4c-4a1f-b5c6-1d2e3f4a5b6c",
  "type": "restore",
  "restore": {
    "sourceDatabaseName": "mydb",
    "dumpObjectKey": "mydb/2026-04-18_03-00-00/mydb_20260418_030000.sql.gz.enc",
    "targetDatabaseName": "mydb_restore",
    "manifestKey": "mydb/2026-04-18_03-00-00/manifest.json.gz.enc",
    "targetFileRoot": "/var/data/myapp",
    "targetConnectionName": "main-pg",
    "storageName": "prod-s3"
  }
}
```

| Поле      | Тип           | Описание                                                                                         |
|-----------|---------------|--------------------------------------------------------------------------------------------------|
| `id`      | Guid          | ID задачи; используется во всех последующих запросах                                             |
| `type`    | enum (string) | `restore`, `delete`, `backup`                                                                    |
| `restore` | object?       | Payload для `type=restore`. Для других типов — `null`                                            |
| `delete`  | object?       | Payload для `type=delete`. Для других типов — `null`                                             |
| `backup`  | object?       | Payload для `type=backup`. Для других типов — `null`                                             |

#### `restore` payload (`RestoreTaskPayload`)

| Поле                   | Тип     | Описание                                                                                           |
|------------------------|---------|----------------------------------------------------------------------------------------------------|
| `sourceDatabaseName`   | string  | Имя исходной БД (для резолва `DatabaseConfig`, если `storageName` не пришёл)                       |
| `dumpObjectKey`        | string  | Ключ дампа в хранилище                                                                             |
| `targetDatabaseName`   | string? | Куда восстановить БД. `null` = тот же `sourceDatabaseName`                                         |
| `manifestKey`          | string? | Ключ манифеста файлов. `null` = файловой части нет, восстанавливаем только БД                      |
| `targetFileRoot`       | string? | Куда класть файлы. `null` = в служебную папку агента (`RestoreSettings.FileRestoreBasePath`, дефолт `restore-files/`); она очищается перед каждым restore и доступна только на хосте агента |
| `targetConnectionName` | string? | Override подключения. `null` = подключение из `DatabaseConfig` исходной БД                         |
| `storageName`          | string? | Override хранилища. `null` = хранилище из `DatabaseConfig` исходной БД                             |
| `backupMode`           | enum (string)? | `logical`, `physical` или `physicalDifferential` — режим, в котором был снят бэкап. Если `null` (старый дашборд), агент инференсит по `DatabaseType`: `Mssql` → `Physical`, остальные → `Logical` |
| `chain`                | object[]?      | Только для `backupMode = physicalDifferential`. Упорядоченный массив звеньев цепочки восстановления (от FULL к выбранной точке). Дашборд разворачивает цепочку из `BackupRecord.BaseBackupRecordId` сам; агент скачивает каждое звено и применяет. У других режимов — `null` |
| `chain[].backupRecordId` | Guid         | ID записи звена цепочки                                                                            |
| `chain[].dumpObjectKey` | string         | Ключ дампа звена в хранилище                                                                       |
| `chain[].backupMode`    | enum (string)  | `physical` для первого звена (корневой FULL), `physicalDifferential` для остальных                 |
| `chain[].pgBaseManifestKey` | string?    | Ключ зашифрованного `backup_manifest.enc` звена. Заполняется только для PostgreSQL (нужен `pg_combinebackup` при склейке цепочки); для MSSQL — `null` |

Пример `type=delete`:

```json
{
  "id": "1b7c8d2e-3f4a-5b6c-7d8e-9f0a1b2c3d4e",
  "type": "delete",
  "delete": {
    "storageName": "prod-s3",
    "dumpObjectKey": "mydb/2026-04-10_03-00-00/mydb_20260410_030000.sql.gz.enc",
    "manifestKey": "mydb/2026-04-10_03-00-00/manifest.json.gz.enc",
    "pgBaseManifestKey": "mydb/2026-04-10_03-00-00/mydb_20260410_030000.backup_manifest.enc"
  }
}
```

#### `delete` payload (`DeleteTaskPayload`)

| Поле                | Тип     | Описание                                                                                                                                                  |
|---------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `storageName`       | string  | Имя хранилища из `Storages[]`, в котором лежат удаляемые объекты                                                                                          |
| `dumpObjectKey`     | string? | Ключ дампа. `null`, если бэкап упал до загрузки — удалять нечего                                                                                          |
| `manifestKey`       | string? | Ключ манифеста файлов. `null` = файловой части не было                                                                                                    |
| `pgBaseManifestKey` | string? | Ключ зашифрованного `backup_manifest.enc` от `pg_basebackup`. Заполняется только для PostgreSQL physical (full и differential); для других СУБД — `null`  |

Агент удаляет перечисленные ключи (best-effort, 404 swallow) в порядке `manifestKey` → `pgBaseManifestKey` → `dumpObjectKey`; чанки, на которые манифест ссылался, становятся непривязанными — `ChunkGcWorker` уберёт их через `GcSettings.GraceHours` (дефолт 24).

Пример `type=backup` для БД (ручной «Backup Now» по БД):

```json
{
  "id": "2c8d9e3f-4a5b-6c7d-8e9f-0a1b2c3d4e5f",
  "type": "backup",
  "backup": {
    "databaseName": "mydb",
    "fileSetName": null
  }
}
```

Пример `type=backup` для file-set (ручной «Backup Now» по file-set):

```json
{
  "id": "3d9e0f4a-5b6c-7d8e-9f0a-1b2c3d4e5f60",
  "type": "backup",
  "backup": {
    "databaseName": "",
    "fileSetName": "config-backups"
  }
}
```

#### `backup` payload (`BackupTaskPayload`)

| Поле           | Тип             | Описание                                                                                                                                          |
|----------------|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `databaseName` | string          | Имя БД из `Databases[].Database` — агент находит `DatabaseConfig` и запускает `BackupJob`. Для file-set — пустая строка                           |
| `fileSetName`  | string?         | Имя file-set из `FileSets[].Name` — агент находит `FileSetConfig` и запускает `FileSetBackupJob`. `null` для БД-бэкапа                            |
| `backupMode`   | enum (string)?  | `logical`, `physical` или `physicalDifferential`. Режим, в котором агент должен снять бэкап. `null` (или поле отсутствует) → агент использует `logical`. Для file-set не применимо |
| `storageName`  | string?         | Имя хранилища из `Storages[].Name`, в которое класть бэкап. `null` (или поле отсутствует, старый дашборд) → агент использует `DatabaseConfig.StorageName`/`FileSetConfig.StorageName` как legacy-fallback. Если значение задано, но storage не найден в конфиге агента — задача завершается `Failed` с RU-сообщением |
| `baseBackupRecordId` | Guid?     | Только для `backupMode = physicalDifferential`. Идентификатор корневого полного бэкапа, который дашборд уже зарезолвил. Агент кладёт его в `OpenBackupRecordDto.baseBackupRecordId` при открытии записи (секция 1). Для других режимов — `null` |

Прогресс бэкапа идёт через **record-канал** (`/api/v1/agent/backup-record/{id}/progress`, секция 3), не через task-progress — это те же стадии, что у cron-бэкапов. Task-строка в «Историю задач» показывает только финальный статус; интерактивный прогресс UI берёт из соответствующего `BackupRecord`.

---

## 9.5. Backup — найти родительский (для дифференциального)

Перед запуском дифференциального бэкапа по расписанию агент дёргает этот эндпоинт, чтобы получить ID последнего успешного полного бэкапа (`Physical`) для пары `(database, storage)`. ID идёт дальше в `OpenBackupRecordDto.baseBackupRecordId` (секция 1). Если эндпоинт вернул `404` — DIFF на этом тике пропускается с warning, родителя нет.

Эндпоинт не вызывается для ручных backup-now: там дашборд сам резолвит родителя на своей стороне и кладёт `baseBackupRecordId` уже в `BackupTaskPayload` (секция 9).

- **Метод / URL:** `GET {DashboardUrl}/api/v1/agent/backup-records/last-successful?database={name}&storage={name}&mode={enum}`
- **Клиент:** `Services/Dashboard/Clients/BackupRecordClient.GetLastSuccessfulAsync`
- **Retry:** нет (любая сетевая ошибка → агент трактует как «нет родителя» и пропускает тик)
- **Ожидаемые ответы:**
  - `200 OK` с телом — родитель найден
  - `400 Bad Request` — пустые `database`/`storage`
  - `401 Unauthorized` — токен невалиден
  - `404 Not Found` — подходящего родителя нет (или дашборд старой версии, не знает эндпоинт)

### Параметры query

| Параметр   | Тип           | Описание                                                                                                                                          |
|------------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `database` | string        | Имя БД из `Databases[].Database` (НЕ имя коннекшна)                                                                                              |
| `storage`  | string        | Имя хранилища из `Storages[].Name`                                                                                                                |
| `mode`     | enum (string) | Режим бэкапа, для которого ищем родителя. Агент всегда шлёт `physical` (родитель для DIFF — это FULL Physical). Эндпоинт принимает любое значение из `BackupMode`, но семантика «последний Successful запись данного режима для этой БД и хранилища, не помеченный StorageUnreachable» |

### Тело ответа (`LastSuccessfulBackupResponseDto`)

```json
{
  "id": "0f4c1c1e-7b8f-4e9a-9f2b-1b3a4c5d6e7f",
  "dumpObjectKey": "mydb/2026-04-19_03-00-00/mydb_20260419_030000.tar.gz.enc",
  "pgBaseManifestKey": "mydb/2026-04-19_03-00-00/mydb_20260419_030000.backup_manifest.enc",
  "backupAt": "2026-04-19T03:00:42.318Z"
}
```

| Поле                | Тип            | Описание                                                                                       |
|---------------------|----------------|------------------------------------------------------------------------------------------------|
| `id`                | Guid           | ID родительского бэкапа — кладётся в `OpenBackupRecordDto.baseBackupRecordId` при следующем тике |
| `dumpObjectKey`     | string?        | Ключ дампа родителя. Информативный — агент использует ключ, который придёт в ответе на open    |
| `pgBaseManifestKey` | string?        | Ключ зашифрованного `backup_manifest.enc` родителя. Для MSSQL — `null`                          |
| `backupAt`          | datetime?(UTC) | Когда родитель был снят (ISO 8601). Используется только для лога                                |

---

## 10. Task — прогресс

Heartbeat задачи. Шлётся не чаще раза в 5 секунд + немедленно при смене стадии. Ошибки swallow-ятся.

- **Метод / URL:** `POST {DashboardUrl}/api/v1/agent/task/{id}/progress`
- **Клиент:** `Services/Dashboard/Clients/AgentTaskClient.ReportProgressAsync`
- **Retry:** нет. Таймаут 3 с
- **Ожидаемые ответы:**
  - `204 No Content` — прогресс принят
  - `401 Unauthorized` — токен невалиден
  - `403 Forbidden` — задача принадлежит другому агенту
  - `404 Not Found` — задача не найдена
  - `409 Conflict` — задача уже не `InProgress`

### Тело запроса (`AgentTaskProgressDto`)

```json
{
  "stage": "restoringFiles",
  "processed": 84,
  "total": 142,
  "unit": "files",
  "currentItem": "/var/data/myapp/assets/logo.png"
}
```

| Поле          | Тип     | Описание                                                                                                                                                                  |
|---------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `stage`       | string  | camelCase-имя стадии. Словарь значений зависит от `type` задачи: для `restore` — `downloadingDump`, `decryptingDump`, `decompressingDump`, `preparingDatabase`, `restoringDatabase`, `downloadingManifest`, `restoringFiles`; для `delete` — `resolving`, `deletingManifest`, `deletingDump`, `completed`. Для `type=backup` task-progress не отправляется — прогресс идёт через record-канал (секция 3) |
| `processed`   | long?   | Обработано единиц                                                                                                                                                         |
| `total`       | long?   | Всего единиц                                                                                                                                                              |
| `unit`        | string? | `"bytes"` или `"files"`                                                                                                                                                   |
| `currentItem` | string? | Имя текущего файла/объекта (иначе `null`)                                                                                                                                 |

---

## 11. Task — финализация

Закрывает задачу финальным статусом. **Идемпотентно:** повторная финализация уже финального таска возвращает текущее состояние без изменений в БД (см. `AgentTaskService.PatchTaskAsync` — ветка `IsFinal(task.Status)`).

- **Метод / URL:** `PATCH {DashboardUrl}/api/v1/agent/task/{id}`
- **Клиент:** `Services/Dashboard/Clients/AgentTaskClient.PatchTaskAsync`
- **Retry:** Polly, 3 попытки (1/2/4 с)
- **Ожидаемые ответы:**
  - `200 OK` с телом `AgentTaskResponseDto` — текущее состояние таска (после финализации либо без изменений, если уже финален)
  - `400 Bad Request` — невалидный `Status`
  - `401 Unauthorized` — токен невалиден
  - `403 Forbidden` — задача принадлежит другому агенту
  - `404 Not Found` — задача не найдена

Агент тело ответа не парсит — Polly считает любой 2xx успехом. Тело описано на случай, если в будущем потребуется.

### Тело запроса (`PatchAgentTaskDto`)

```json
{
  "status": "partial",
  "errorMessage": "2 файла не удалось восстановить: permission denied ...",
  "restore": {
    "databaseStatus": "success",
    "filesStatus": "partial",
    "filesRestoredCount": 140,
    "filesFailedCount": 2
  }
}
```

| Поле           | Тип           | Описание                                                                                                      |
|----------------|---------------|---------------------------------------------------------------------------------------------------------------|
| `status`       | enum (string) | `success`, `failed`, `partial`                                                                                |
| `errorMessage` | string?       | Человекочитаемое сообщение. Для `partial` — список первых 20 ошибок, обрезанный до 2000 символов              |
| `restore`      | object?       | Type-specific результат для `type=restore`. Для других типов — `null`                                         |
| `backup`       | object?       | Type-specific результат для `type=backup`. Для других типов — `null`                                          |

#### `restore` result (`RestoreTaskResult`)

| Поле                 | Тип           | Описание                                                                                        |
|----------------------|---------------|-------------------------------------------------------------------------------------------------|
| `databaseStatus`     | enum (string) | `success`, `failed`. `null` — задача не затрагивала БД                                          |
| `filesStatus`        | enum (string) | `success`, `failed`, `partial`, `skipped`. `null` — задача не затрагивала файлы                 |
| `filesRestoredCount` | int?          | Сколько файлов успешно восстановилось                                                           |
| `filesFailedCount`   | int?          | Сколько файлов не удалось восстановить                                                          |

#### `backup` result (`BackupTaskResult`)

| Поле             | Тип   | Описание                                                                                          |
|------------------|-------|---------------------------------------------------------------------------------------------------|
| `backupRecordId` | Guid? | ID записи `BackupRecord`, созданной в record-канале. Дашборд связывает таск с записью по этому ID |

Для `type=delete` отдельного `result`-поля нет: исход сообщается только через `status` + `errorMessage`.

---

## 12. Retention — забрать просроченные записи

Агент-оркестратор retention периодически (раз в `RetentionSettings.IntervalHours`, дефолт 6 часов) забирает у дашборда пачку записей, у которых истёк срок хранения по тарифу пользователя.

- **Метод / URL:** `GET {DashboardUrl}/api/v1/agent/backup-records/expired?limit=N`
- **Клиент:** `Services/Dashboard/Clients/RetentionClient.GetExpiredAsync`
- **Retry:** нет
- **Тело запроса:** пустое
- **Ожидаемые ответы:** `200 OK` с массивом, `401 Unauthorized`

Сервер фильтрует записи: только этого агента, статус не `inProgress`, `BackupAt < NOW − retentionDays(plan)`, `StorageName` непустой, `StorageUnreachableAt IS NULL` (записи, ранее помеченные как «нет хранилища», в выдачу не попадают — иначе агент бы зацикливался).

### Тело ответа (`ExpiredBackupRecordDto[]`)

```json
[
  {
    "id": "0f4c1c1e-7b8f-4e9a-9f2b-1b3a4c5d6e7f",
    "storageName": "prod-s3",
    "dumpObjectKey": "mydb/2026-04-01_03-00-00/mydb_20260401_030000.sql.gz.enc",
    "manifestKey": "mydb/2026-04-01_03-00-00/manifest.json.gz.enc"
  }
]
```

| Поле            | Тип     | Описание                                                                                  |
|-----------------|---------|-------------------------------------------------------------------------------------------|
| `id`            | Guid    | ID записи                                                                                 |
| `storageName`   | string  | Имя хранилища, в котором лежат `dumpObjectKey` и `manifestKey`                            |
| `dumpObjectKey` | string? | Ключ дампа в хранилище. `null`, если бэкап упал до загрузки                               |
| `manifestKey`   | string? | Ключ манифеста файлов. `null`, если файловой части не было. У легаси-бэкапов может оканчиваться на `manifest.json.enc` |

---

## 13. Retention — удалить запись

После того как агент удалил `dumpObjectKey` и `manifestKey` из хранилища, он закрывает запись.

- **Метод / URL:** `DELETE {DashboardUrl}/api/v1/agent/backup-records/{id}`
- **Клиент:** `Services/Dashboard/Clients/RetentionClient.DeleteAsync`
- **Retry:** Polly, 3 попытки (1/2/4 с)
- **Тело запроса:** пустое
- **Ожидаемые ответы:** `204 No Content`, `403 Forbidden` (запись чужого агента), `401 Unauthorized`

Идемпотентно: повторный DELETE на уже удалённую запись возвращает `204`.

---

## 14. Retention — пометить запись как «нет хранилища»

Если для записи в выдаче `/expired` агент не нашёл `storageName` в своём конфиге `Storages[]`, он одним батчем сообщает об этом дашборду. Бэкенд выставляет таким записям `StorageUnreachableAt = NOW`, после чего они исключаются из последующих выдач `/expired` (см. фильтр в #11). UI покажет такую запись с пометкой «хранилище не найдено» и предложит ручное действие.

- **Метод / URL:** `POST {DashboardUrl}/api/v1/agent/backup-records/mark-unreachable`
- **Клиент:** `Services/Dashboard/Clients/RetentionClient.MarkStorageUnreachableAsync`
- **Retry:** Polly, 3 попытки (1/2/4 с)
- **Ожидаемые ответы:** `204 No Content`, `403 Forbidden` (хотя бы одна запись принадлежит чужому агенту), `401 Unauthorized`

### Тело запроса (`MarkStorageUnreachableDto`)

```json
{
  "ids": [
    "0f4c1c1e-7b8f-4e9a-9f2b-1b3a4c5d6e7f",
    "1a2b3c4d-5e6f-7081-9293-a4b5c6d7e8f9"
  ]
}
```

| Поле  | Тип      | Описание                                                                       |
|-------|----------|--------------------------------------------------------------------------------|
| `ids` | Guid[]   | ID записей, для которых агент не смог разрешить `storageName` через `StorageResolver` |

---

## Итоговая сводка: где формируется payload

| Запрос                          | Код, который строит тело                                     |
|---------------------------------|--------------------------------------------------------------|
| `POST /backup-record`           | `Services/Dashboard/Clients/BackupRecordClient.OpenAsync`            |
| `POST /backup-record/{id}/progress` | `Services/Common/Progress/ProgressReporterFactory.ToDto` → `BackupRecordClient.ReportProgressAsync` |
| `PATCH /backup-record/{id}`     | `Services/Backup/BackupJob.BuildFinalizeDto` → `BackupRecordClient.FinalizeAsync` |
| `GET /backup-records/last-successful` | — (query-параметры собираются в `BackupRecordClient.GetLastSuccessfulAsync`)         |
| `GET /schedule`                 | —                                                            |
| `POST /connections`             | `Services/Dashboard/Sync/ConnectionSyncService.BuildPayload`      |
| `POST /databases`               | `Services/Dashboard/Sync/DatabaseSyncService.BuildPayload`        |
| `POST /filesets`                | `Services/Dashboard/Sync/FileSetSyncService.BuildPayload`         |
| `POST /storages`                | `Services/Dashboard/Sync/StorageSyncService.BuildPayload`         |
| `GET /task`                     | —                                                            |
| `POST /task/{id}/progress`      | `Services/Common/Progress/ProgressReporterFactory.ToTaskDto` → `AgentTaskClient.ReportProgressAsync` |
| `PATCH /task/{id}`              | `Workers/Handlers/{Restore,Delete,Backup}TaskHandler.HandleAsync` (выбираются через `AgentTaskPollingService.DispatchAsync` по `task.Type`) → `AgentTaskClient.PatchTaskAsync` |
| `GET /backup-records/expired`   | —                                                                                       |
| `DELETE /backup-records/{id}`   | —                                                                                       |
| `POST /backup-records/mark-unreachable` | `Workers/RetentionWorker` (собирает список нерезолвнутых) → `RetentionClient.MarkStorageUnreachableAsync` |

Если вы меняете что-то в этих файлах — обновите и этот документ в том же PR.
