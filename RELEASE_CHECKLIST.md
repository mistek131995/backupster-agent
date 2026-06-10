# Release checklist — BackupsterAgent

Чек-лист ручного прогона перед публикацией релиза агента. Заполняется по версиям: один прогон = одна колонка.

## Легенда

- **✓** — прогнан, прошёл.
- **✗** — прогнан, упал. Рядом — короткая пометка или ссылка на issue/PR.
- **N/A** — фича в этой версии отсутствует или по контракту должна вести себя именно так.
- *(пусто)* — не прогонялся.

Дедуп и chunk GC проверяются внутри upload-строки соответствующего провайдера: повторная загрузка того же набора → новых чанков почти ноль; GC-проход удаляет осиротевшие чанки старше `GraceHours`.

## Прогоны

| Тест-кейс | 1.4.0 | 1.4.1 | 1.4.2 | 1.5.0 |
|---|---|---|---|---|
| **Бэкап** |  |  |  |  |
| Postgres — logical | ✓ | ✓ |  | ✓ |
| Postgres — physical | ✓ | ✓ |  | ✓ |
| Postgres — physical differential | ✓ | ✓ |  | ✓ |
| MySQL — logical | ✓ |  |  | ✓ |
| MySQL — physical | N/A | N/A | N/A | ✓ |
| MongoDB — logical | N/A | N/A | N/A | ✓ |
| MSSQL — logical | ✓ | ✓ | ✓ | ✓ |
| MSSQL — physical | ✓ | ✓ | ✓ | ✓ |
| MSSQL — physical differential | ✓ | ✓ | ✓ | ✓ |
| File-set | ✓ |  |  |  |
| **Восстановление** |  |  |  |  |
| Postgres — logical | ✓ | ✓ |  | ✓ |
| Postgres — physical | ✓ | ✓ |  | ✓ |
| Postgres — physical differential | ✓ | ✓ |  | ✓ |
| MySQL — logical | ✓ |  |  | ✓ |
| MySQL — physical | N/A | N/A | N/A | ✓ |
| MongoDB — logical | N/A | N/A | N/A | ✓ |
| MSSQL — logical | ✓ | ✓ | ✓ | ✓ |
| MSSQL — physical | ✓ | ✓ | ✓ | ✓ |
| MSSQL — physical differential | ✓ | ✓ | ✓ | ✓ |
| БД + файлы вместе | ✓ |  |  |  |
| File-set only | ✓ |  |  |  |
| **Хранилища** |  |  |  |  |
| S3 — upload | ✓ |  |  | ✓ |
| S3 — download | ✓ |  |  | ✓ |
| SFTP — upload | ✓ |  |  | ✓ |
| SFTP — download | ✓ |  |  | ✓ |
| Azure Blob — upload |  |  |  |  |
| Azure Blob — download |  |  |  |  |
| WebDAV — upload | ✓ |  |  | ✓ |
| WebDAV — download | ✓ |  |  | ✓ |
| LocalFs — upload | ✓ |  |  |  |
| LocalFs — download | ✓ |  |  |  |
