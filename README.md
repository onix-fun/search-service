# Search Service

Самостоятельный сервис полнотекстового поиска поверх Meilisearch. Документы поступают через RabbitMQ, revision/idempotency state хранится в Redis, поиск доступен через HTTP API.

## Контракт индексирования

```json
{
  "event_id": "evt-1",
  "operation": "upsert",
  "collection": "subjects",
  "document_id": "opaque-id",
  "revision": 1,
  "document": {"name": "Example", "visibility": "PUBLIC"},
  "occurred_at": "2026-06-13T12:00:00Z"
}
```

`document_id` является opaque string. Для удаления передаются те же поля без `document`.

## Запуск

```bash
docker compose up -d
search-service config validate --config=config/config.example.yaml
search-service migrate-index --config=config/config.example.yaml
search-service serve --config=config/config.example.yaml --role=all
```

Поиск: `POST /v1/collections/{collection}/search`. Health endpoints: `/livez`, `/readyz`, `/metrics`.

Конфигурация коллекций задает отдельный index, RabbitMQ topology, searchable/filterable/sortable/returnable fields. Секреты передаются через environment variables.

## Надежность

- at-least-once RabbitMQ consumer;
- latest-wins по монотонной revision;
- Redis lease для active-passive worker;
- retry и DLQ;
- подтверждение сообщения только после завершения Meilisearch task.

Лицензия: MIT.
