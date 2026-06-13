# Search Service

Go-сервис полнотекстового поиска UUID. События полного состояния документа приходят через Redis Streams, индексируются в Meilisearch, а клиенты выполняют поиск через gRPC.

## Возможности

- `upsert` и `delete` с latest-wins семантикой по монотонной `revision`;
- active-passive индексатор с Redis lease и восстановлением pending-сообщений через `XAUTOCLAIM`;
- подтверждение Redis-события только после успешного завершения Meilisearch task;
- DLQ после исчерпания retry либо сразу для невалидных событий;
- ru/en транслитерация, Snowball stemming, синонимы, typo tolerance и prefix search Meilisearch;
- отдельные runtime-роли `api`, `indexer`, `all`;
- HTTP `/livez`, `/readyz` и стандартный gRPC health service.

## Локальный запуск

Запустить зависимости:

```bash
docker compose up -d redis meilisearch
```

Применить настройки индекса и синонимы:

```bash
docker compose --profile tools run --rm migrate-index
```

Запустить сервис:

```bash
docker compose up --build -d search-service
curl http://localhost:8080/readyz
```

Redis запускается с AOF persistence. Состояние `revision` и tombstone после удаления также хранятся в Redis. При потере Redis state требуется полная переиндексация.

## Публикация событий

Stream: `search.index.events`. Запись содержит JSON в поле `payload`.

```bash
redis-cli XADD search.index.events '*' payload \
  '{"event_id":"01HY0000000000000000000000","operation":"upsert","uuid":"9dd2e47e-7a2d-4b99-b7a1-ff0d94b7e301","revision":1,"source":"users","title":"Иван Петров","description":"Backend разработчик","text":"Go Redis PostgreSQL Meilisearch","keywords":["golang","backend","поиск"],"updated_at":"2026-06-02T10:00:00Z"}'
```

Удаление:

```bash
redis-cli XADD search.index.events '*' payload \
  '{"event_id":"01HY0000000000000000000001","operation":"delete","uuid":"9dd2e47e-7a2d-4b99-b7a1-ff0d94b7e301","revision":2,"updated_at":"2026-06-02T10:05:00Z"}'
```

`revision` должна строго возрастать для каждого UUID. Повтор той же версии с тем же payload считается дублем. Другой payload для уже примененной версии попадает в DLQ.

## Поиск

После установки `grpcurl`:

```bash
grpcurl -plaintext \
  -import-path api/proto \
  -proto search/v1/search.proto \
  -d '{"query":"Ivan Petrov","limit":20}' \
  localhost:9090 search.v1.SearchService/Search
```

Ответ содержит ранжированный список UUID:

```json
{"uuids":["9dd2e47e-7a2d-4b99-b7a1-ff0d94b7e301"]}
```

## DLQ

Необрабатываемые события записываются в `search.index.events.dlq`. Запись содержит исходный `payload`, `source_stream`, `source_id`, `attempts`, `reason`, `failed_at`.

```bash
redis-cli XRANGE search.index.events.dlq - +
```

## Конфигурация

Пример находится в `config/config.example.yaml`. Любой runtime-параметр можно переопределить переменной окружения с префиксом `SEARCH_`, например:

```bash
SEARCH_REDIS_ADDR=localhost:6379
SEARCH_MEILISEARCH_HOST=http://localhost:7700
SEARCH_MEILISEARCH_API_KEY=local-development-key
```

Синонимы хранятся отдельно в `config/synonyms.yaml` и применяются только командой `migrate-index`, чтобы обычный запуск сервиса не инициировал переиндексацию.

## Разработка

```bash
make generate
make test
make race
make build
make docker-build
```

Если локальный Go недоступен, проверки можно выполнить контейнером:

```bash
docker run --rm -v "$PWD":/src -w /src golang:1.26.3-alpine go test ./...
```
