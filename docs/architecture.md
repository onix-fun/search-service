# Архитектура Search Service

## Компоненты
1. **Search API (HTTP):** Высокоскоростной прокси к Meilisearch с проверкой прав.
2. **gRPC API:** Внутренний интерфейс для поиска и приема индексных событий.
3. **Postgres Inbox:** Durable прием событий и очередь `indexing_tasks`.
4. **Indexer Worker:** Арендует задачи из Postgres и обновляет данные в Meilisearch.
5. **Meilisearch:** Основной поисковый движок.
6. **Redis:** Используется для Leader Election между инстансами индексатора.

## Процесс индексации
1. Сервис-источник отправляет batch событий в `SearchIndex.IngestEvents` по gRPC.
2. Search валидирует envelope, дедуплицирует `(source_service, event_id)` и записывает событие в Postgres inbox.
3. Для принятого события создается `indexing_tasks`; gRPC request не вызывает Meilisearch в request path.
4. Индексатор арендует задачи через `FOR UPDATE SKIP LOCKED`, проверяет ревизию в Postgres `applied_revisions`.
5. Индексатор применяет enrichment (транслит и т.д.) и отправляет batch в Meilisearch.
