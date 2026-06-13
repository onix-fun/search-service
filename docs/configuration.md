# Конфигурация Search Service

Сервис настраивается через YAML файл (по умолчанию `config/local.yaml`) или переменные окружения.

## Параметры конфигурации

### Service
- `name`: Имя сервиса.
- `grpc_addr`: Адрес и порт для gRPC сервера (например, `:9090`).
- `http_addr`: Адрес и порт для HTTP API (например, `:8080`).
- `internal_auth_secret`: Секрет для авторизации внутренних запросов.

### Redis
Используется для распределенной блокировки (Leader Election) индексатора.
- `addr`: Адрес Redis.
- `lease_key`: Ключ блокировки.
- `lease_duration`: Время жизни блокировки.
- `lease_renew`: Интервал обновления блокировки.

### RabbitMQ
- `url`: Адрес RabbitMQ.
- `dlq_exchange`: Обменник для Dead Letter Queue.
- `dlq_queue`: Очередь для Dead Letter Queue.
- `prefetch_count`: Лимит одновременно обрабатываемых сообщений индексатором.

### Meilisearch
- `host`: Адрес Meilisearch.
- `api_key`: API ключ Meilisearch.
- `task_poll_interval`: Интервал опроса состояния задач Meilisearch.
- `task_timeout`: Таймаут ожидания выполнения задачи.

### Search
- `default_limit`: Лимит результатов поиска по умолчанию.
- `max_limit`: Максимально допустимый лимит результатов.

### Enrichment
Настройки обогащения поисковых запросов.
- `transliteration`: Включить автоматическую транслитерацию.
- `morphology`: Использовать морфологический анализ.
- `synonyms_file`: Путь к YAML файлу со словарем синонимов.

### Indexer
Параметры пакетной индексации.
- `shards`: Количество шардов (параллельных воркеров).
- `queue_size`: Размер внутреннего буфера очереди.
- `flush_interval`: Интервал принудительного сброса буфера в Meilisearch.
- `max_retries`: Максимальное количество попыток при ошибках.

### Collections
Описание индексируемых сущностей и их маппинга.
- `name`: Внутреннее имя коллекции.
- `index`: Имя индекса в Meilisearch.
- `exchange`: RabbitMQ обменник, из которого приходят события.
- `queue`: Очередь RabbitMQ для этой коллекции.
- `routing_key`: Ключ маршрутизации.
- `revision_prefix`: Префикс ключа в Redis для хранения ревизий.
- `searchable_fields`: Поля, по которым доступен полнотекстовый поиск.
- `filterable_fields`: Поля для фильтрации.
- `sortable_fields`: Поля для сортировки.
- `returnable_fields`: Поля, возвращаемые в ответе.

### API Keys
Статические ключи для HTTP Search API.
- `name`: Имя ключа.
- `value`: Значение ключа.
- `scopes`: Разрешения (например, `search:read`).
- `collections`: Список доступных коллекций (или `["*"]` для всех).
