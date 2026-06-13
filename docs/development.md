# Разработка и запуск

## Требования
- Go 1.26+
- Meilisearch 1.x
- Redis
- RabbitMQ

## Команды Makefile
- `make build`: Сборка сервиса.
- `make generate`: Генерация gRPC кода из proto.
- `make swagger`: Генерация Search API документации.
- `make migrate`: Применение настроек индексов (фильтры, сортировка) в Meilisearch.

## Локальный запуск (Docker Compose)
```bash
make compose-up
```
Это запустит Redis, Meilisearch и сам Search Service.
