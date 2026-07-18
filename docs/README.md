# Search Service Documentation

Search Service — это высокопроизводительный сервис полнотекстового поиска и индексации на базе Meilisearch.

## Содержание
- [Архитектура](architecture.md) — Обзор индексации и поиска.
- [Конфигурация](configuration.md) — Настройка Meilisearch, Redis и коллекций.
- [Разработка](development.md) — Запуск и миграции индексов.
- [API Reference](swagger.yaml) — OpenAPI спецификация Search API.

## Основные возможности
- **Async Indexing:** Durable gRPC ingest через Postgres inbox и асинхронную индексацию.
- **Advanced Search:** Поддержка транслитерации, синонимов и морфологии.
- **Multi-tenant:** Управление несколькими коллекциями данных с разграничением прав.
- **HA Indexer:** Распределенная индексация с использованием Redis для координации.
