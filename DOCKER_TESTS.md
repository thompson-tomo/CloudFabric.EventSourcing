# Локальный запуск тестов с Docker Compose

Этот файл описывает, как быстро запустить все необходимые сервисы для локального тестирования.

## Требования

- Docker и Docker Compose
- .NET 9.0 SDK

## Быстрый старт

### 1. Запуск сервисов (PostgreSQL, Elasticsearch, CosmosDB Emulator)

```bash
docker-compose -f docker-compose.tests.yml up -d
```

### 2. Проверка статуса сервисов

```bash
docker-compose -f docker-compose.tests.yml ps
```

Подождите, пока все сервисы станут healthy:

- PostgreSQL: ~10 секунд
- Elasticsearch: ~30-60 секунд
- CosmosDB Emulator: ~1-2 минуты (первый запуск может занять больше времени)

### 3. Запуск тестов

```bash
# Все тесты
dotnet test

# Только PostgreSQL тесты
dotnet test --filter "FullyQualifiedName~Postgresql"

# Только ElasticSearch тесты
dotnet test --filter "FullyQualifiedName~ElasticSearch"

# Только CosmosDB тесты
dotnet test --filter "FullyQualifiedName~CosmosDb"

# Только InMemory тесты (не требуют внешних сервисов)
dotnet test --filter "FullyQualifiedName~InMemory"
```

### 4. Остановка сервисов

```bash
docker-compose -f docker-compose.tests.yml down
```

Для полной очистки (включая данные):

```bash
docker-compose -f docker-compose.tests.yml down -v
```

## Конфигурация

Тесты используют переменные окружения для настройки подключений. По умолчанию используются **нестандартные порты** для избежания конфликтов с локально установленными сервисами.

| Переменная | Значение по умолчанию | Описание |
|-----------|----------------------|----------|
| `POSTGRES_HOST` | `localhost` | Хост PostgreSQL |
| `POSTGRES_PORT` | `5433` | Порт PostgreSQL (нестандартный) |
| `POSTGRES_USER` | `cloudfabric_eventsourcing_test` | Пользователь PostgreSQL |
| `POSTGRES_PASSWORD` | `cloudfabric_eventsourcing_test` | Пароль PostgreSQL |
| `POSTGRES_DATABASE` | `cloudfabric_eventsourcing_test` | База данных PostgreSQL |
| `ELASTICSEARCH_URL` | `http://localhost:9222` | URL Elasticsearch (нестандартный порт) |
| `COSMOSDB_CONNECTION_STRING` | `AccountEndpoint=https://localhost:8089/;AccountKey=...` | CosmosDB connection string (нестандартный порт) |

## Сервисы

### PostgreSQL

- **Порт**: 5433 (внешний) → 5432 (внутренний)
- **Пользователь**: cloudfabric_eventsourcing_test
- **Пароль**: cloudfabric_eventsourcing_test
- **База данных**: cloudfabric_eventsourcing_test

### Elasticsearch

- **Порт**: 9222 (HTTP, внешний) → 9200 (внутренний)
- **Порт**: 9333 (Transport, внешний) → 9300 (внутренний)
- **Версия**: 8.5.0
- **Безопасность**: отключена (xpack.security.enabled=false)

### CosmosDB Emulator

> ⚠️ **Примечание:** CosmosDB эмулятор в Docker на Windows работает очень медленно. Рекомендуется использовать [нативный Windows эмулятор](https://aka.ms/cosmosdb-emulator).

Если вы установили нативный эмулятор:

- **Порт**: 8081 (стандартный) или 8089 (если настроено)
- **Account Key**: Стандартный ключ эмулятора
- **Web UI**: <https://localhost:8081/_explorer/index.html>

## Примечания

- CosmosDB эмулятор использует самоподписанный сертификат. Тесты уже настроены на его принятие.
- Для первого запуска может потребоваться дополнительное время на загрузку образов
- Elasticsearch требует минимум 512MB RAM
- CosmosDB эмулятор требует значительно больше ресурсов (~2GB RAM)
