# CloudMesh

Распределённая микросервисная инфраструктура для сбора и агрегации логов/метрик.

## Архитектура

```
log-producer
  → gRPC CollectBatch (500 событий/вызов)
  → log-collector
  → Kafka cloudmesh.logs.raw (12 партиций, lz4)
  → log-aggregator (буфер 5000 строк, сброс каждые 5с)
  → ClickHouse cloudmesh.logs (MergeTree + 2 материализованных представления)
  → metrics-api FastAPI (запросы через материализованные представления)

log-aggregator → RabbitMQ cloudmesh.alerts (topic exchange)
  → q.alerts.error_rate (error% > 10% за 60с)
```

## Сервисы

| Сервис | Порт | Описание |
|---|---|---|
| log-collector | 50051 (gRPC), 50052 (HTTP) | Принимает логи от продюсеров |
| log-producer | — | Генерирует синтетические логи |
| log-aggregator | — | Kafka → ClickHouse + RabbitMQ алерты |
| metrics-api | 8000 | REST API для аналитики |

## Быстрый старт

```bash
# 1. Поднять всю инфраструктуру
docker-compose up -d

# 2. Следить за потоком событий
docker-compose logs -f log-producer log-aggregator

# 3. Открыть Swagger UI
open http://localhost:8000/docs

# 4. RabbitMQ Management UI
open http://localhost:15672  # guest / guest
```

## Проверка заявлений из резюме

### gRPC + Protobuf — 5x прирост пропускной способности

```bash
# Запустить бенчмарк (требует запущенного стека)
./scripts/benchmark.sh

# Ожидаемый результат:
# gRPC:  ~85 000 событий/сек
# HTTP:  ~16 000 событий/сек
# Ratio: ~5x
```

Причины выигрыша:
- Бинарная сериализация Protobuf vs текст JSON (~3–5x меньше по размеру)
- HTTP/2 мультиплексирование vs HTTP/1.1
- Батчинг: 500 событий на RPC-вызов против 1 события на HTTP-запрос

### Kafka + RabbitMQ — 15 млн событий в сутки

```bash
# 200 событий/сек × 86 400 сек/сутки = 17.28 млн событий ✓

# Проверить Consumer Lag (должен быть близок к 0)
docker exec -it $(docker-compose ps -q kafka) \
  kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group clickhouse-writers \
  --describe

# RabbitMQ: поднять error rate для триггера алертов
# Установить ERROR_RATE_PCT=80 в log-producer → алерт придёт на q.alerts.error_rate
```

### ClickHouse — поиск по историческим данным в 10 раз быстрее

```bash
# Запросить error rate через материализованное представление (быстро)
curl "http://localhost:8123/?query=SELECT+service_name,sum(error_count),sum(total_count)+FROM+cloudmesh.error_rate_per_minute+GROUP+BY+service_name+FORMAT+JSON"

# Сравнить с прямым сканированием сырой таблицы (медленно при большом объёме)
# Разница растёт с объёмом данных: при 10M строк — порядок величины
```

Почему ClickHouse быстрее:
- Столбцовое хранение: запрос по `level` читает только столбец `level`, не всю строку
- `LowCardinality(String)` — dictionary encoding для `service_name`, `level`
- `PARTITION BY (toYYYYMM, service_name)` — пропуск целых партиций при запросах
- Материализованные представления — предагрегированные данные, O(minutes) вместо O(rows)

### Kubernetes + HPA

```bash
# Применить манифесты (требует minikube или kind)
kubectl apply -f k8s/

# Проверить поды
kubectl get pods -n cloudmesh

# Наблюдать за автоскейлингом при нагрузке
kubectl set env deployment/log-producer -n cloudmesh EVENTS_PER_SECOND=2000
kubectl get hpa -n cloudmesh --watch
# log-collector: 2 → до 8 реплик при CPU > 60%
# log-aggregator: 1 → до 12 реплик (≤ кол-ву партиций Kafka)
```

### CI/CD — GitHub Actions

При открытии Pull Request автоматически запускается:
1. Ruff lint для каждого сервиса
2. Unit тесты (без инфраструктуры)
3. Integration тесты (ClickHouse как service container)
4. Docker build для каждого сервиса

Push в `main` → CD pipeline собирает и пушит Docker образы в Docker Hub.

## Структура проекта

```
CloudMesh/
├── proto/logs.proto              # Protobuf-контракт (единый для всех сервисов)
├── services/
│   ├── log-collector/            # gRPC сервер + Kafka producer
│   ├── log-producer/             # Генератор синтетических логов
│   ├── log-aggregator/           # Kafka consumer + ClickHouse writer + RabbitMQ alerter
│   └── metrics-api/              # FastAPI аналитика
├── scripts/
│   ├── init-clickhouse-schema.sql
│   └── benchmark.sh
├── tests/
│   ├── test_error_rate_monitor.py   # Unit тесты (без инфраструктуры)
│   ├── test_grpc_roundtrip.py
│   ├── test_metrics_api.py
│   └── test_clickhouse_write.py     # Integration тесты
├── k8s/                          # Kubernetes манифесты
├── .github/workflows/            # CI/CD
└── docker-compose.yml
```

## API Reference

После запуска стека: **http://localhost:8000/docs** (Swagger UI)

```
GET /analytics/stats          — общая статистика за 7 дней
GET /analytics/services       — список сервисов
GET /analytics/error-rate     — error rate по минутам (из матвью)
GET /analytics/volume         — объём событий по часам (из матвью)
GET /analytics/logs           — последние события (из сырой таблицы)
```

## Требования

- Docker + Docker Compose
- Python 3.11+ (для локального запуска тестов)
- minikube / kind (для проверки K8s манифестов)
