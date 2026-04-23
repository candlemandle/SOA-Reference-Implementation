# Online Cinema: Event Streaming + Analytics Pipeline

Pipeline обработки событий для аналитики онлайн-кинотеатра.
Стек: **Kafka + Schema Registry + ClickHouse + PostgreSQL + Grafana + MinIO (S3)**.

---

## Быстрый старт

```bash
# Поднять весь стек одной командой
docker compose up -d --build
# или
make up
```

Готово. Дальше сервисы доступны по портам:

| Сервис | URL | Креды |
|---|---|---|
| Producer HTTP API (Swagger) | http://localhost:8000/docs | — |
| Aggregation Service | http://localhost:8001/docs | — |
| Export Service | http://localhost:8002/docs | — |
| **Grafana** | http://localhost:3000 | admin / admin |
| ClickHouse HTTP | http://localhost:8123 | — |
| Schema Registry | http://localhost:8081 | — |
| MinIO S3 API | http://localhost:9001 | minioadmin |
| MinIO Console | http://localhost:9002 | minioadmin / minioadmin |

Продюсер сразу начинает генерить синтетические события (EPS=5),
так что в Grafana через пару минут появятся данные.

### Проверить что всё живое

```bash
# события в ClickHouse
make query-ch

# агрегаты в PostgreSQL (сначала дождись первого крон-цикла или дёрни вручную)
make aggregate
make query-pg

# экспорт в S3
make export
make query-s3
```

### Интеграционный тест

```bash
make test
```

Поднимается отдельным контейнером в сети compose, публикует
событие через продюсер, ждёт его в ClickHouse, проверяет поля.

### Полный сброс

```bash
make clean   # удаляет контейнеры + volumes
```

---

## Архитектура

```
                         ┌──────────────────┐
                         │   Producer       │
                         │   (HTTP + Gen)   │
                         └─────────┬────────┘
                                   │ Avro
                                   ▼
                        ┌─────────────────────┐
                        │   Kafka (2 брокера) │◄── Schema Registry
                        │   topic movie-events│
                        │   rep.factor=2      │
                        └─────────┬───────────┘
                                  │ Kafka Engine
                                  ▼
           ┌──────────────────────────────────────┐
           │   ClickHouse (analytics)             │
           │   • movie_events_raw (MergeTree)     │
           │   • *_agg (AggregatingMergeTree)     │
           └──────┬─────────────────┬─────────────┘
                  │ SELECT          │
                  ▼                 │
        ┌──────────────────┐        │
        │ Aggregation Svc  │        │ direct query
        │ (cron + /run)    │        │
        └────────┬─────────┘        │
                 │ UPSERT           │
                 ▼                  ▼
        ┌──────────────┐     ┌─────────────┐
        │ PostgreSQL   │     │   Grafana   │
        │ (aggregates) │     │  Dashboard  │
        └──────┬───────┘     └─────────────┘
               │ SELECT
               ▼
        ┌──────────────┐
        │ Export Svc   │──► MinIO (S3)
        │ (daily cron) │    movie-analytics/daily/YYYY-MM-DD/
        └──────────────┘
```

---

## Как это соотносится с заданием

### Блок 1–4 балла: базовая инфраструктура

| # | Пункт | Где реализовано |
|---|---|---|
| 1 | **Kafka topic + Avro schema (1 балл)** | `producer/schemas/movie_event.avsc` — Avro-схема со всеми обязательными полями. Регистрируется в Schema Registry автоматически при первой публикации (`auto.register.schemas: True` в `kafka_producer.py`). Топик создаётся сервисом `kafka-init` в `docker-compose.yml` с **3 партициями** и `replication.factor=2`. Ключ партиционирования = `user_id` (обоснование: все события одной пользовательской сессии попадают в одну партицию → сохраняется порядок внутри сессии, что критично для корректного retention и вычисления `progress_seconds`). |
| 2 | **Продюсер (1 балл)** | `producer/` — FastAPI-сервис. HTTP `POST /events` принимает JSON, валидирует Pydantic-моделью + повторно Avro-схемой перед отправкой (`kafka_producer.py::produce_event`). Генератор (`generator.py`) фоново создаёт реалистичные сессии: `VIEW_STARTED → VIEW_PAUSED/RESUMED → VIEW_FINISHED`, `progress_seconds` монотонно растёт. Продюсер настроен с `acks=all`, `enable.idempotence=true`, `retries=10`, `retry.backoff.ms=500` (exponential), логирует каждую публикацию (`event_id`, `event_type`, `timestamp`, `user_id`). |
| 3 | **ClickHouse (1 балл)** | `clickhouse/init/01_schema.sql` — таблица `movie_events_kafka` (движок `Kafka`, формат `AvroConfluent` с URL Schema Registry), таблица `movie_events_raw` (`MergeTree`, `PARTITION BY event_date`, `ORDER BY (event_date, event_type, user_id, event_time)` для быстрых запросов за период + по пользователю), Materialized View `movie_events_mv` переливает данные из kafka-таблицы в MergeTree с приведением типов. |
| 4 | **Интеграционный тест (1 балл)** | `tests/test_pipeline.py` — публикует событие через HTTP API продюсера, поллит ClickHouse пока не появится запись с тем же `event_id`, проверяет все поля. Тест идемпотентный (каждый запуск — свежий UUID). Запуск одной командой: `make test`. Также есть тесты на отклонение невалидных событий и на корректную последовательность внутри сессии. |

### Блок 5–7 баллов: агрегация (все 3 балла)

| Пункт | Где реализовано |
|---|---|
| **Отдельный сервис в отдельном контейнере**, не связан с продюсером | `aggregation/` — самостоятельный микросервис. |
| **Читает raw-события из ClickHouse (не из Kafka)** | `aggregation/metrics.py` — все запросы идут к ClickHouse через `clickhouse-connect`. |
| **Крон-расписание, настраиваемое через env** | `SCHEDULE_CRON` в `docker-compose.yml`, `APScheduler` с `CronTrigger.from_crontab()`. По умолчанию каждые 5 минут (для демо). |
| **HTTP для ручного запуска за конкретную дату** | `POST /run?date=YYYY-MM-DD` в `aggregation/main.py`. |
| **Логирование цикла: сколько записей / сколько времени** | `main.py::run_cycle` логирует `cycle finished, date=X records=N elapsed=Y.YYs`. |
| **Метрики**: DAU | `uniqMerge(uniq_users)` над `dau_agg` (AggregatingMergeTree). |
| **Среднее время просмотра** | `avgMerge(avg_progress)` над `avg_progress_agg` для `VIEW_FINISHED`. |
| **Топ фильмов** | `countMerge(views)` над `movie_views_agg`. |
| **Конверсия просмотра** | `countIf(VIEW_FINISHED) / countIf(VIEW_STARTED)` из `movie_events_raw`. |
| **Retention D1, D7** | `metrics.py::compute_retention_cohort` — определяет дату первой активности каждого пользователя (`HAVING toDate(min(event_time)) = :d`), джойнит с последующей активностью, считает возвращаемость по дням. |
| **Агрегатные / оконные функции** | Используются: `uniqState/uniqMerge`, `countState/countMerge`, `avgState/avgMerge`, `countIf`, `dateDiff` — простого `count(*)` нигде нет. |
| **Материализованы в ClickHouse** | Три `AggregatingMergeTree` + MV: `dau_agg`, `movie_views_agg`, `avg_progress_agg`. |
| **Идемпотентный UPSERT в PostgreSQL** | `metrics_daily` с `UNIQUE (metric_date, metric_name, dimensions)` + `ON CONFLICT ... DO UPDATE` в `db.py::upsert_metrics`. Для retention — `PRIMARY KEY (cohort_date, day_offset)` + upsert. Для топа — `DELETE + INSERT` в одной транзакции. |
| **Обработка ошибок: retry + логирование** | `tenacity` decorator `@retry(stop_after_attempt(3), wait=wait_exponential)` на всех записях в PG. |

### Блок 8–10 баллов: визуализация, экспорт, отказоустойчивость

| # | Пункт | Где реализовано |
|---|---|---|
| 6 | **Grafana-дашборд (1 балл)** | `grafana/provisioning/datasources/clickhouse.yml` — автодатасорс на ClickHouse. `grafana/dashboards/movie_analytics.json` — дашборд с 6 панелями: **Retention Cohort Heatmap** (обязательная, CTE с `first_day` + джойн активности + нормировка на размер когорты, цветовая градация по thresholds), DAU (timeseries), View Conversion (stat с цветовой индикацией), Топ-10 фильмов (barchart), Распределение по устройствам (piechart), Среднее время просмотра (timeseries). Панели используют материализованные агрегаты (`dau_agg`, `avg_progress_agg`) где возможно. |
| 7 | **Экспорт в S3 (1 балл)** | `export_service/` — MinIO как S3-совместимое хранилище (в compose). Формат настраивается через `EXPORT_FORMAT` (csv/json/**parquet**). Путь: `s3://movie-analytics/daily/YYYY-MM-DD/aggregates.{format}`. Cron ежедневно в 01:00 UTC + `POST /run?date=...&format=...` для ручного запуска. `put_object` детерминированно перезаписывает объект → идемпотентность. Ретрай через `tenacity` при ошибках S3/PG. |
| 8 | **Отказоустойчивая Kafka (1 балл)** | 2 брокера `kafka-1` + `kafka-2` в compose. Топик `movie-events` создаётся с `--replication-factor 2` и `--config min.insync.replicas=1`. Schema Registry поднят с `SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR=2`. Healthcheck-и настроены для всех компонентов: `kafka-broker-api-versions`, `curl ping` у ClickHouse/Grafana/MinIO, `pg_isready` у Postgres, `curl /subjects` у Schema Registry, `curl /health` у кастомных сервисов. Продюсер работает с обоими брокерами (`bootstrap.servers=kafka-1:29092,kafka-2:29093`). |

---

## Типовые команды для демонстрации

```bash
# 1. Поднять стек
make up

# 2. Подождать минуту-две пока генератор наполнит данными,
#    смотреть можно логи продюсера:
docker compose logs -f producer

# 3. Послать своё событие руками
make send-test-event

# 4. Увидеть его в ClickHouse
make query-ch

# 5. Дёрнуть агрегацию вручную за сегодня
curl -X POST "http://localhost:8001/run?date=$(date -u +%F)"

# 6. Посмотреть агрегаты в PG
make query-pg

# 7. Прогнать экспорт в S3
curl -X POST "http://localhost:8002/run?date=$(date -u +%F)&format=parquet"

# 8. Проверить что файл в MinIO
# Открыть http://localhost:9002, зайти minioadmin/minioadmin,
# увидеть bucket movie-analytics/daily/YYYY-MM-DD/aggregates.parquet

# 9. Запустить интеграционные тесты
make test

# 10. Открыть Grafana
# http://localhost:3000 -> Dashboards -> Movie Analytics
```

---

## Структура проекта

```
task5/
├── docker-compose.yml          # оркестратор всех компонентов
├── Makefile                    # удобные команды
├── README.md                   # этот файл
├── producer/                   # HTTP API + генератор событий
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                 # FastAPI приложение
│   ├── kafka_producer.py       # Avro + Schema Registry, acks=all, idempotence
│   ├── generator.py            # реалистичные сессии
│   └── schemas/movie_event.avsc
├── clickhouse/
│   └── init/01_schema.sql      # Kafka Engine + MergeTree + AggregatingMergeTree
├── postgres/
│   └── init/01_schema.sql      # metrics_daily + retention_cohort + top_movies
├── aggregation/                # агрегация CH→PG
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                 # cron + HTTP
│   ├── metrics.py              # DAU, conversion, retention и т.д.
│   └── db.py                   # идемпотентные UPSERT'ы
├── export_service/             # PG → S3
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/clickhouse.yml
│   │   └── dashboards/dashboards.yml
│   └── dashboards/movie_analytics.json
└── tests/                      # интеграционный тест
    ├── Dockerfile
    ├── requirements.txt
    └── test_pipeline.py
```

---

## Что может сломаться и как чинить

- **Grafana не видит ClickHouse.** Плагин `grafana-clickhouse-datasource` устанавливается при старте — посмотри `docker compose logs grafana`, если там ошибки скачивания — перезапусти `docker compose restart grafana`.
- **ClickHouse Kafka Engine не читает.** Проверь `SELECT * FROM system.kafka_consumers`. Часто помогает `DETACH TABLE movie_events_kafka; ATTACH TABLE movie_events_kafka;`.
- **Агрегация падает с "no data".** Это нормально пока не накопились события — продюсер пишет вчерашнюю/сегодняшнюю дату, но `run_cycle()` по умолчанию считает за ВЧЕРА. Дёрни `curl -X POST "http://localhost:8001/run?date=$(date -u +%F)"` за сегодня.
- **`make test` падает с timeout.** Подними стек, подожди ~40 секунд (пока все healthcheck'и позеленеют), затем `make test`.
