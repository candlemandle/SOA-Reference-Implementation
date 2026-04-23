-- ============================================================
-- ClickHouse schema for movie analytics
-- ============================================================
-- Идея:
-- 1. Таблица movie_events_kafka с движком Kafka — читает сырые сообщения
--    из топика movie-events, десериализует Avro через Schema Registry.
-- 2. Таблица movie_events_raw (MergeTree) — постоянное хранилище событий.
-- 3. Materialized View перекачивает данные из Kafka-таблицы в MergeTree.
-- ============================================================

CREATE DATABASE IF NOT EXISTS analytics;
USE analytics;

-- ---------- 1. Kafka Engine ----------
-- Подписывается на топик и парсит Avro через Schema Registry.
-- AvroConfluent знает про 5-байтовый magic prefix со schema ID.
CREATE TABLE IF NOT EXISTS movie_events_kafka
(
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       LowCardinality(String),
    timestamp        Int64,
    device_type      LowCardinality(String),
    session_id       String,
    progress_seconds Nullable(Int32)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:29092,kafka-2:29093',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-consumer',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 2,
    kafka_max_block_size = 1048576,
    format_avro_schema_registry_url = 'http://schema-registry:8081';


-- ---------- 2. Постоянное хранилище ----------
-- Партиционируем по дню, сортируем так, чтобы типовые запросы
-- (за период + по пользователю/фильму) били по индексу.
CREATE TABLE IF NOT EXISTS movie_events_raw
(
    event_id         UUID,
    user_id          String,
    movie_id         String,
    event_type       LowCardinality(String),
    event_time       DateTime64(3, 'UTC'),
    event_date       Date MATERIALIZED toDate(event_time),
    device_type      LowCardinality(String),
    session_id       String,
    progress_seconds Nullable(Int32),
    ingested_at      DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (event_date, event_type, user_id, event_time)
TTL event_date + INTERVAL 90 DAY;


-- ---------- 3. Materialized View: Kafka -> MergeTree ----------
-- Автоматически переливает каждое сообщение из kafka-таблицы
-- в постоянное хранилище, конвертируя типы.
CREATE MATERIALIZED VIEW IF NOT EXISTS movie_events_mv
TO movie_events_raw AS
SELECT
    toUUID(event_id)                                     AS event_id,
    user_id,
    movie_id,
    event_type,
    fromUnixTimestamp64Milli(timestamp, 'UTC')           AS event_time,
    device_type,
    session_id,
    progress_seconds
FROM movie_events_kafka;


-- ============================================================
-- Материализованные агрегаты (используются дашбордом + сервисом агрегации)
-- ============================================================

-- ---------- DAU (уникальные пользователи по дням) ----------
CREATE TABLE IF NOT EXISTS dau_agg
(
    event_date  Date,
    uniq_users  AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY event_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS dau_agg_mv
TO dau_agg AS
SELECT
    event_date,
    uniqState(user_id) AS uniq_users
FROM movie_events_raw
GROUP BY event_date;


-- ---------- Просмотры по фильмам ----------
CREATE TABLE IF NOT EXISTS movie_views_agg
(
    event_date  Date,
    movie_id    String,
    views       AggregateFunction(count)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, movie_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS movie_views_agg_mv
TO movie_views_agg AS
SELECT
    event_date,
    movie_id,
    countState() AS views
FROM movie_events_raw
WHERE event_type = 'VIEW_STARTED'
GROUP BY event_date, movie_id;


-- ---------- Средний прогресс просмотра (по завершённым) ----------
CREATE TABLE IF NOT EXISTS avg_progress_agg
(
    event_date Date,
    avg_progress AggregateFunction(avg, Int32)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY event_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS avg_progress_agg_mv
TO avg_progress_agg AS
SELECT
    event_date,
    avgState(assumeNotNull(progress_seconds)) AS avg_progress
FROM movie_events_raw
WHERE event_type = 'VIEW_FINISHED' AND progress_seconds IS NOT NULL
GROUP BY event_date;
