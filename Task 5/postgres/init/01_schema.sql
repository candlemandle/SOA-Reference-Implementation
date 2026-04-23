-- ============================================================
-- PostgreSQL: хранилище "готовых" бизнес-метрик
-- ============================================================
-- Таблица metrics_daily — универсальная таблица ключ-значение
-- на каждый день. UNIQUE (metric_date, metric_name) + UPSERT
-- обеспечивают идемпотентность: пересчёт за ту же дату обновляет,
-- а не дублирует записи.
-- ============================================================

CREATE TABLE IF NOT EXISTS metrics_daily (
    id              BIGSERIAL PRIMARY KEY,
    metric_date     DATE        NOT NULL,
    metric_name     TEXT        NOT NULL,
    metric_value    DOUBLE PRECISION NOT NULL,
    dimensions      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_metric UNIQUE (metric_date, metric_name, dimensions)
);

CREATE INDEX IF NOT EXISTS idx_metrics_date ON metrics_daily (metric_date);
CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics_daily (metric_name);


-- Когортный анализ retention: по каждой когорте (по дате первой
-- активности) храним процент вернувшихся на day_offset = 0..7
CREATE TABLE IF NOT EXISTS retention_cohort (
    cohort_date  DATE    NOT NULL,
    day_offset   INT     NOT NULL,
    cohort_size  INT     NOT NULL,
    returned     INT     NOT NULL,
    retention_pct DOUBLE PRECISION NOT NULL,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cohort_date, day_offset)
);

CREATE INDEX IF NOT EXISTS idx_retention_date ON retention_cohort (cohort_date);


-- Топ фильмов за день (удобнее хранить отдельной таблицей, чтобы не пихать
-- массив в jsonb)
CREATE TABLE IF NOT EXISTS top_movies_daily (
    metric_date DATE    NOT NULL,
    movie_id    TEXT    NOT NULL,
    rank        INT     NOT NULL,
    views       BIGINT  NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (metric_date, movie_id)
);

CREATE INDEX IF NOT EXISTS idx_top_movies_date_rank
    ON top_movies_daily (metric_date, rank);
