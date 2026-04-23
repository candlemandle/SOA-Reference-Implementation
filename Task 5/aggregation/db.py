"""
Работа с БД: коннекты, идемпотентные апсерты в PostgreSQL.
"""
import logging
from contextlib import contextmanager
from typing import Iterable

import clickhouse_connect
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from tenacity import retry, stop_after_attempt, wait_exponential

from metrics import Metric

log = logging.getLogger("aggregation.db")


def make_clickhouse_client(host: str, port: int, database: str):
    return clickhouse_connect.get_client(
        host=host, port=port, database=database, username="default",
    )


class PostgresWriter:
    """Обёртка над psycopg с retry и идемпотентными UPSERT'ами."""

    def __init__(self, host: str, port: int, db: str,
                 user: str, password: str):
        self.dsn = (
            f"host={host} port={port} dbname={db} "
            f"user={user} password={password}"
        )

    @contextmanager
    def connect(self):
        # autocommit=False: коммитим всю пачку метрик в одной транзакции,
        # ошибка => rollback, consistency сохраняется.
        conn = psycopg.connect(self.dsn, row_factory=dict_row)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @retry(stop=stop_after_attempt(3),
           wait=wait_exponential(multiplier=1, min=1, max=10),
           reraise=True)
    def upsert_metrics(self, metrics: Iterable[Metric]):
        """Записывает список метрик; повторный вызов за ту же дату
        обновляет значения (ON CONFLICT DO UPDATE)."""
        metrics = list(metrics)  # материализуем, чтобы и писать, и считать
        sql = """
        INSERT INTO metrics_daily (metric_date, metric_name, metric_value,
                                   dimensions, computed_at)
        VALUES (%s, %s, %s, %s::jsonb, now())
        ON CONFLICT (metric_date, metric_name, dimensions)
        DO UPDATE SET
            metric_value = EXCLUDED.metric_value,
            computed_at  = EXCLUDED.computed_at
        """
        with self.connect() as conn, conn.cursor() as cur:
            for m in metrics:
                cur.execute(sql, (
                    m.date, m.name, m.value,
                    Jsonb(m.dimensions),
                ))
        log.info("upserted %d metric rows", len(metrics))

    @retry(stop=stop_after_attempt(3),
           wait=wait_exponential(multiplier=1, min=1, max=10),
           reraise=True)
    def upsert_top_movies(self, target_date, rows: list):
        """Полная перезапись топа за день (сначала delete, потом insert).
        За счёт транзакции это идемпотентно."""
        sql_delete = "DELETE FROM top_movies_daily WHERE metric_date = %s"
        sql_insert = """
        INSERT INTO top_movies_daily (metric_date, movie_id, rank, views,
                                      computed_at)
        VALUES (%s, %s, %s, %s, now())
        """
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(sql_delete, (target_date,))
            for row in rows:
                cur.execute(sql_insert, (
                    target_date, row["movie_id"], row["rank"], row["views"],
                ))
        log.info("upserted %d top-movies rows for %s", len(rows), target_date)

    @retry(stop=stop_after_attempt(3),
           wait=wait_exponential(multiplier=1, min=1, max=10),
           reraise=True)
    def upsert_retention(self, rows: list):
        """Идемпотентная запись когорт."""
        sql = """
        INSERT INTO retention_cohort (cohort_date, day_offset, cohort_size,
                                      returned, retention_pct, computed_at)
        VALUES (%s, %s, %s, %s, %s, now())
        ON CONFLICT (cohort_date, day_offset)
        DO UPDATE SET
            cohort_size   = EXCLUDED.cohort_size,
            returned      = EXCLUDED.returned,
            retention_pct = EXCLUDED.retention_pct,
            computed_at   = EXCLUDED.computed_at
        """
        with self.connect() as conn, conn.cursor() as cur:
            for r in rows:
                cur.execute(sql, (
                    r["cohort_date"], r["day_offset"], r["cohort_size"],
                    r["returned"], r["retention_pct"],
                ))
        log.info("upserted %d retention rows", len(rows))
