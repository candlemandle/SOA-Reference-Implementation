"""
Главный модуль сервиса агрегации.

Что делает:
  1. По крону читает raw-события из ClickHouse, считает бизнес-метрики,
     пишет результаты в PostgreSQL (идемпотентно).
  2. Предоставляет HTTP:
       GET  /health             — healthcheck
       POST /run?date=YYYY-MM-DD — ручной пересчёт за конкретную дату
       GET  /metrics/{date}     — посмотреть что насчитано
"""
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Query

from db import PostgresWriter, make_clickhouse_client
from metrics import compute_all_for_date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
log = logging.getLogger("aggregation")

# ---------- ENV ----------
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_DB   = os.getenv("CLICKHOUSE_DB", "analytics")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "analytics")
PG_USER = os.getenv("POSTGRES_USER", "analytics")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "analytics")

SCHEDULE_CRON = os.getenv("SCHEDULE_CRON", "*/5 * * * *")  # по умолчанию каждые 5 минут

# глобальные ресурсы
scheduler: Optional[BackgroundScheduler] = None
pg_writer: Optional[PostgresWriter] = None


# ---------- Основной цикл вычисления ----------
def run_cycle(target_date: Optional[date] = None) -> dict:
    """Полный цикл: посчитать за дату и записать в PG."""
    if target_date is None:
        # по умолчанию "вчера" UTC, потому что сегодня ещё не закрыто
        target_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()

    started = time.time()
    log.info("=== aggregation cycle started for %s ===", target_date)

    ch_client = make_clickhouse_client(CH_HOST, CH_PORT, CH_DB)

    try:
        result = compute_all_for_date(ch_client, target_date)

        # записываем в PG
        pg_writer.upsert_metrics(result["metrics"])
        pg_writer.upsert_top_movies(target_date, result["top_movies"])
        pg_writer.upsert_retention(result["retention"])

        elapsed = time.time() - started
        records = (len(result["metrics"]) +
                   len(result["top_movies"]) +
                   len(result["retention"]))

        log.info(
            "=== cycle finished, date=%s records=%d elapsed=%.2fs ===",
            target_date, records, elapsed,
        )

        return {
            "status": "ok",
            "date": str(target_date),
            "records": records,
            "elapsed_sec": round(elapsed, 2),
        }
    finally:
        try:
            ch_client.close()
        except Exception:
            pass


# ---------- Lifespan ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler, pg_writer

    pg_writer = PostgresWriter(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS)

    scheduler = BackgroundScheduler(timezone="UTC")
    # Планируем по cron-выражению из env
    trigger = CronTrigger.from_crontab(SCHEDULE_CRON)
    scheduler.add_job(
        run_cycle,
        trigger=trigger,
        id="aggregation_cycle",
        replace_existing=True,
        misfire_grace_time=300,
    )
    scheduler.start()
    log.info("scheduler started with cron '%s'", SCHEDULE_CRON)

    # первый прогон сразу, чтобы не ждать крона
    try:
        run_cycle()
    except Exception as e:
        log.warning("initial cycle failed (ClickHouse maybe empty): %s", e)

    yield

    log.info("shutting down scheduler")
    if scheduler:
        scheduler.shutdown(wait=False)


app = FastAPI(title="Aggregation Service", lifespan=lifespan)


# ---------- Endpoints ----------
@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/run")
def run_manual(target_date: Optional[str] = Query(None, alias="date")):
    """Ручной запуск пересчёта за дату (или за вчера, если не указана)."""
    parsed: Optional[date] = None
    if target_date:
        try:
            parsed = datetime.strptime(target_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(400, "date must be YYYY-MM-DD")
    try:
        return run_cycle(parsed)
    except Exception as e:
        log.exception("manual run failed")
        raise HTTPException(500, f"run failed: {e}")


@app.get("/metrics/{target_date}")
def get_metrics(target_date: str):
    """Читает посчитанные метрики из PG за указанную дату."""
    try:
        d = datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "date must be YYYY-MM-DD")

    with pg_writer.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT metric_name, metric_value, dimensions, computed_at "
            "FROM metrics_daily WHERE metric_date = %s",
            (d,),
        )
        metrics = cur.fetchall()

        cur.execute(
            "SELECT movie_id, rank, views FROM top_movies_daily "
            "WHERE metric_date = %s ORDER BY rank",
            (d,),
        )
        top = cur.fetchall()

        cur.execute(
            "SELECT day_offset, cohort_size, returned, retention_pct "
            "FROM retention_cohort WHERE cohort_date = %s ORDER BY day_offset",
            (d,),
        )
        retention = cur.fetchall()

    return {"date": target_date, "metrics": metrics,
            "top_movies": top, "retention": retention}
