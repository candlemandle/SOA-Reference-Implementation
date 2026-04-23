"""
Сервис ежедневного экспорта агрегатов из PostgreSQL в S3 (MinIO).

Что делает:
  - По крону выгружает все агрегаты за указанную дату одним файлом.
  - Поддерживает форматы: CSV, JSON, Parquet.
  - Кладёт файл по пути: s3://{bucket}/daily/{YYYY-MM-DD}/aggregates.{ext}
  - Повторный вызов перезаписывает (не дублирует).
  - HTTP /run?date=... — ручной запуск.

Идемпотентность: имя объекта детерминировано от даты +
boto3 put_object всегда перезаписывает -> повторный экспорт не создаёт дубликат.
"""
import io
import json
import logging
import os
from contextlib import asynccontextmanager, contextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import boto3
import pandas as pd
import psycopg
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from botocore.client import Config
from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
log = logging.getLogger("export")

# ---------- ENV ----------
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "analytics")
PG_USER = os.getenv("POSTGRES_USER", "analytics")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "analytics")

S3_ENDPOINT   = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET     = os.getenv("S3_BUCKET", "movie-analytics")
EXPORT_FORMAT = os.getenv("EXPORT_FORMAT", "parquet").lower()
SCHEDULE_CRON = os.getenv("SCHEDULE_CRON", "0 1 * * *")

scheduler: Optional[BackgroundScheduler] = None


# ---------- Helpers ----------
def pg_dsn() -> str:
    return (f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} "
            f"user={PG_USER} password={PG_PASS}")


@contextmanager
def pg_conn():
    conn = psycopg.connect(pg_dsn(), row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


# ---------- Основной экспорт ----------
@retry(stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1, min=2, max=30),
       reraise=True)
def export_date(target_date: date, fmt: str = None) -> dict:
    fmt = (fmt or EXPORT_FORMAT).lower()
    if fmt not in {"csv", "json", "parquet"}:
        raise ValueError(f"unsupported format: {fmt}")

    log.info("=== export started date=%s format=%s ===", target_date, fmt)

    # 1. Читаем агрегаты
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT metric_date, metric_name, metric_value, dimensions, "
            "computed_at FROM metrics_daily WHERE metric_date = %s",
            (target_date,),
        )
        metrics = cur.fetchall()

        cur.execute(
            "SELECT metric_date, movie_id, rank, views, computed_at "
            "FROM top_movies_daily WHERE metric_date = %s ORDER BY rank",
            (target_date,),
        )
        top = cur.fetchall()

        cur.execute(
            "SELECT cohort_date, day_offset, cohort_size, returned, "
            "retention_pct, computed_at FROM retention_cohort "
            "WHERE cohort_date = %s ORDER BY day_offset",
            (target_date,),
        )
        retention = cur.fetchall()

    total = len(metrics) + len(top) + len(retention)
    if total == 0:
        log.warning("no data for %s — nothing to export", target_date)

    # 2. Формируем файл
    payload = {
        "export_date": str(target_date),
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "metrics": [
            {**m, "metric_date": str(m["metric_date"]),
             "computed_at": m["computed_at"].isoformat()}
            for m in metrics
        ],
        "top_movies": [
            {**t, "metric_date": str(t["metric_date"]),
             "computed_at": t["computed_at"].isoformat()}
            for t in top
        ],
        "retention": [
            {**r, "cohort_date": str(r["cohort_date"]),
             "computed_at": r["computed_at"].isoformat()}
            for r in retention
        ],
    }

    buf = io.BytesIO()
    if fmt == "json":
        buf.write(json.dumps(payload, ensure_ascii=False,
                             default=str).encode("utf-8"))
        content_type = "application/json"
    elif fmt == "csv":
        # CSV: сливаем все три таблицы в одну с колонкой table_name
        rows = []
        for m in payload["metrics"]:
            rows.append({"table": "metrics", **m})
        for t in payload["top_movies"]:
            rows.append({"table": "top_movies", **t})
        for r in payload["retention"]:
            rows.append({"table": "retention", **r})
        df = pd.DataFrame(rows)
        df.to_csv(buf, index=False)
        content_type = "text/csv"
    else:  # parquet
        # Parquet удобно писать по одной таблице, соберём их в нескольких полях.
        # Самый практичный вариант — три parquet'а в одном "логическом" объекте,
        # но задание говорит "один файл за день", поэтому складываем всё с
        # колонкой table_name.
        rows = []
        for m in payload["metrics"]:
            rows.append({
                "table": "metrics",
                "metric_date": m["metric_date"],
                "metric_name": m["metric_name"],
                "metric_value": m["metric_value"],
                "dimensions": json.dumps(m["dimensions"]),
                "computed_at": m["computed_at"],
                "movie_id": None, "rank": None, "views": None,
                "cohort_date": None, "day_offset": None,
                "cohort_size": None, "returned": None, "retention_pct": None,
            })
        for t in payload["top_movies"]:
            rows.append({
                "table": "top_movies",
                "metric_date": t["metric_date"],
                "metric_name": None, "metric_value": None, "dimensions": None,
                "computed_at": t["computed_at"],
                "movie_id": t["movie_id"], "rank": t["rank"], "views": t["views"],
                "cohort_date": None, "day_offset": None,
                "cohort_size": None, "returned": None, "retention_pct": None,
            })
        for r in payload["retention"]:
            rows.append({
                "table": "retention",
                "metric_date": None, "metric_name": None,
                "metric_value": None, "dimensions": None,
                "computed_at": r["computed_at"],
                "movie_id": None, "rank": None, "views": None,
                "cohort_date": r["cohort_date"],
                "day_offset": r["day_offset"],
                "cohort_size": r["cohort_size"],
                "returned": r["returned"],
                "retention_pct": r["retention_pct"],
            })
        df = pd.DataFrame(rows)
        df.to_parquet(buf, index=False, engine="pyarrow")
        content_type = "application/octet-stream"

    buf.seek(0)

    # 3. Выгружаем в S3
    key = f"daily/{target_date.isoformat()}/aggregates.{fmt}"
    client = s3_client()
    client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buf.getvalue(),
        ContentType=content_type,
    )

    log.info("=== export done key=s3://%s/%s size=%d bytes records=%d ===",
             S3_BUCKET, key, buf.getbuffer().nbytes, total)

    return {
        "status": "ok",
        "bucket": S3_BUCKET,
        "key": key,
        "records": total,
        "size_bytes": buf.getbuffer().nbytes,
        "format": fmt,
    }


# ---------- Lifespan ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler
    scheduler = BackgroundScheduler(timezone="UTC")
    trigger = CronTrigger.from_crontab(SCHEDULE_CRON)
    scheduler.add_job(
        lambda: export_date((datetime.now(timezone.utc) - timedelta(days=1)).date()),
        trigger=trigger,
        id="export_daily",
        misfire_grace_time=3600,
    )
    scheduler.start()
    log.info("export scheduler started, cron='%s'", SCHEDULE_CRON)
    yield
    if scheduler:
        scheduler.shutdown(wait=False)


app = FastAPI(title="Export Service", lifespan=lifespan)


# ---------- Endpoints ----------
@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/run")
def run(target_date: Optional[str] = Query(None, alias="date"),
        fmt: Optional[str] = Query(None, alias="format")):
    """Ручной экспорт за дату."""
    try:
        d = (datetime.strptime(target_date, "%Y-%m-%d").date()
             if target_date else
             (datetime.now(timezone.utc) - timedelta(days=1)).date())
    except ValueError:
        raise HTTPException(400, "date must be YYYY-MM-DD")
    try:
        return export_date(d, fmt)
    except Exception as e:
        log.exception("export failed")
        raise HTTPException(500, f"export failed: {e}")
