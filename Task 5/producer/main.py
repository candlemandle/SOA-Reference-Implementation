"""
Движок продюсера: HTTP API + генератор синтетики.

HTTP API принимает JSON-события, валидирует их по Avro-схеме
(через Schema Registry) и шлёт в Kafka.

Генератор работает в фоне и эмулирует реалистичное поведение
пользователей: сессия начинается с VIEW_STARTED, дальше идут
VIEW_PAUSED/VIEW_RESUMED и завершается VIEW_FINISHED, progress_seconds
монотонно растёт.
"""
import asyncio
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional, Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

from kafka_producer import AvroEventProducer
from generator import EventGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
log = logging.getLogger("producer")

# ---------- ENV ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29093")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "movie-events")
GENERATOR_ENABLED = os.getenv("GENERATOR_ENABLED", "true").lower() == "true"
GENERATOR_EPS = int(os.getenv("GENERATOR_EPS", "5"))

# Одиночный инстанс продюсера на всё приложение
producer: Optional[AvroEventProducer] = None
generator_task: Optional[asyncio.Task] = None


# ---------- Pydantic модель для валидации входящих JSON ----------
class EventIn(BaseModel):
    """Входящее событие в JSON. Валидируем типы до отправки в Kafka."""
    event_id: Optional[str] = None  # если не указан — сгенерим
    user_id: str = Field(..., min_length=1)
    movie_id: str = Field(..., min_length=1)
    event_type: Literal[
        "VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED",
        "VIEW_RESUMED", "LIKED", "SEARCHED",
    ]
    timestamp: Optional[int] = None  # миллисекунды
    device_type: Literal["MOBILE", "DESKTOP", "TV", "TABLET"]
    session_id: str = Field(..., min_length=1)
    progress_seconds: Optional[int] = Field(None, ge=0)

    @field_validator("event_id", mode="before")
    @classmethod
    def _check_uuid(cls, v):
        if v is None:
            return None
        try:
            uuid.UUID(v)
        except (ValueError, AttributeError, TypeError) as e:
            raise ValueError(f"event_id must be valid UUID, got {v!r}") from e
        return v


# ---------- Lifespan (инициализация/завершение) ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, generator_task
    log.info("Starting producer, bootstrap=%s", KAFKA_BOOTSTRAP)
    producer = AvroEventProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        topic=KAFKA_TOPIC,
        schema_path="schemas/movie_event.avsc",
    )

    if GENERATOR_ENABLED:
        gen = EventGenerator(producer=producer, eps=GENERATOR_EPS)
        generator_task = asyncio.create_task(gen.run())
        log.info("Background generator started (EPS=%s)", GENERATOR_EPS)

    yield

    log.info("Shutting down producer")
    if generator_task:
        generator_task.cancel()
        try:
            await generator_task
        except asyncio.CancelledError:
            pass
    if producer:
        producer.flush()


app = FastAPI(title="Movie Events Producer", lifespan=lifespan)


# ---------- HTTP endpoints ----------
@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/events")
def publish_event(ev: EventIn):
    """Принимает событие, валидирует, шлёт в Kafka.
    Возвращает event_id при успехе или 400/500 при ошибке.
    """
    # заполняем автогенерируемые поля
    event_id = ev.event_id or str(uuid.uuid4())
    ts = ev.timestamp or int(datetime.now(timezone.utc).timestamp() * 1000)

    payload = {
        "event_id": event_id,
        "user_id": ev.user_id,
        "movie_id": ev.movie_id,
        "event_type": ev.event_type,
        "timestamp": ts,
        "device_type": ev.device_type,
        "session_id": ev.session_id,
        "progress_seconds": ev.progress_seconds,
    }

    try:
        producer.produce_event(payload)
    except ValueError as e:
        # ошибка валидации по Avro-схеме
        log.warning("validation failed: %s", e)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log.exception("publish failed")
        raise HTTPException(status_code=500, detail=f"publish failed: {e}")

    return {"event_id": event_id, "status": "accepted"}


@app.get("/stats")
def stats():
    """Вернёт счётчик опубликованных событий."""
    return producer.stats() if producer else {}
