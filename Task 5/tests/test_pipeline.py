"""
Интеграционный тест пайплайна.

Что проверяем:
  1. Событие, отправленное в HTTP API продюсера,
  2. проходит через Kafka,
  3. попадает в ClickHouse (movie_events_raw),
  4. с корректными значениями всех полей.

Тест идемпотентный:
  - каждый раз генерирует свежий event_id/session_id (изоляция),
  - не требует очистки данных.

Запуск (из корня проекта):
    docker compose up -d
    docker compose run --rm tests
"""
import os
import time
import uuid
from datetime import datetime, timezone

import clickhouse_connect
import pytest
import requests

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://producer:8000")
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_DB   = os.getenv("CLICKHOUSE_DB", "analytics")

MAX_WAIT_SEC = 60


@pytest.fixture(scope="session")
def ch_client():
    # Ждём, пока ClickHouse ответит
    deadline = time.time() + 30
    last_err = None
    while time.time() < deadline:
        try:
            c = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT,
                                              database=CH_DB)
            c.query("SELECT 1")
            yield c
            try:
                c.close()
            except Exception:
                pass
            return
        except Exception as e:
            last_err = e
            time.sleep(1)
    pytest.fail(f"ClickHouse not ready: {last_err}")


@pytest.fixture(scope="session", autouse=True)
def producer_ready():
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            r = requests.get(f"{PRODUCER_URL}/health", timeout=2)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(1)
    pytest.fail("Producer not ready")


def _wait_for_event(ch_client, event_id: str, timeout: int = MAX_WAIT_SEC):
    """Поллим ClickHouse пока не увидим событие с таким event_id."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        # ВАЖНО: при использовании Kafka Engine данные появляются только
        # после того, как consumer обработает batch. Настроен kafka_max_block_size,
        # поэтому иногда нужно подождать
        rows = ch_client.query(
            "SELECT event_id, user_id, movie_id, event_type, device_type, "
            "session_id, progress_seconds "
            "FROM movie_events_raw WHERE event_id = toUUID(%(id)s)",
            parameters={"id": event_id},
        ).result_rows
        if rows:
            return rows[0]
        time.sleep(2)
    return None


# ------------------------------------------------------------
# Тест 1: одно событие проходит весь путь
# ------------------------------------------------------------
def test_single_event_end_to_end(ch_client):
    event_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    payload = {
        "event_id": event_id,
        "user_id": "test_user_001",
        "movie_id": "movie_test_001",
        "event_type": "VIEW_STARTED",
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "device_type": "DESKTOP",
        "session_id": session_id,
        "progress_seconds": 0,
    }

    # 1. Публикуем через HTTP
    r = requests.post(f"{PRODUCER_URL}/events", json=payload, timeout=5)
    assert r.status_code == 200, f"producer rejected event: {r.text}"
    body = r.json()
    assert body["event_id"] == event_id
    assert body["status"] == "accepted"

    # 2. Ждём появления в ClickHouse
    row = _wait_for_event(ch_client, event_id)
    assert row is not None, f"event {event_id} didn't arrive in ClickHouse within {MAX_WAIT_SEC}s"

    # 3. Проверяем поля
    ch_event_id, user_id, movie_id, event_type, device_type, sess_id, progress = row
    assert str(ch_event_id) == event_id
    assert user_id == "test_user_001"
    assert movie_id == "movie_test_001"
    assert event_type == "VIEW_STARTED"
    assert device_type == "DESKTOP"
    assert sess_id == session_id
    assert progress == 0


# ------------------------------------------------------------
# Тест 2: невалидное событие отклоняется продюсером
# ------------------------------------------------------------
def test_invalid_event_rejected():
    bad_payload = {
        "user_id": "test_user_bad",
        "movie_id": "movie_bad",
        "event_type": "WATCH_SOMETHING",  # нет в enum
        "device_type": "DESKTOP",
        "session_id": "sess-bad",
    }
    r = requests.post(f"{PRODUCER_URL}/events", json=bad_payload, timeout=5)
    assert r.status_code == 422, f"expected 422, got {r.status_code}: {r.text}"


# ------------------------------------------------------------
# Тест 3: последовательность событий сессии
# ------------------------------------------------------------
def test_session_sequence(ch_client):
    session_id = str(uuid.uuid4())
    user_id = f"test_user_seq_{uuid.uuid4().hex[:8]}"
    now = int(datetime.now(timezone.utc).timestamp() * 1000)

    events = [
        ("VIEW_STARTED",  0),
        ("VIEW_PAUSED",   15),
        ("VIEW_RESUMED",  15),
        ("VIEW_FINISHED", 90),
    ]

    event_ids = []
    for i, (etype, progress) in enumerate(events):
        eid = str(uuid.uuid4())
        event_ids.append(eid)
        r = requests.post(f"{PRODUCER_URL}/events", json={
            "event_id": eid,
            "user_id": user_id,
            "movie_id": "movie_seq_001",
            "event_type": etype,
            "timestamp": now + i * 1000,
            "device_type": "MOBILE",
            "session_id": session_id,
            "progress_seconds": progress,
        }, timeout=5)
        assert r.status_code == 200, f"{etype}: {r.text}"

    # Все события должны появиться в ClickHouse
    for eid in event_ids:
        row = _wait_for_event(ch_client, eid)
        assert row is not None, f"event {eid} missing"

    # Проверим что видны все 4 события для этой сессии
    count = ch_client.query(
        "SELECT count() FROM movie_events_raw WHERE session_id = %(s)s",
        parameters={"s": session_id},
    ).result_rows[0][0]
    assert count == 4
