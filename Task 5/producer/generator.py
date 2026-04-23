"""
Генератор реалистичных сессий.

Моделирует пользовательскую сессию просмотра:
  1. VIEW_STARTED
  2. несколько VIEW_PAUSED / VIEW_RESUMED
  3. либо VIEW_FINISHED (досмотрел), либо сессия обрывается (не досмотрел)
  4. в любой момент могут быть LIKED и SEARCHED (тоже пишутся в топик)

progress_seconds монотонно растёт в рамках одной сессии.
"""
import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone
from typing import List

from kafka_producer import AvroEventProducer

log = logging.getLogger("producer.generator")

USERS = [f"user_{i:04d}" for i in range(1, 201)]          # 200 пользователей
MOVIES = [f"movie_{i:03d}" for i in range(1, 51)]         # 50 фильмов
DEVICES = ["MOBILE", "DESKTOP", "TV", "TABLET"]


class EventGenerator:
    def __init__(self, producer: AvroEventProducer, eps: int = 5):
        self.producer = producer
        self.eps = max(1, eps)
        # Список активных сессий: {session_id, user_id, movie_id,
        # device_type, progress, started_at}
        self.active: List[dict] = []

    def _now_ms(self) -> int:
        return int(datetime.now(timezone.utc).timestamp() * 1000)

    def _emit(self, session: dict, event_type: str,
              progress: int | None = None):
        payload = {
            "event_id": str(uuid.uuid4()),
            "user_id": session["user_id"],
            "movie_id": session["movie_id"],
            "event_type": event_type,
            "timestamp": self._now_ms(),
            "device_type": session["device_type"],
            "session_id": session["session_id"],
            "progress_seconds": progress,
        }
        try:
            self.producer.produce_event(payload)
        except Exception as e:
            # не валим генератор целиком из-за одной ошибки
            log.error("generator publish failed: %s", e)

    def _start_session(self):
        user = random.choice(USERS)
        movie = random.choice(MOVIES)
        session = {
            "session_id": str(uuid.uuid4()),
            "user_id": user,
            "movie_id": movie,
            "device_type": random.choice(DEVICES),
            "progress": 0,
        }
        self._emit(session, "VIEW_STARTED", progress=0)
        self.active.append(session)

    def _advance_session(self, session: dict):
        """Двигает сессию вперёд: пауза/резюм/досмотр/лайк."""
        # Прогресс растёт на 10-30 секунд за шаг
        session["progress"] += random.randint(10, 30)
        roll = random.random()

        if roll < 0.15:
            self._emit(session, "VIEW_PAUSED", progress=session["progress"])
        elif roll < 0.3:
            self._emit(session, "VIEW_RESUMED", progress=session["progress"])
        elif roll < 0.4:
            self._emit(session, "LIKED", progress=None)
        elif roll < 0.55 and session["progress"] > 60:
            # ~45% шанс досмотра (если прошло больше минуты)
            self._emit(session, "VIEW_FINISHED", progress=session["progress"])
            self.active.remove(session)
        elif roll < 0.6:
            # изредка обрыв сессии без finish
            self.active.remove(session)

    def _emit_search(self):
        user = random.choice(USERS)
        session = {
            "session_id": str(uuid.uuid4()),
            "user_id": user,
            "movie_id": random.choice(MOVIES),
            "device_type": random.choice(DEVICES),
        }
        self._emit(session, "SEARCHED", progress=None)

    async def run(self):
        log.info("generator loop started, EPS=%d", self.eps)
        interval = 1.0 / self.eps
        while True:
            try:
                # с шансом 30% начинаем новую сессию или поиск
                roll = random.random()
                if not self.active or roll < 0.2:
                    self._start_session()
                elif roll < 0.25:
                    self._emit_search()
                else:
                    # двигаем случайную активную сессию
                    session = random.choice(self.active)
                    self._advance_session(session)

                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                log.info("generator cancelled")
                break
            except Exception:
                log.exception("generator loop error")
                await asyncio.sleep(1)
