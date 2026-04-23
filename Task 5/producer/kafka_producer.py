"""
Обёртка над confluent_kafka.Producer с Avro-сериализацией
через Schema Registry.

Ключевые фичи:
  - Avro-валидация до отправки (fastavro)
  - Регистрация схемы в Schema Registry при старте
  - Ключ партиционирования = user_id
    (чтобы события одного пользователя шли в одну партицию и сохранялся порядок)
  - acks=all + retries + exponential backoff
  - Логирование каждой публикации (event_id, type, ts)
"""
import json
import logging
import time
from pathlib import Path
from threading import Lock
from typing import Any, Dict

import fastavro
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext, MessageField, StringSerializer,
)

log = logging.getLogger("producer.kafka")


class AvroEventProducer:
    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str,
        schema_path: str,
    ):
        self.topic = topic
        self.schema_path = schema_path

        # Загружаем схему из файла
        with open(schema_path, "r", encoding="utf-8") as f:
            self.schema_str = f.read()
        self.parsed_schema = fastavro.parse_schema(json.loads(self.schema_str))

        # Schema Registry + сериализатор
        self._sr_client = SchemaRegistryClient({"url": schema_registry_url})
        self._avro_serializer = AvroSerializer(
            schema_registry_client=self._sr_client,
            schema_str=self.schema_str,
            conf={"auto.register.schemas": True},
        )
        self._key_serializer = StringSerializer("utf_8")

        # Продюсер Kafka. acks=all — запись подтверждается всеми in-sync
        # репликами. retries+backoff обеспечивают устойчивость к сбоям.
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,   # exactly-once семантика на продюсере
            "retries": 10,
            "retry.backoff.ms": 500,      # начальный бэкофф
            "max.in.flight.requests.per.connection": 5,
            "compression.type": "snappy",
            "linger.ms": 20,
            "client.id": "movie-producer",
        })

        self._counter = 0
        self._lock = Lock()

    # -------- публикация --------
    def produce_event(self, event: Dict[str, Any]) -> None:
        """Сериализует событие в Avro и шлёт в Kafka.
        Raises ValueError если схема не прошла.
        """
        # 1. Валидация по Avro перед отправкой
        try:
            fastavro.validate(event, self.parsed_schema, raise_errors=True)
        except fastavro.validation.ValidationError as e:
            raise ValueError(f"Avro validation failed: {e}") from e

        # 2. Сериализация через Schema Registry
        ctx = SerializationContext(self.topic, MessageField.VALUE)
        value_bytes = self._avro_serializer(event, ctx)
        # ключ — user_id: гарантирует порядок внутри одного пользователя
        key_bytes = self._key_serializer(event["user_id"])

        # 3. Отправка с callback
        self._producer.produce(
            topic=self.topic,
            key=key_bytes,
            value=value_bytes,
            on_delivery=self._on_delivery,
            headers={
                "event_type": event["event_type"].encode("utf-8"),
                "event_id": event["event_id"].encode("utf-8"),
            },
        )
        # poll освобождает внутренние колбэки; без него буфер может
        # заполниться при большом EPS
        self._producer.poll(0)

        with self._lock:
            self._counter += 1

        # лог факта публикации
        log.info(
            "produced event_id=%s type=%s ts=%s user=%s",
            event["event_id"], event["event_type"],
            event["timestamp"], event["user_id"],
        )

    def _on_delivery(self, err, msg):
        if err is not None:
            log.error("delivery failed: %s", err)
        else:
            log.debug(
                "delivered to %s[%d]@%d",
                msg.topic(), msg.partition(), msg.offset(),
            )

    def flush(self, timeout: float = 10.0):
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            log.warning("flush timeout, %d messages still in queue", remaining)

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {"produced_total": self._counter}
