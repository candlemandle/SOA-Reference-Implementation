import os
import asyncio
import logging
import json
import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response, JSONResponse
from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from cassandra_client import CassandraClient
from handlers import EventHandler
from metrics import (
    PrometheusMiddleware,
    events_processed_total,
    event_processing_duration_seconds,
    cassandra_write_errors_total,
    consumer_lag,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC = "warehouse-events"
DLQ_TOPIC = "warehouse-events-dlq"
GROUP_ID = "warehouse-state-consumer"
CASSANDRA_CONTACT_POINTS = os.getenv("CASSANDRA_CONTACT_POINTS", "cassandra").split(",")

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
avro_deserializer = AvroDeserializer(schema_registry_client)

cassandra_client = None
event_handler = None
consumer = None
producer = None


def delivery_report(err, msg):
    if err:
        logger.error(f"DLQ delivery failed: {err}")


def send_to_dlq(original_event, error_reason, partition, offset):
    global producer
    if producer is None:
        producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    dlq_msg = {
        "original_event": original_event,
        "error_reason": error_reason,
        "error_code": "PROCESSING_ERROR",
        "failed_at": datetime.datetime.utcnow().isoformat() + "Z",
        "kafka_metadata": {"partition": partition, "offset": offset},
    }
    producer.produce(DLQ_TOPIC, value=json.dumps(dlq_msg).encode("utf-8"), callback=delivery_report)
    producer.poll(0)


def process_message(msg):
    try:
        event = avro_deserializer(msg.value(), None)
    except Exception as e:
        logger.exception("Deserialization error, sending to DLQ")
        send_to_dlq({"raw": msg.value().decode(errors="ignore")}, str(e), msg.partition(), msg.offset())
        consumer.commit(message=msg)
        return

    event_id = event.get("event_id")
    event_type = event.get("event_type")
    logger.info(f"Processing {event_type} id={event_id} partition={msg.partition()} offset={msg.offset()}")

    if cassandra_client.is_processed(event_id):
        logger.info(f"Duplicate event {event_id}, skipping")
        consumer.commit(message=msg)
        return

    with event_processing_duration_seconds.labels(event_type=event_type).time():
        try:
            event_handler.handle(event)
            events_processed_total.labels(event_type=event_type).inc()
            consumer.commit(message=msg)
        except Exception:
            logger.exception(f"Error handling event {event_id}")
            cassandra_write_errors_total.inc()
            send_to_dlq(event, str(Exception), msg.partition(), msg.offset())
            consumer.commit(message=msg)


async def update_consumer_lag():
    while True:
        if consumer is None:
            await asyncio.sleep(5)
            continue
        try:
            assignment = consumer.assignment()
            if not assignment:
                await asyncio.sleep(5)
                continue
            for tp in assignment:
                low, high = consumer.get_watermark_offsets(tp)
                committed = consumer.committed([tp])[0].offset
                lag_val = high - committed if committed >= 0 else 0
                consumer_lag.labels(partition=tp.partition).set(lag_val)
        except Exception as e:
            logger.warning(f"Failed to update consumer lag: {e}")
        if cassandra_client:
            cassandra_client.update_host_metrics()
        await asyncio.sleep(15)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global cassandra_client, event_handler, consumer
    cassandra_client = CassandraClient(hosts=CASSANDRA_CONTACT_POINTS, keyspace="warehouse")
    cassandra_client.connect()
    event_handler = EventHandler(cassandra_client)

    logger.info("Waiting for Cassandra availability...")
    while True:
        try:
            cassandra_client.session.execute("SELECT event_id FROM processed_events LIMIT 1")
            logger.info("Cassandra is available, starting consumer.")
            break
        except Exception as exc:
            logger.warning(f"Cassandra not ready ({exc}), retrying in 5s...")
            await asyncio.sleep(5)

    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC])
    lag_task = asyncio.create_task(update_consumer_lag())

    loop = asyncio.get_event_loop()

    async def consume_loop():
        while True:
            msg = await loop.run_in_executor(None, consumer.poll, 0.5)
            if msg is None:
                await asyncio.sleep(0.05)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    await asyncio.sleep(0.05)
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                await asyncio.sleep(1)
                continue
            try:
                await loop.run_in_executor(None, process_message, msg)
            except Exception as exc:
                logger.exception(f"Unhandled error: {exc}")
            await asyncio.sleep(0)

    task = asyncio.create_task(consume_loop())
    yield
    task.cancel()
    lag_task.cancel()
    consumer.close()
    cassandra_client.close()


app = FastAPI(lifespan=lifespan)
app.add_middleware(PrometheusMiddleware)


@app.get("/health")
async def health():
    if cassandra_client is None or consumer is None:
        return JSONResponse(status_code=503, content={"status": "unavailable"})
    try:
        cassandra_client.session.execute("SELECT now() FROM system.local")
    except Exception:
        return JSONResponse(status_code=503, content={"status": "cassandra unreachable"})
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/inventory/{product_id}/{zone_id}")
async def get_inventory(product_id: str, zone_id: str):
    if cassandra_client is None:
        raise HTTPException(status_code=503, detail="Not ready")
    inv = cassandra_client.get_zone_inventory(product_id, zone_id)
    return {"product_id": product_id, "zone_id": zone_id, **inv}


@app.get("/inventory/{product_id}")
async def get_product_inventory(product_id: str):
    if cassandra_client is None:
        raise HTTPException(status_code=503, detail="Not ready")
    inv = cassandra_client.get_product_inventory(product_id)
    return {"product_id": product_id, **inv}


@app.get("/order/{order_id}")
async def get_order(order_id: str):
    if cassandra_client is None:
        raise HTTPException(status_code=503, detail="Not ready")
    order = cassandra_client.get_order(order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return {"order_id": order.order_id, "status": order.status, "items": order.items}
