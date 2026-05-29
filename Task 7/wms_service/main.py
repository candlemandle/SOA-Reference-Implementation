import os
import json
import logging
import time
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
import requests as http_requests

from models import EventV1, EventV2
from metrics import PrometheusMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="WMS Service")
app.add_middleware(PrometheusMiddleware)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC = "warehouse-events"


def load_schema(filename: str) -> str:
    with open(f"/app/schemas/{filename}", "r") as f:
        return f.read()


SCHEMA_V1 = load_schema("warehouse_event_v1.avsc")
SCHEMA_V2 = load_schema("warehouse_event_v2.avsc")

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


def register_schema_with_backward_compatibility(subject: str, schema_str: str):
    compat_url = f"{SCHEMA_REGISTRY_URL}/config/{subject}"
    http_requests.put(compat_url, json={"compatibility": "BACKWARD"}, timeout=10)
    schema_id = schema_registry_client.register_schema(subject, Schema(schema_str, "AVRO"))
    logger.info(f"Registered {subject} schema id={schema_id}")
    return schema_id


subject = f"{TOPIC}-value"
try:
    register_schema_with_backward_compatibility(subject, SCHEMA_V1)
    register_schema_with_backward_compatibility(subject, SCHEMA_V2)
except Exception as e:
    logger.warning(f"Schema registration skipped (already registered?): {e}")

avro_serializer_v1 = AvroSerializer(schema_registry_client, SCHEMA_V1)
avro_serializer_v2 = AvroSerializer(schema_registry_client, SCHEMA_V2)

producer_conf = {"bootstrap.servers": KAFKA_BOOTSTRAP}
producer = Producer(producer_conf)


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


def _produce(data: dict, serializer: AvroSerializer):
    serialized = serializer(data, SerializationContext(TOPIC, MessageField.VALUE))
    key = data.get("product_id") or data.get("order_id") or data.get("event_id")
    producer.produce(
        TOPIC,
        key=key.encode("utf-8") if key else None,
        value=serialized,
        callback=delivery_report,
    )
    producer.flush()


@app.post("/events")
async def send_event_v1(event: EventV1):
    try:
        _produce(event.dict(), avro_serializer_v1)
        return {"status": "sent", "event_id": event.event_id, "version": "v1"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/events/v2")
async def send_event_v2(event: EventV2):
    try:
        _produce(event.dict(), avro_serializer_v2)
        return {"status": "sent", "event_id": event.event_id, "version": "v2"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _send(event_dict: dict, version: str = "v1"):
    serializer = avro_serializer_v2 if version == "v2" else avro_serializer_v1
    _produce(event_dict, serializer)


@app.post("/scenario/{name}")
async def run_scenario(name: str):
    base_ts = int(time.time() * 1000)

    if name == "basic-cycle":
        _send({"event_id": "recv-1", "event_type": "PRODUCT_RECEIVED",
               "event_timestamp": base_ts, "product_id": "SKU-001",
               "quantity": 100, "zone_id": "ZONE-A"})
        _send({"event_id": "res-1", "event_type": "PRODUCT_RESERVED",
               "event_timestamp": base_ts + 300_000, "product_id": "SKU-001",
               "quantity": 30, "zone_id": "ZONE-A"})
        _send({"event_id": "move-1", "event_type": "PRODUCT_MOVED",
               "event_timestamp": base_ts + 600_000, "product_id": "SKU-001",
               "quantity": 20, "from_zone_id": "ZONE-A", "to_zone_id": "ZONE-B"})
        _send({"event_id": "ship-1", "event_type": "PRODUCT_SHIPPED",
               "event_timestamp": base_ts + 900_000, "product_id": "SKU-001",
               "quantity": 10, "zone_id": "ZONE-A"})
        order_items = json.dumps([{"product_id": "SKU-001", "zone_id": "ZONE-A", "quantity": 15}])
        _send({"event_id": "order-1", "event_type": "ORDER_CREATED",
               "event_timestamp": base_ts + 1_200_000,
               "order_id": "ORD-001", "order_items": order_items})
        _send({"event_id": "order-complete-1", "event_type": "ORDER_COMPLETED",
               "event_timestamp": base_ts + 1_500_000, "order_id": "ORD-001"})
        return {"scenario": "basic-cycle", "status": "executed"}

    elif name == "idempotency":
        _send({"event_id": "dup-1", "event_type": "PRODUCT_RECEIVED",
               "event_timestamp": base_ts, "product_id": "SKU-002",
               "quantity": 50, "zone_id": "ZONE-A"})
        _send({"event_id": "dup-1", "event_type": "PRODUCT_RECEIVED",
               "event_timestamp": base_ts, "product_id": "SKU-002",
               "quantity": 50, "zone_id": "ZONE-A"})
        return {"scenario": "idempotency", "status": "executed"}

    elif name == "schema-evolution":
        _send({"event_id": "v1-recv-1", "event_type": "PRODUCT_RECEIVED",
               "event_timestamp": base_ts, "product_id": "SKU-007",
               "quantity": 60, "zone_id": "ZONE-C"}, version="v1")
        _send({"event_id": "v2-recv-1", "event_type": "PRODUCT_RECEIVED",
               "event_timestamp": base_ts + 300_000, "product_id": "SKU-008",
               "quantity": 40, "zone_id": "ZONE-C", "supplier_id": "SUP-001"}, version="v2")
        return {"scenario": "schema-evolution", "status": "executed"}

    else:
        raise HTTPException(status_code=404, detail=f"Unknown scenario: {name}")
