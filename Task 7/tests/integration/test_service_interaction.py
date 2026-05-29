"""
Integration tests: verify interaction between WMS and Consumer services.

Requires docker-compose to be running.
Services communicate via Kafka; state is verified in Cassandra through the Consumer API.
"""
import os
import time
import uuid
import pytest
import requests

WMS_URL = os.getenv("WMS_URL", "http://localhost:8000")
CONSUMER_URL = os.getenv("CONSUMER_URL", "http://localhost:8001")
WAIT_SECONDS = int(os.getenv("WAIT_SECONDS", "30"))


def wait_for_processing(product_id, zone_id, expected_available, timeout=WAIT_SECONDS):
    """Poll consumer service until inventory matches expected value or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{CONSUMER_URL}/inventory/{product_id}/{zone_id}", timeout=5)
            if r.status_code == 200:
                data = r.json()
                if data.get("available") == expected_available:
                    return data
        except requests.ConnectionError:
            pass
        time.sleep(1)
    r = requests.get(f"{CONSUMER_URL}/inventory/{product_id}/{zone_id}", timeout=5)
    return r.json() if r.status_code == 200 else None


class TestServiceInteraction:
    """Tests that verify WMS -> Kafka -> Consumer -> Cassandra pipeline."""

    def test_services_healthy(self):
        r1 = requests.get(f"{WMS_URL}/health", timeout=5)
        assert r1.status_code == 200
        assert r1.json()["status"] == "ok"

        r2 = requests.get(f"{CONSUMER_URL}/health", timeout=5)
        assert r2.status_code == 200
        assert r2.json()["status"] == "ok"

    def test_event_flows_through_pipeline(self):
        """Send PRODUCT_RECEIVED via WMS, verify it arrives in Cassandra via Consumer API."""
        uid = uuid.uuid4().hex[:8]
        product_id = f"INT-SKU-{uid}"
        zone_id = "ZONE-INT"
        quantity = 42

        r = requests.post(f"{WMS_URL}/events", json={
            "event_id": f"int-{uid}",
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": int(time.time() * 1000),
            "product_id": product_id,
            "quantity": quantity,
            "zone_id": zone_id,
        }, timeout=10)
        assert r.status_code == 200
        assert r.json()["status"] == "sent"

        inv = wait_for_processing(product_id, zone_id, quantity)
        assert inv is not None, "Consumer did not process event in time"
        assert inv["available"] == quantity

    def test_receive_then_ship(self):
        """Send PRODUCT_RECEIVED then PRODUCT_SHIPPED, verify final stock."""
        uid = uuid.uuid4().hex[:8]
        product_id = f"INT-SKU-{uid}"
        zone_id = "ZONE-INT2"
        ts = int(time.time() * 1000)

        requests.post(f"{WMS_URL}/events", json={
            "event_id": f"recv-{uid}",
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": ts,
            "product_id": product_id,
            "quantity": 100,
            "zone_id": zone_id,
        }, timeout=10)

        wait_for_processing(product_id, zone_id, 100)

        requests.post(f"{WMS_URL}/events", json={
            "event_id": f"ship-{uid}",
            "event_type": "PRODUCT_SHIPPED",
            "event_timestamp": ts + 1000,
            "product_id": product_id,
            "quantity": 25,
            "zone_id": zone_id,
        }, timeout=10)

        inv = wait_for_processing(product_id, zone_id, 75)
        assert inv is not None, "Consumer did not process ship event in time"
        assert inv["available"] == 75

    def test_idempotency(self):
        """Sending the same event twice should not double-count."""
        uid = uuid.uuid4().hex[:8]
        product_id = f"INT-IDEM-{uid}"
        zone_id = "ZONE-IDEM"
        event_id = f"idem-{uid}"
        ts = int(time.time() * 1000)

        payload = {
            "event_id": event_id,
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": ts,
            "product_id": product_id,
            "quantity": 50,
            "zone_id": zone_id,
        }

        requests.post(f"{WMS_URL}/events", json=payload, timeout=10)
        wait_for_processing(product_id, zone_id, 50)

        requests.post(f"{WMS_URL}/events", json=payload, timeout=10)
        time.sleep(5)

        r = requests.get(f"{CONSUMER_URL}/inventory/{product_id}/{zone_id}", timeout=5)
        assert r.json()["available"] == 50

    def test_metrics_endpoints_available(self):
        """Both services expose /metrics with Prometheus data."""
        for url in [WMS_URL, CONSUMER_URL]:
            r = requests.get(f"{url}/metrics", timeout=5)
            assert r.status_code == 200
            assert "http_requests_total" in r.text
