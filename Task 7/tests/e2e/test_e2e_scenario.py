"""
E2E test: full warehouse lifecycle scenario.

Verifies the complete user journey:
  PRODUCT_RECEIVED -> PRODUCT_RESERVED -> ORDER_CREATED -> ORDER_COMPLETED
  with state verification in Cassandra after each step.

Requires docker-compose to be running.
"""
import os
import time
import json
import uuid
import pytest
import requests

WMS_URL = os.getenv("WMS_URL", "http://localhost:8000")
CONSUMER_URL = os.getenv("CONSUMER_URL", "http://localhost:8001")
WAIT_SECONDS = int(os.getenv("WAIT_SECONDS", "20"))


def poll_inventory(product_id, zone_id, check_fn, timeout=WAIT_SECONDS):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{CONSUMER_URL}/inventory/{product_id}/{zone_id}", timeout=5)
            if r.status_code == 200 and check_fn(r.json()):
                return r.json()
        except requests.ConnectionError:
            pass
        time.sleep(1)
    r = requests.get(f"{CONSUMER_URL}/inventory/{product_id}/{zone_id}", timeout=5)
    return r.json() if r.status_code == 200 else None


def poll_order(order_id, expected_status, timeout=WAIT_SECONDS):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{CONSUMER_URL}/order/{order_id}", timeout=5)
            if r.status_code == 200 and r.json().get("status") == expected_status:
                return r.json()
        except requests.ConnectionError:
            pass
        time.sleep(1)
    return None


def send_event(event_data):
    r = requests.post(f"{WMS_URL}/events", json=event_data, timeout=10)
    assert r.status_code == 200, f"Failed to send event: {r.text}"
    return r.json()


class TestWarehouseE2EScenario:
    """
    Complete warehouse lifecycle:
    1. Receive 200 units of product into ZONE-A
    2. Reserve 50 units
    3. Create an order with 30 units
    4. Complete the order
    5. Verify final inventory state
    """

    def test_full_warehouse_lifecycle(self):
        uid = uuid.uuid4().hex[:8]
        product_id = f"E2E-SKU-{uid}"
        zone_id = "ZONE-E2E"
        ts = int(time.time() * 1000)
        order_id = f"E2E-ORD-{uid}"

        # Step 1: Receive product
        send_event({
            "event_id": f"e2e-recv-{uid}",
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": ts,
            "product_id": product_id,
            "quantity": 200,
            "zone_id": zone_id,
        })

        inv = poll_inventory(product_id, zone_id, lambda d: d["available"] == 200)
        assert inv is not None, "Step 1 failed: PRODUCT_RECEIVED not processed"
        assert inv["available"] == 200
        assert inv["reserved"] == 0

        # Step 2: Reserve some stock
        send_event({
            "event_id": f"e2e-res-{uid}",
            "event_type": "PRODUCT_RESERVED",
            "event_timestamp": ts + 1000,
            "product_id": product_id,
            "quantity": 50,
            "zone_id": zone_id,
        })

        inv = poll_inventory(product_id, zone_id, lambda d: d["reserved"] == 50)
        assert inv is not None, "Step 2 failed: PRODUCT_RESERVED not processed"
        assert inv["available"] == 150
        assert inv["reserved"] == 50

        # Step 3: Create order (reserves additional 30 units)
        order_items = json.dumps([{
            "product_id": product_id,
            "zone_id": zone_id,
            "quantity": 30,
        }])
        send_event({
            "event_id": f"e2e-order-{uid}",
            "event_type": "ORDER_CREATED",
            "event_timestamp": ts + 2000,
            "order_id": order_id,
            "order_items": order_items,
        })

        order = poll_order(order_id, "CREATED")
        assert order is not None, "Step 3 failed: ORDER_CREATED not processed"
        assert order["status"] == "CREATED"

        inv = poll_inventory(product_id, zone_id, lambda d: d["reserved"] == 80)
        assert inv is not None, "Step 3 failed: reservation not reflected"
        assert inv["available"] == 120
        assert inv["reserved"] == 80

        # Step 4: Complete order (releases reserved stock)
        send_event({
            "event_id": f"e2e-complete-{uid}",
            "event_type": "ORDER_COMPLETED",
            "event_timestamp": ts + 3000,
            "order_id": order_id,
        })

        order = poll_order(order_id, "COMPLETED")
        assert order is not None, "Step 4 failed: ORDER_COMPLETED not processed"
        assert order["status"] == "COMPLETED"

        inv = poll_inventory(product_id, zone_id, lambda d: d["reserved"] == 50)
        assert inv is not None, "Step 4 failed: reservation release not reflected"
        assert inv["available"] == 120
        assert inv["reserved"] == 50

        # Step 5: Verify product totals
        r = requests.get(f"{CONSUMER_URL}/inventory/{product_id}", timeout=5)
        assert r.status_code == 200
        totals = r.json()
        assert totals["total_available"] == 120
        assert totals["total_reserved"] == 50

    def test_http_status_codes(self):
        """Verify HTTP status codes for various responses."""
        r = requests.get(f"{WMS_URL}/health", timeout=5)
        assert r.status_code == 200

        r = requests.get(f"{CONSUMER_URL}/health", timeout=5)
        assert r.status_code == 200

        r = requests.get(f"{CONSUMER_URL}/order/nonexistent-order", timeout=5)
        assert r.status_code == 404

    def test_response_body_structure(self):
        """Verify response body fields and types."""
        uid = uuid.uuid4().hex[:8]
        r = requests.post(f"{WMS_URL}/events", json={
            "event_id": f"struct-{uid}",
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": int(time.time() * 1000),
            "product_id": f"STRUCT-{uid}",
            "quantity": 10,
            "zone_id": "ZONE-STRUCT",
        }, timeout=10)
        data = r.json()
        assert "status" in data
        assert "event_id" in data
        assert "version" in data
        assert isinstance(data["status"], str)
        assert isinstance(data["event_id"], str)
