"""Unit tests for consumer EventHandler with mocked CassandraClient."""
import sys
import os
import pytest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'consumer_service'))

with patch.dict('sys.modules', {
    'metrics': MagicMock(),
}):
    from handlers import EventHandler


class FakeCassandraClient:
    """In-memory mock of CassandraClient for unit testing."""

    def __init__(self):
        self.inventory = {}
        self.product_totals = {}
        self.zone_inventory = {}
        self.processed = set()
        self.orders = {}

    def is_processed(self, event_id):
        return event_id in self.processed

    def get_zone_inventory(self, product_id, zone_id):
        return self.inventory.get((product_id, zone_id),
                                  {"available": 0, "reserved": 0, "last_ts": 0})

    def get_product_inventory(self, product_id):
        return self.product_totals.get(product_id,
                                        {"total_available": 0, "total_reserved": 0})

    def get_zone_product_inventory(self, zone_id, product_id):
        return self.zone_inventory.get((zone_id, product_id),
                                       {"available": 0, "reserved": 0})

    def get_order(self, order_id):
        return self.orders.get(order_id)

    def execute_inventory_update(self, event_id, event_type, product_id, zone_id,
                                  new_avail, new_reserved, event_ts,
                                  prod_total_avail, prod_total_reserved,
                                  zone_avail, zone_reserved, history_qty,
                                  supplier_id=None):
        self.inventory[(product_id, zone_id)] = {
            "available": new_avail, "reserved": new_reserved, "last_ts": event_ts,
        }
        self.product_totals[product_id] = {
            "total_available": prod_total_avail, "total_reserved": prod_total_reserved,
        }
        self.zone_inventory[(zone_id, product_id)] = {
            "available": zone_avail, "reserved": zone_reserved,
        }
        self.processed.add(event_id)

    def execute_move_update(self, event_id, event_type, product_id,
                            from_zone_id, to_zone_id,
                            from_avail, from_reserved, to_avail, to_reserved,
                            event_ts,
                            zone_from_avail, zone_from_reserved,
                            zone_to_avail, zone_to_reserved, quantity):
        self.inventory[(product_id, from_zone_id)] = {
            "available": from_avail, "reserved": from_reserved, "last_ts": event_ts,
        }
        self.inventory[(product_id, to_zone_id)] = {
            "available": to_avail, "reserved": to_reserved, "last_ts": event_ts,
        }
        self.processed.add(event_id)

    def mark_processed_only(self, event_id, event_type):
        self.processed.add(event_id)

    def create_order(self, order_id, items_json, event_id, event_type):
        self.orders[order_id] = MagicMock(order_id=order_id, status="CREATED", items=items_json)
        self.processed.add(event_id)

    def complete_order(self, order_id, event_id, event_type):
        if order_id in self.orders:
            self.orders[order_id].status = "COMPLETED"
        self.processed.add(event_id)


class TestProductReceived:
    def test_receive_increases_inventory(self):
        db = FakeCassandraClient()
        handler = EventHandler(db)
        handler.handle({
            "event_id": "r1", "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": 1000, "product_id": "SKU-1",
            "quantity": 100, "zone_id": "ZONE-A",
        })
        inv = db.get_zone_inventory("SKU-1", "ZONE-A")
        assert inv["available"] == 100
        assert inv["reserved"] == 0

    def test_receive_accumulates(self):
        db = FakeCassandraClient()
        handler = EventHandler(db)
        handler.handle({
            "event_id": "r1", "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": 1000, "product_id": "SKU-1",
            "quantity": 50, "zone_id": "ZONE-A",
        })
        handler.handle({
            "event_id": "r2", "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": 2000, "product_id": "SKU-1",
            "quantity": 30, "zone_id": "ZONE-A",
        })
        inv = db.get_zone_inventory("SKU-1", "ZONE-A")
        assert inv["available"] == 80


class TestProductShipped:
    def test_ship_decreases_inventory(self):
        db = FakeCassandraClient()
        handler = EventHandler(db)
        handler.handle({
            "event_id": "r1", "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": 1000, "product_id": "SKU-1",
            "quantity": 100, "zone_id": "ZONE-A",
        })
        handler.handle({
            "event_id": "s1", "event_type": "PRODUCT_SHIPPED",
            "event_timestamp": 2000, "product_id": "SKU-1",
            "quantity": 30, "zone_id": "ZONE-A",
        })
        inv = db.get_zone_inventory("SKU-1", "ZONE-A")
        assert inv["available"] == 70

    def test_ship_insufficient_stock_raises(self):
        db = FakeCassandraClient()
        handler = EventHandler(db)
        with pytest.raises(ValueError, match="Insufficient stock"):
            handler.handle({
                "event_id": "s1", "event_type": "PRODUCT_SHIPPED",
                "event_timestamp": 1000, "product_id": "SKU-1",
                "quantity": 10, "zone_id": "ZONE-A",
            })


class TestProductReserved:
    def test_reserve_moves_to_reserved(self):
        db = FakeCassandraClient()
        handler = EventHandler(db)
        handler.handle({
            "event_id": "r1", "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": 1000, "product_id": "SKU-1",
            "quantity": 100, "zone_id": "ZONE-A",
        })
        handler.handle({
            "event_id": "res1", "event_type": "PRODUCT_RESERVED",
            "event_timestamp": 2000, "product_id": "SKU-1",
            "quantity": 20, "zone_id": "ZONE-A",
        })
        inv = db.get_zone_inventory("SKU-1", "ZONE-A")
        assert inv["available"] == 80
        assert inv["reserved"] == 20


class TestNegativeQuantity:
    def test_negative_quantity_raises(self):
        db = FakeCassandraClient()
        handler = EventHandler(db)
        with pytest.raises(ValueError, match="Invalid quantity"):
            handler.handle({
                "event_id": "bad1", "event_type": "PRODUCT_RECEIVED",
                "event_timestamp": 1000, "product_id": "SKU-1",
                "quantity": -5, "zone_id": "ZONE-A",
            })


class TestUnknownEventType:
    def test_unknown_type_raises(self):
        db = FakeCassandraClient()
        handler = EventHandler(db)
        with pytest.raises(ValueError, match="Unknown event_type"):
            handler.handle({
                "event_id": "u1", "event_type": "NONEXISTENT",
                "event_timestamp": 1000,
            })


class TestOutOfOrder:
    def test_stale_event_dropped(self):
        db = FakeCassandraClient()
        handler = EventHandler(db)
        handler.handle({
            "event_id": "r1", "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": 2000, "product_id": "SKU-1",
            "quantity": 100, "zone_id": "ZONE-A",
        })
        handler.handle({
            "event_id": "r2", "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": 1000, "product_id": "SKU-1",
            "quantity": 50, "zone_id": "ZONE-A",
        })
        inv = db.get_zone_inventory("SKU-1", "ZONE-A")
        assert inv["available"] == 100
