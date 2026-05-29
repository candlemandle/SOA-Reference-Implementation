"""Unit tests for WMS service Pydantic model validation."""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'wms_service'))

from models import EventV1, EventV2


class TestEventV1Validation:
    def test_valid_product_received(self):
        event = EventV1(
            event_id="evt-1",
            event_type="PRODUCT_RECEIVED",
            event_timestamp=1700000000000,
            product_id="SKU-001",
            quantity=100,
            zone_id="ZONE-A",
        )
        assert event.event_id == "evt-1"
        assert event.event_type == "PRODUCT_RECEIVED"
        assert event.quantity == 100

    def test_valid_product_moved(self):
        event = EventV1(
            event_id="evt-2",
            event_type="PRODUCT_MOVED",
            event_timestamp=1700000000000,
            product_id="SKU-001",
            quantity=20,
            from_zone_id="ZONE-A",
            to_zone_id="ZONE-B",
        )
        assert event.from_zone_id == "ZONE-A"
        assert event.to_zone_id == "ZONE-B"

    def test_valid_order_created(self):
        event = EventV1(
            event_id="evt-3",
            event_type="ORDER_CREATED",
            event_timestamp=1700000000000,
            order_id="ORD-001",
            order_items='[{"product_id":"SKU-001","zone_id":"ZONE-A","quantity":5}]',
        )
        assert event.order_id == "ORD-001"
        assert event.order_items is not None

    def test_missing_required_fields(self):
        with pytest.raises(Exception):
            EventV1(event_type="PRODUCT_RECEIVED", event_timestamp=1700000000000)

    def test_optional_fields_default_none(self):
        event = EventV1(
            event_id="evt-4",
            event_type="TEST",
            event_timestamp=1700000000000,
        )
        assert event.product_id is None
        assert event.quantity is None
        assert event.zone_id is None
        assert event.from_zone_id is None
        assert event.to_zone_id is None
        assert event.order_id is None
        assert event.order_items is None

    def test_dict_serialization(self):
        event = EventV1(
            event_id="evt-5",
            event_type="PRODUCT_RECEIVED",
            event_timestamp=1700000000000,
            product_id="SKU-001",
            quantity=50,
            zone_id="ZONE-A",
        )
        d = event.dict()
        assert d["event_id"] == "evt-5"
        assert d["quantity"] == 50
        assert "product_id" in d


class TestEventV2Validation:
    def test_v2_with_supplier_id(self):
        event = EventV2(
            event_id="evt-v2-1",
            event_type="PRODUCT_RECEIVED",
            event_timestamp=1700000000000,
            product_id="SKU-001",
            quantity=40,
            zone_id="ZONE-C",
            supplier_id="SUP-001",
        )
        assert event.supplier_id == "SUP-001"

    def test_v2_without_supplier_id(self):
        event = EventV2(
            event_id="evt-v2-2",
            event_type="PRODUCT_RECEIVED",
            event_timestamp=1700000000000,
            product_id="SKU-001",
            quantity=40,
            zone_id="ZONE-C",
        )
        assert event.supplier_id is None

    def test_v2_inherits_v1_fields(self):
        event = EventV2(
            event_id="evt-v2-3",
            event_type="TEST",
            event_timestamp=1700000000000,
        )
        assert hasattr(event, 'product_id')
        assert hasattr(event, 'order_id')
        assert hasattr(event, 'supplier_id')
