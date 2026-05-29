from pydantic import BaseModel
from typing import Optional


class EventV1(BaseModel):
    event_id: str
    event_type: str
    event_timestamp: int
    product_id: Optional[str] = None
    quantity: Optional[int] = None
    zone_id: Optional[str] = None
    from_zone_id: Optional[str] = None
    to_zone_id: Optional[str] = None
    order_id: Optional[str] = None
    order_items: Optional[str] = None


class EventV2(EventV1):
    supplier_id: Optional[str] = None
