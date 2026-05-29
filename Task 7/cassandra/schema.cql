CREATE KEYSPACE IF NOT EXISTS warehouse
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE warehouse;

CREATE TABLE IF NOT EXISTS inventory_by_product_zone (
    product_id          TEXT,
    zone_id             TEXT,
    available_quantity  INT,
    reserved_quantity   INT,
    last_event_id       TEXT,
    last_event_timestamp BIGINT,
    updated_at          TIMESTAMP,
    supplier_id         TEXT,
    PRIMARY KEY (product_id, zone_id)
) WITH CLUSTERING ORDER BY (zone_id ASC);

CREATE TABLE IF NOT EXISTS inventory_by_product (
    product_id          TEXT PRIMARY KEY,
    total_available     INT,
    total_reserved      INT,
    last_updated        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS inventory_by_zone (
    zone_id             TEXT,
    product_id          TEXT,
    available_quantity  INT,
    reserved_quantity   INT,
    last_updated        TIMESTAMP,
    PRIMARY KEY (zone_id, product_id)
) WITH CLUSTERING ORDER BY (product_id ASC);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id        TEXT PRIMARY KEY,
    event_type      TEXT,
    processed_at    TIMESTAMP
) WITH default_time_to_live = 604800;

CREATE TABLE IF NOT EXISTS orders (
    order_id        TEXT PRIMARY KEY,
    status          TEXT,
    items           TEXT,
    created_at      TIMESTAMP,
    completed_at    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS event_history (
    product_id          TEXT,
    event_timestamp     BIGINT,
    event_id            TEXT,
    event_type          TEXT,
    zone_id             TEXT,
    quantity            INT,
    PRIMARY KEY (product_id, event_timestamp, event_id)
) WITH CLUSTERING ORDER BY (event_timestamp DESC, event_id ASC)
  AND default_time_to_live = 2592000;
