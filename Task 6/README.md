# Smart Warehouse – Event-Driven State Management

## Design Decisions

### Cassandra Data Model
- `inventory_by_product_zone` – partition key (product_id, zone_id) for O(1) lookup of stock per zone.
- `inventory_by_product` – partition key product_id, aggregated totals for fast global stock.
- `inventory_by_zone` – partition key zone_id, clustering product_id to list all products in a zone.
Three additional tables for idempotency (`processed_events`), orders, and audit log.

### Consistency Levels
- **Writes**: `QUORUM` – ensures majority of 3 nodes acknowledge, tolerating one node failure.
- **Reads**: `ONE` – lower latency, eventual consistency acceptable for inventory views (write path already quorum).

### Out‑of‑Order Handling
Each event has a `timestamp`. The last processed timestamp is stored per product. Events older than the last processed are ignored.

### Idempotency
`processed_events` table stores `event_id` before processing. Duplicate events are skipped.

### Atomic Updates (Logged Batches)
All three inventory tables are updated in a single `LOGGED BATCH` per event, guaranteeing either all or none succeed.

### Dead Letter Queue
Invalid events (e.g., negative quantity) are sent to `warehouse-events-dlq` with error reason, partition, offset. Consumer continues processing subsequent events.

### Schema Evolution
- Backward compatibility via Schema Registry.
- V2 added `supplier_id` with `default: null`.
- Consumer handles both versions; V1 gets `null` in Cassandra.

### Monitoring
- `/metrics` exposes Prometheus counters/histograms.
- `/health` returns 200 if Cassandra and Kafka are reachable.
- Grafana dashboard shows consumer lag, throughput, and write errors.

## Running the System

```bash
docker-compose up --build