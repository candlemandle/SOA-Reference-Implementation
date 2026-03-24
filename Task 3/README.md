HW3 — Flight Booking System: gRPC + Redis
Overview
A microservice-based flight booking system demonstrating inter-service communication, fault tolerance, and high availability.

The system consists of two main components:

Booking Service (FastAPI): A REST API handling client requests and user bookings.

Flight Service (gRPC): An internal service managing flight data, seat availability, and reservations.

Architecture
Plaintext
Client (REST) → Booking Service → (gRPC) → Flight Service
                      ↓                          ↓
                 PostgreSQL               PostgreSQL + Redis (Sentinel)
Core Features Implemented
Microservice Data Segregation: Two independent services, each with its own PostgreSQL database (booking_db and flight_db), ensuring clear domain boundaries.

Strict Contracts: Communication between services is strictly typed using Protocol Buffers (flight.proto).

Concurrency & Idempotency: * Uses row-level locking (SELECT ... FOR UPDATE) during seat reservations to prevent race conditions.

Uses booking_id as an idempotency key to safely handle duplicate requests without double-booking seats.

Inter-Service Security: Implemented gRPC interceptors (client and server-side) to authorize internal requests using an x-api-key.

Fault Tolerance & Resilience:

Retries & Exponential Backoff: Automatically retries transient gRPC errors (UNAVAILABLE, DEADLINE_EXCEEDED) while ignoring business-logic errors.

Circuit Breaker: Prevents cascading failures by halting requests to the Flight Service if it becomes unresponsive (transitions through CLOSED → OPEN → HALF_OPEN).

High Availability Caching:

Cache-Aside Pattern: Flight searches and details are cached in Redis to reduce DB load, with automatic invalidation upon seat reservation or cancellation.

Redis Sentinel: Configured a highly available Redis setup (Master, Replica, Sentinel). The Flight Service dynamically reconnects to the new master during a failover.

How to Run
Start the infrastructure and services:

Bash
docker compose up --build
Test the REST API (Booking Service):

Search flights: curl "http://localhost:8000/flights?origin=SVO&destination=LED"

Create a booking:

Bash
curl -X POST "http://localhost:8000/bookings" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "u1", "flight_id": 1, "passenger_name": "John Doe", "passenger_email": "john@example.com", "seat_count": 2}'
Cancel a booking: curl -X POST "http://localhost:8000/bookings/<BOOKING_ID>/cancel"

Testing Resilience Mechanisms
Circuit Breaker & Retries:

Stop the gRPC service: docker compose stop flight-service

Send requests to the Booking API. You will initially see retries, followed by the Circuit Breaker opening and immediately returning 503 Service Unavailable.

Start the service again (docker compose start flight-service) to see the Circuit Breaker recover.

Redis Sentinel Failover:

Check the current master: docker compose exec redis-sentinel redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster

Stop the master: docker compose stop redis-master

Wait a few seconds and check the master again. Sentinel will have automatically promoted the replica, and the Flight Service will continue functioning.

Unit Tests:
Run the test suite (covers retry and backoff mechanics):

Bash
cd tests
python3 -m pytest -v