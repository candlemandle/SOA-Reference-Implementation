import time
from prometheus_client import Counter, Gauge, Histogram
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

# --- HTTP metrics (required by assignment) ---

http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)

http_request_errors_total = Counter(
    "http_request_errors_total",
    "Total HTTP request errors",
    ["method", "endpoint", "error_type"],
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

# --- Event processing metrics ---

events_processed_total = Counter(
    "events_processed_total",
    "Total warehouse events successfully processed",
    ["event_type"],
)

events_dlq_total = Counter(
    "events_dlq_total",
    "Total events routed to DLQ",
)

cassandra_write_errors_total = Counter(
    "cassandra_write_errors_total",
    "Total Cassandra write failures",
)

event_processing_duration_seconds = Histogram(
    "event_processing_duration_seconds",
    "Processing time per event (Kafka receive -> Cassandra commit)",
    ["event_type"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

consumer_lag = Gauge(
    "consumer_lag",
    "Current consumer lag per partition",
    ["partition"],
)

# --- Cassandra infrastructure metrics ---

cassandra_read_duration_seconds = Histogram(
    "cassandra_read_duration_seconds",
    "Cassandra read latency",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

cassandra_write_duration_seconds = Histogram(
    "cassandra_write_duration_seconds",
    "Cassandra write (batch) latency",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)

cassandra_connected_hosts = Gauge(
    "cassandra_connected_hosts",
    "Number of connected Cassandra hosts",
)


class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path == "/metrics":
            return await call_next(request)

        method = request.method
        path = request.url.path

        start = time.time()
        try:
            response = await call_next(request)
            status = str(response.status_code)
            http_requests_total.labels(method=method, endpoint=path, status=status).inc()
            if response.status_code >= 400:
                err = "client_error" if response.status_code < 500 else "server_error"
                http_request_errors_total.labels(method=method, endpoint=path, error_type=err).inc()
            return response
        except Exception:
            http_request_errors_total.labels(method=method, endpoint=path, error_type="exception").inc()
            http_requests_total.labels(method=method, endpoint=path, status="500").inc()
            raise
        finally:
            http_request_duration_seconds.labels(method=method, endpoint=path).observe(time.time() - start)
