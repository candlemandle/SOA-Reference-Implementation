import time
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

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
