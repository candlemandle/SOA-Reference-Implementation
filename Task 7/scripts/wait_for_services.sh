#!/bin/bash
set -e

WMS_URL="${WMS_URL:-http://localhost:8000}"
CONSUMER_URL="${CONSUMER_URL:-http://localhost:8001}"
PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"
MAX_WAIT=300
INTERVAL=5

echo "Waiting for services to be ready (timeout: ${MAX_WAIT}s)..."

wait_for_url() {
    local name="$1"
    local url="$2"
    local elapsed=0

    while [ $elapsed -lt $MAX_WAIT ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            echo "  $name is ready."
            return 0
        fi
        echo "  $name not ready, retrying in ${INTERVAL}s... (${elapsed}s elapsed)"
        sleep $INTERVAL
        elapsed=$((elapsed + INTERVAL))
    done

    echo "  ERROR: $name did not become ready within ${MAX_WAIT}s"
    return 1
}

wait_for_url "WMS Service" "${WMS_URL}/health"
wait_for_url "Consumer Service" "${CONSUMER_URL}/health"
wait_for_url "Prometheus" "${PROMETHEUS_URL}/-/ready"

echo "All services are ready."
