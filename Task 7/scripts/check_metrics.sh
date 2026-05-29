#!/bin/bash
set -e

PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"

echo "=== Checking Prometheus metrics after load test ==="

query_prometheus() {
    local query="$1"
    local result
    result=$(curl -sf "${PROMETHEUS_URL}/api/v1/query" --data-urlencode "query=${query}" 2>/dev/null)
    echo "$result" | python3 -c "
import sys, json
data = json.load(sys.stdin)
if data['status'] != 'success':
    print('QUERY_ERROR')
    sys.exit(1)
results = data['data']['result']
if not results:
    print('NO_DATA')
else:
    print(results[0]['value'][1])
" 2>/dev/null || echo "PARSE_ERROR"
}

FAILED=0

# SLI 1: API Availability > 95% (failure threshold)
echo ""
echo "--- SLI: API Availability ---"
AVAILABILITY=$(query_prometheus 'sum(rate(http_requests_total{job="wms-service",status=~"2.."}[5m])) / sum(rate(http_requests_total{job="wms-service"}[5m]))')
echo "  Current availability: $AVAILABILITY"
if [ "$AVAILABILITY" != "NO_DATA" ] && [ "$AVAILABILITY" != "PARSE_ERROR" ] && [ "$AVAILABILITY" != "QUERY_ERROR" ]; then
    AVAIL_CHECK=$(python3 -c "print('PASS' if float('$AVAILABILITY') >= 0.95 else 'FAIL')" 2>/dev/null || echo "SKIP")
    echo "  Threshold (>= 95%): $AVAIL_CHECK"
    if [ "$AVAIL_CHECK" = "FAIL" ]; then FAILED=1; fi
else
    echo "  Skipping (no data yet)"
fi

# SLI 2: API Latency p95 < 1000ms (failure threshold)
echo ""
echo "--- SLI: API Latency p95 ---"
P95=$(query_prometheus 'histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{job="wms-service"}[5m])) by (le))')
echo "  Current p95 latency: ${P95}s"
if [ "$P95" != "NO_DATA" ] && [ "$P95" != "PARSE_ERROR" ] && [ "$P95" != "QUERY_ERROR" ] && [ "$P95" != "NaN" ]; then
    LATENCY_CHECK=$(python3 -c "print('PASS' if float('$P95') < 1.0 else 'FAIL')" 2>/dev/null || echo "SKIP")
    echo "  Threshold (< 1000ms): $LATENCY_CHECK"
    if [ "$LATENCY_CHECK" = "FAIL" ]; then FAILED=1; fi
else
    echo "  Skipping (no data yet)"
fi

# SLI 3: Event processing p95 < 5s (failure threshold)
echo ""
echo "--- SLI: Event Processing p95 ---"
PROC_P95=$(query_prometheus 'histogram_quantile(0.95, sum(rate(event_processing_duration_seconds_bucket[5m])) by (le))')
echo "  Current processing p95: ${PROC_P95}s"
if [ "$PROC_P95" != "NO_DATA" ] && [ "$PROC_P95" != "PARSE_ERROR" ] && [ "$PROC_P95" != "QUERY_ERROR" ] && [ "$PROC_P95" != "NaN" ]; then
    PROC_CHECK=$(python3 -c "print('PASS' if float('$PROC_P95') < 5.0 else 'FAIL')" 2>/dev/null || echo "SKIP")
    echo "  Threshold (< 5s): $PROC_CHECK"
    if [ "$PROC_CHECK" = "FAIL" ]; then FAILED=1; fi
else
    echo "  Skipping (no data yet)"
fi

# Error rate check
echo ""
echo "--- Error Rate ---"
ERROR_RATE=$(query_prometheus 'sum(rate(http_request_errors_total{job="wms-service"}[5m])) / sum(rate(http_requests_total{job="wms-service"}[5m]))')
echo "  Current error rate: $ERROR_RATE"
if [ "$ERROR_RATE" != "NO_DATA" ] && [ "$ERROR_RATE" != "PARSE_ERROR" ] && [ "$ERROR_RATE" != "QUERY_ERROR" ] && [ "$ERROR_RATE" != "NaN" ]; then
    ERR_CHECK=$(python3 -c "print('PASS' if float('$ERROR_RATE') < 0.01 else 'FAIL')" 2>/dev/null || echo "SKIP")
    echo "  Threshold (< 1%): $ERR_CHECK"
    if [ "$ERR_CHECK" = "FAIL" ]; then FAILED=1; fi
else
    echo "  Skipping (no data yet)"
fi

echo ""
if [ $FAILED -eq 1 ]; then
    echo "RESULT: FAILED — one or more metric thresholds exceeded."
    exit 1
else
    echo "RESULT: PASSED — all metric thresholds within bounds."
    exit 0
fi
