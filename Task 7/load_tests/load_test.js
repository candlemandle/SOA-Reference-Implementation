import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

const errorCounter = new Counter('business_errors');

export const options = {
    stages: [
        { duration: '10s', target: 10 },
        { duration: '20s', target: 10 },
        { duration: '10s', target: 0 },
    ],
    thresholds: {
        http_req_duration: ['p(95)<500', 'p(99)<1000'],
        http_req_failed: ['rate<0.01'],
    },
};

export default function () {
    const uid = `k6-${__VU}-${__ITER}-${Date.now()}`;

    // Health check
    const healthRes = http.get(`${BASE_URL}/health`);
    check(healthRes, {
        'health status 200': (r) => r.status === 200,
    });

    // Send PRODUCT_RECEIVED event
    const payload = JSON.stringify({
        event_id: `load-recv-${uid}`,
        event_type: 'PRODUCT_RECEIVED',
        event_timestamp: Date.now(),
        product_id: `LOAD-SKU-${__VU}`,
        quantity: Math.floor(Math.random() * 100) + 1,
        zone_id: `LOAD-ZONE-${__VU % 3}`,
    });

    const res = http.post(`${BASE_URL}/events`, payload, {
        headers: { 'Content-Type': 'application/json' },
    });

    const success = check(res, {
        'event sent (200)': (r) => r.status === 200,
        'response has status field': (r) => {
            try { return JSON.parse(r.body).status === 'sent'; }
            catch (e) { return false; }
        },
    });

    if (!success) {
        errorCounter.add(1);
    }

    sleep(0.1);
}

export function handleSummary(data) {
    const p95 = data.metrics.http_req_duration.values['p(95)'];
    const failRate = data.metrics.http_req_failed.values.rate;
    const totalReqs = data.metrics.http_reqs.values.count;

    const summary = {
        total_requests: totalReqs,
        p95_latency_ms: p95,
        error_rate: failRate,
        thresholds_passed: Object.values(data.root_group.checks).every(c => c.fails === 0),
    };

    return {
        stdout: JSON.stringify(summary, null, 2) + '\n',
        'load_test_results.json': JSON.stringify(summary, null, 2),
    };
}
