#!/bin/bash

# ════════════════════════════════════════════════════════════════════════════════
#  Saturation Test Diagnostics
# ════════════════════════════════════════════════════════════════════════════════

GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
GRAFANA_CREDS="${GRAFANA_CREDS:-admin:admin}"

log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

# Test 1: Grafana connectivity
log "Test 1: Grafana Connectivity"
response=$(curl -s -w "\n%{http_code}" -u "${GRAFANA_CREDS}" "$GRAFANA_URL/api/datasources")
status=$(echo "$response" | tail -1)
body=$(echo "$response" | head -n-1)

if [[ "$status" == "200" ]]; then
    log "  ✓ Grafana reachable (HTTP 200)"
    # Check for Prometheus datasource
    if echo "$body" | grep -q '"prometheus"'; then
        log "  ✓ Prometheus datasource found"
    else
        log "  ✗ Prometheus datasource NOT found"
        log "    Available datasources:"
        echo "$body" | grep -o '"name":"[^"]*"' || echo "    (none)"
    fi
else
    log "  ✗ Grafana returned HTTP $status"
    log "    Check: GRAFANA_URL=$GRAFANA_URL, GRAFANA_CREDS=$GRAFANA_CREDS"
    exit 1
fi

# Test 2: Query available metrics
log ""
log "Test 2: Available Metrics in Prometheus"

query_simple() {
    local query="$1"
    local user="${GRAFANA_CREDS%%:*}"
    local password="${GRAFANA_CREDS##*:}"
    
    node << EOF 2>&1 | grep -v "^Error:" || echo "ERROR"
(async () => {
    const query = '$query';
    const url = '$GRAFANA_URL/api/datasources/proxy/uid/prometheus/api/v1/query?query=' + encodeURIComponent(query);
    const auth = Buffer.from('$user:$password').toString('base64');
    try {
        const response = await fetch(url, { 
            headers: { 'Authorization': 'Basic ' + auth },
            timeout: 5000 
        });
        const data = await response.json();
        const results = (data.data || {}).result || [];
        console.log(results.length);
    } catch (e) {
        console.error('Fetch failed: ' + e.message);
    }
})();
EOF
}

# Check for various metric types
metrics=(
    "http_server_duration_milliseconds_count"
    "k6_http_req_duration_milliseconds_sum"
    "process_cpu_utilization"
    "nodejs_eventloop_utilization_ratio"
    "k6_checks_total"
)

for metric in "${metrics[@]}"; do
    count=$(query_simple "count($metric)")
    log "  $metric: $count series"
done

# Test 3: Check service_name labels
log ""
log "Test 3: Available Service Names"

node << 'EOF'
(async () => {
    const url = 'http://localhost:3000/api/datasources/proxy/uid/prometheus/api/v1/label/service_name/values';
    const auth = Buffer.from('admin:admin').toString('base64');
    try {
        const response = await fetch(url, { 
            headers: { 'Authorization': 'Basic ' + auth },
            timeout: 5000 
        });
        const data = await response.json();
        const values = (data.data || []);
        if (values.length === 0) {
            console.log('  (no service_name labels found)');
        } else {
            values.forEach(v => console.log('  - ' + v));
        }
    } catch (e) {
        console.error('  Error: ' + e.message);
    }
})();
EOF

# Test 4: Docker services status
log ""
log "Test 4: Docker Services Status"
docker-compose -f api-tests/app-compose.yaml ps 2>/dev/null || log "  (docker-compose not available)"

# Test 5: Manual metric query
log ""
log "Test 5: Sample Metric Query (last 30s)"

node << 'EOF'
(async () => {
    const query = 'sum(rate(http_server_duration_milliseconds_count[30s]))';
    const url = 'http://localhost:3000/api/datasources/proxy/uid/prometheus/api/v1/query?query=' + encodeURIComponent(query);
    const auth = Buffer.from('admin:admin').toString('base64');
    try {
        const response = await fetch(url, { 
            headers: { 'Authorization': 'Basic ' + auth },
            timeout: 5000 
        });
        const data = await response.json();
        console.log('Response status:', data.status);
        const results = (data.data || {}).result || [];
        if (results.length === 0) {
            console.log('Result: NO DATA (metric not found or no recent data)');
        } else {
            results.forEach(r => {
                const val = r.value ? r.value[1] : 'null';
                console.log('Result:', val, 'Labels:', JSON.stringify(r.metric));
            });
        }
    } catch (e) {
        console.error('Error:', e.message);
    }
})();
EOF

log ""
log "═══════════════════════════════════════════════════════════════════════════════"
log "Diagnostics complete. Check output above for issues."
log ""
log "Common fixes:"
log "  1. Ensure Docker stack is running: docker-compose -f api-tests/app-compose.yaml ps"
log "  2. Ensure rest-wrapper is exporting metrics: docker logs rest-wrapper"
log "  3. Check Grafana http://localhost:3000 (admin:admin)"
log "  4. Verify Prometheus http://localhost:3000/api/datasources/proxy/uid/prometheus"
