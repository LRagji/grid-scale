#!/bin/bash

# ════════════════════════════════════════════════════════════════════════════════
#  rest-wrapper Saturation Point Finder
# ════════════════════════════════════════════════════════════════════════════════
#
# Find the VU level at which rest-wrapper starts to saturate under load.
# Uses k6 to generate increasing load, samples key metrics from Prometheus,
# and identifies saturation signals.
#
# USAGE:
#   ./find-saturation.sh [--tests file1,file2,...] [vus ...]
#   ./find-saturation.sh                      # Use defaults: 10 25 50 100 200 400
#   ./find-saturation.sh 50 100 200 400 800   # Custom VU sweep
#   ./find-saturation.sh --tests seq-single-write-range-read-tests,seq-bulk-tag-write-range-read-tests 100 200
#
# REQUIREMENTS:
#   - Node.js 18+ (for native fetch())
#   - Docker & docker-compose
#   - redis-cli
#   - rest-wrapper + k6 stack running (auto-starts with -s flag)
#

# ── Configuration ──────────────────────────────────────────────────────────────

# Saturation thresholds (tune these based on your SLOs)
THR_CPU=0.80              # 80% - process CPU utilization
THR_EVLOOP=0.80           # 80% - Node.js event loop
THR_EVDELAY_S=0.05        # 50ms - event loop delay P90
THR_RESP_MS=500           # 500ms - k6 avg response time
THR_CHECK_RATE=0.99       # 99% - K6 check pass rate
THR_RPS_GROWTH=0.05       # 5% - minimum RPS growth floor

# Grafana/Prometheus access
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
GRAFANA_CREDS="${GRAFANA_CREDS:-admin:admin}"

# Docker/k6 configuration
DOCKER_NETWORK="grid-store-network"
K6_IMAGE="grafana/k6:latest"
START_STACK=false
WARMUP_SECS=60
SETTLE_SECS=30

# Script location
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TEST_FILES_RAW="${K6_TEST_FILES:-seq-single-write-range-read-tests}"
K6_TEST_SCRIPTS=()
K6_CONTAINERS=()
RESULTS_CSV="/tmp/saturation-results-$$.csv"

# ── Utilities ──────────────────────────────────────────────────────────────────

log() {
    echo "$(date '+%H:%M:%S') | $*"
}

hr() {
    printf '%s\n' "$(printf '═%.0s' {1..98})"
}

check_prereqs() {
    log "Checking prerequisites..."
    
    command -v node >/dev/null 2>&1 || {
        log "ERROR: Node.js not found. Install Node.js 18+"
        exit 1
    }
    
    command -v docker >/dev/null 2>&1 || {
        log "ERROR: Docker not found"
        exit 1
    }
    
    command -v redis-cli >/dev/null 2>&1 || {
        log "WARNING: redis-cli not found (needed for FLUSHALL)"
    }
    
    local node_major
    node_major=$(node -v | cut -d. -f1 | sed 's/v//')
    [[ "$node_major" -ge 18 ]] || {
        log "ERROR: Node.js 18+ required (found $(node -v))"
        exit 1
    }
    
    ensure_k6_test_scripts
    
    log "  ✓ Prerequisites OK"
}

resolve_test_script_path() {
    local input="$1"

    if [[ -f "$input" ]]; then
        echo "$input"
        return
    fi

    if [[ -f "$SCRIPT_DIR/$input" ]]; then
        echo "$SCRIPT_DIR/$input"
        return
    fi

    if [[ -f "$REPO_ROOT/$input" ]]; then
        echo "$REPO_ROOT/$input"
        return
    fi

    if [[ "$input" == *.ts ]]; then
        local base
        base=$(basename "$input" .ts)
        echo "$REPO_ROOT/dist/api-tests/${base}.js"
        return
    fi

    if [[ "$input" == *.js ]]; then
        echo "$input"
        return
    fi

    echo "$REPO_ROOT/dist/api-tests/${input}.js"
}

ensure_k6_test_scripts() {
    local needs_build=false
    local item resolved
    K6_TEST_SCRIPTS=()

    IFS=',' read -r -a input_tests <<< "$TEST_FILES_RAW"
    for item in "${input_tests[@]}"; do
        item=$(echo "$item" | xargs)
        [[ -n "$item" ]] || continue
        resolved=$(resolve_test_script_path "$item")
        K6_TEST_SCRIPTS+=("$resolved")
        [[ -f "$resolved" ]] || needs_build=true
    done

    if [[ ${#K6_TEST_SCRIPTS[@]} -eq 0 ]]; then
        log "ERROR: No valid test files specified via --tests/K6_TEST_FILES"
        exit 1
    fi

    if [[ "$needs_build" == true ]]; then
        log "  One or more compiled k6 scripts missing; building project test artifacts..."
        (
            cd "$REPO_ROOT" && npm run build-test
        ) >/tmp/find-saturation-build.log 2>&1 || {
            log "ERROR: Failed to build k6 scripts"
            log "  Build log: /tmp/find-saturation-build.log"
            exit 1
        }
    fi

    for resolved in "${K6_TEST_SCRIPTS[@]}"; do
        [[ -f "$resolved" ]] || {
            log "ERROR: k6 script not found: $resolved"
            log "  Build log (if any): /tmp/find-saturation-build.log"
            exit 1
        }
    done

    log "  ✓ Using k6 test scripts: ${K6_TEST_SCRIPTS[*]}"
}

# ── Prometheus Queries ────────────────────────────────────────────────────────

query_prom() {
    # Usage: query_prom "promql"
    # Returns: scalar value or "0" if no data
    local query="$1"
    local user="${GRAFANA_CREDS%%:*}"
    local password="${GRAFANA_CREDS##*:}"

    node << EOF
(async () => {
    const query = '$query';
    const baseUrl = '$GRAFANA_URL';
    const user = '$user';
    const password = '$password';
    const url = baseUrl + '/api/datasources/proxy/uid/prometheus/api/v1/query?query=' + encodeURIComponent(query);
    const auth = Buffer.from(user + ':' + password).toString('base64');
    try {
        const response = await fetch(url, {
            headers: { 'Authorization': 'Basic ' + auth },
            timeout: 10000
        });
        if (!response.ok) {
            console.error('HTTP ' + response.status);
            console.log("0");
            return;
        }
        const data = await response.json();
        if (data.status !== 'success') {
            console.error('Prometheus: ' + (data.error || 'query failed'));
            console.log("0");
            return;
        }
        const results = (data.data || {}).result || [];
        if (!results.length) {
            console.log("0");
            return;
        }
        const bad = new Set(['NaN', 'Inf', '+Inf', '-Inf']);
        for (const r of results) {
            const v = r.value ? r.value[1] : null;
            if (v && !bad.has(v)) {
                console.log(v);
                return;
            }
        }
        console.log("0");
    } catch (e) {
        console.error('Fetch error: ' + e.message);
        console.log("0");
    }
})();
EOF
}

# ── Docker/Redis ───────────────────────────────────────────────────────────────

start_stack() {
    log "Starting Docker Compose stack..."
    cd "$SCRIPT_DIR"
    local compose_output status
    compose_output=$(docker-compose -f app-compose.yaml up -d 2>&1)
    status=$?

    if [[ $status -ne 0 ]] && echo "$compose_output" | grep -q 'incorrect label com.docker.compose.network'; then
        log "  Found stale Docker network '$DOCKER_NETWORK'; recreating it for compose"
        docker network rm "$DOCKER_NETWORK" >/dev/null 2>&1 || true
        compose_output=$(docker-compose -f app-compose.yaml up -d 2>&1)
        status=$?
    fi

    if [[ $status -ne 0 ]]; then
        echo "$compose_output"
        log "ERROR: Failed to start Docker Compose stack"
        exit 1
    fi

    echo "$compose_output"
    sleep 5
    log "  ✓ Stack started"
}

is_stack_running() {
    docker ps --format '{{.Names}}' | grep -Eq '(^|-)rest-wrapper(-|$)|(^|-)otel(-|$)|(^|-)redis-meta(-|$)|(^|-)redis-data(-|$)'
}

ensure_stack_running() {
    if is_stack_running; then
        log "  ✓ App stack already running"
        return
    fi

    log "  App stack not detected; auto-starting app-compose.yaml"
    start_stack

    local i
    for i in {1..30}; do
        if is_stack_running; then
            log "  ✓ App stack is now running"
            return
        fi
        sleep 2
    done

    log "ERROR: App stack did not become ready in time"
    exit 1
}

flush_redis() {
    redis-cli -h 127.0.0.1 FLUSHALL >/dev/null 2>&1 || \
    docker exec api-tests-redis-meta-1 redis-cli FLUSHALL >/dev/null 2>&1 || \
    true
}

resolve_docker_network() {
    if docker network inspect "$DOCKER_NETWORK" >/dev/null 2>&1; then
        return
    fi

    local candidate network_name
    for candidate in api-tests-rest-wrapper-1 rest-wrapper; do
        if docker inspect "$candidate" >/dev/null 2>&1; then
            network_name=$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{println $k}}{{end}}' "$candidate" | head -n1 | tr -d '[:space:]')
            if [[ -n "$network_name" ]]; then
                DOCKER_NETWORK="$network_name"
                log "  Using detected Docker network: $DOCKER_NETWORK"
                return
            fi
        fi
    done

    log "  Docker network '$DOCKER_NETWORK' not found; creating it"
    docker network create "$DOCKER_NETWORK" >/dev/null || {
        log "ERROR: Failed to create Docker network '$DOCKER_NETWORK'"
        exit 1
    }
}

ensure_rest_wrapper_on_network() {
    local attached
    attached=$(docker network inspect "$DOCKER_NETWORK" --format '{{json .Containers}}' 2>/dev/null | grep -E 'api-tests-rest-wrapper-1|"rest-wrapper"' || true)
    if [[ -n "$attached" ]]; then
        return
    fi

    log "  rest-wrapper not attached to '$DOCKER_NETWORK'; recovering service network"
    (
        cd "$SCRIPT_DIR" && docker-compose -f app-compose.yaml up -d rest-wrapper
    ) >/tmp/find-saturation-rest-wrapper-recover.log 2>&1 || {
        log "ERROR: Failed to recover rest-wrapper network attachment"
        log "  Recovery log: /tmp/find-saturation-rest-wrapper-recover.log"
        exit 1
    }

    local i
    for i in {1..20}; do
        attached=$(docker network inspect "$DOCKER_NETWORK" --format '{{json .Containers}}' 2>/dev/null | grep -E 'api-tests-rest-wrapper-1|"rest-wrapper"' || true)
        if [[ -n "$attached" ]]; then
            log "  ✓ rest-wrapper attached to '$DOCKER_NETWORK'"
            return
        fi
        sleep 1
    done

    log "ERROR: rest-wrapper is still not attached to '$DOCKER_NETWORK'"
    log "  Recovery log: /tmp/find-saturation-rest-wrapper-recover.log"
    exit 1
}

# ── K6 Management ──────────────────────────────────────────────────────────────

start_k6() {
    local vus="$1"
    local idx script container_name
    
    log "  Starting k6 (VUs=${vus}, tests=${#K6_TEST_SCRIPTS[@]})..."

    K6_CONTAINERS=()
    for idx in "${!K6_TEST_SCRIPTS[@]}"; do
        script="${K6_TEST_SCRIPTS[$idx]}"
        container_name="k6-saturation-$RANDOM-$idx"

        docker run -d \
            --name "$container_name" \
            --network "$DOCKER_NETWORK" \
            -v "${script}:/scripts/test.js:ro" \
            -e "TEST_URL=http://rest-wrapper:8080" \
            -e "SCRIPT_VUS=${vus}" \
            -e "SCRIPT_ITERATIONS=999999" \
            -e "SCRIPT_MAX_DURATION=20m" \
            -e "SLEEP_DURATION=0" \
            -e "K6_OTEL_GRPC_EXPORTER_INSECURE=true" \
            -e "K6_OTEL_METRIC_PREFIX=k6_" \
            -e "K6_OTEL_GRPC_EXPORTER_ENDPOINT=otel:4317" \
            "$K6_IMAGE" \
            run --out opentelemetry /scripts/test.js > /dev/null || {
            log "ERROR: Failed to start k6 container for script '$script' on network '$DOCKER_NETWORK'"
            stop_k6
            exit 1
        }

        K6_CONTAINERS+=("$container_name")
    done
}

stop_k6() {
    log "  Stopping k6..."
    local c
    for c in "${K6_CONTAINERS[@]}"; do
        docker stop "$c" >/dev/null 2>&1 || true
        docker rm -f "$c" >/dev/null 2>&1 || true
    done
    K6_CONTAINERS=()
}

# ── Metric Sampling ────────────────────────────────────────────────────────────

sample_metrics() {
    local vus="$1"
    local win="${SETTLE_SECS}s"

    log "  Sampling metrics (${win} window)..."

    # Sample all metrics
    local rps resp_sum resp_count resp_val cpu evloop evdelay checks redis_ops
    local cpu_pct el_pct checks_pct resp_int redis_ops_int

    rps=$(query_prom "sum(rate(http_server_duration_milliseconds_count{service_name=\"rest-wrapper\"}[${win}]))")
    
    resp_sum=$(query_prom "sum(rate(k6_http_req_duration_milliseconds_sum{service_name=\"k6\"}[${win}]))")
    resp_count=$(query_prom "sum(rate(k6_http_req_duration_milliseconds_count{service_name=\"k6\"}[${win}]))")
    
    # Handle NaN in response calculation
    if [[ "$resp_count" == "0" ]] || [[ "$resp_sum" == "0" ]]; then
        resp_val="0"
    else
        resp_val=$(node -e "const a=parseFloat('${resp_sum}'); const b=parseFloat('${resp_count}'); const r = b > 0 ? a/b : 0; console.log(isFinite(r) ? r : 0)")
    fi

    cpu=$(query_prom "avg_over_time(sum(process_cpu_utilization{service_name=\"rest-wrapper\",process_cpu_state=~\"user|system\"})[${win}:])")
    evloop=$(query_prom "avg_over_time(nodejs_eventloop_utilization_ratio{service_name=\"rest-wrapper\"}[${win}])")
    evdelay=$(query_prom "avg_over_time(nodejs_eventloop_delay_p90_seconds{service_name=\"rest-wrapper\"}[${win}])")
    
    # K6 check pass rate - handle missing metrics
    checks=$(query_prom "sum(rate(k6_checks_total{service_name=\"k6\",condition=\"pass\"}[${win}])) / sum(rate(k6_checks_total{service_name=\"k6\"}[${win}]))")
    
    # Handle NaN from checks division
    checks=$(node -e "const c = parseFloat('${checks}'); console.log(isFinite(c) ? c : 1)")
    # Redis commands/sec (meta + data combined)
    redis_ops=$(query_prom "sum(rate(redis_commands_processed_total[${win}]))")
    redis_ops=$(node -e "const r = parseFloat('${redis_ops}'); console.log(isNaN(r) ? '0' : r)")

    # Format percentages - ensure no NaN
    cpu_pct=$(node -e "const c = parseFloat('${cpu}'); console.log(isNaN(c) ? '0.0' : (c*100).toFixed(1))")
    el_pct=$(node -e "const e = parseFloat('${evloop}'); console.log(isNaN(e) ? '0.0' : (e*100).toFixed(1))")
    checks_pct=$(node -e "const chk = parseFloat('${checks}'); console.log(isNaN(chk) ? '100.00' : (chk*100).toFixed(2))")
    resp_int=$(node -e "const r = parseFloat('${resp_val}'); console.log(isNaN(r) ? '0' : Math.round(r))")
    redis_ops_int=$(node -e "const r = parseFloat('${redis_ops}'); console.log(isNaN(r) ? '0' : Math.round(r))")

    log "  ✓ VUs=${vus} | RPS=$(printf '%.2f' "${rps}") | Resp=${resp_int}ms | CPU=${cpu_pct}% | EL=${el_pct}% | Checks=${checks_pct}% | Redis=${redis_ops_int}ops/s"

    # Ensure no NaN in CSV output
    rps=$(node -e "const r = parseFloat('${rps}'); console.log(isNaN(r) ? '0' : r)")
    resp_val=$(node -e "const r = parseFloat('${resp_val}'); console.log(isNaN(r) ? '0' : r)")
    cpu=$(node -e "const c = parseFloat('${cpu}'); console.log(isNaN(c) ? '0' : c)")
    evloop=$(node -e "const e = parseFloat('${evloop}'); console.log(isNaN(e) ? '0' : e)")
    evdelay=$(node -e "const d = parseFloat('${evdelay}'); console.log(isNaN(d) ? '0' : d)")
    checks=$(node -e "const c = parseFloat('${checks}'); console.log(isNaN(c) ? '1' : c)")

    # Write CSV row
    printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$vus" "$rps" "$resp_val" "$cpu" "$evloop" "$evdelay" "$checks" "$redis_ops" >> "$RESULTS_CSV"
}

# ── Analysis & Verdict ────────────────────────────────────────────────────────

analyze_and_print() {
    local csv_file="$1"
    
    log "Analyzing results..."
    
    CSV_FILE="$csv_file" \
    THR_CPU="$THR_CPU" \
    THR_EVLOOP="$THR_EVLOOP" \
    THR_EVDELAY_S="$THR_EVDELAY_S" \
    THR_RESP_MS="$THR_RESP_MS" \
    THR_CHECK_RATE="$THR_CHECK_RATE" \
    THR_RPS_GROWTH="$THR_RPS_GROWTH" \
    node << 'ANALYZE'
(async () => {
    const fs = require('fs');
    const csv_path = process.env.CSV_FILE;
    const csv = fs.readFileSync(csv_path, 'utf-8');
    const lines = csv.trim().split('\n');
    const header = lines[0].split(',');
    const rows = lines.slice(1).map(line => {
        const vals = line.split(',');
        return {
            vus: Math.max(0, parseFloat(vals[0]) || 0),
            rps: Math.max(0, parseFloat(vals[1]) || 0),
            resp: Math.max(0, parseFloat(vals[2]) || 0),
            cpu: Math.max(0, Math.min(1, parseFloat(vals[3]) || 0)),
            evloop: Math.max(0, Math.min(1, parseFloat(vals[4]) || 0)),
            evdelay: Math.max(0, parseFloat(vals[5]) || 0),
            checks: Math.max(0, Math.min(1, parseFloat(vals[6]) || 1)),
            redis_ops: Math.max(0, parseFloat(vals[7]) || 0)
        };
    }).filter(r => !isNaN(r.vus) && r.vus > 0);

    if (rows.length === 0) {
        console.log('ERROR: No valid data in results CSV');
        return;
    }

    // Saturation detection thresholds
    const THR_CPU = parseFloat(process.env.THR_CPU);
    const THR_EVLOOP = parseFloat(process.env.THR_EVLOOP);
    const THR_EVDELAY_S = parseFloat(process.env.THR_EVDELAY_S);
    const THR_RESP_MS = parseFloat(process.env.THR_RESP_MS);
    const THR_CHECK_RATE = parseFloat(process.env.THR_CHECK_RATE);
    const THR_RPS_GROWTH = parseFloat(process.env.THR_RPS_GROWTH);

    // Identify saturation point
    let saturation_vu = null;
    let saturation_reason = [];
    let max_rps = 0;
    let max_rps_vu = 0;

    for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        max_rps = Math.max(max_rps, r.rps);
        
        if (r.rps > max_rps_vu) max_rps_vu = r.rps;

        const signals = [];
        if (r.cpu >= THR_CPU) signals.push('CPU');
        if (r.evloop >= THR_EVLOOP) signals.push('EventLoop');
        if (r.evdelay >= THR_EVDELAY_S) signals.push('ELDelay');
        if (r.resp >= THR_RESP_MS) signals.push('Latency');
        if (r.checks < THR_CHECK_RATE && r.checks > 0) signals.push('Checks');

        // RPS growth stall
        if (i > 0) {
            const prev_rps = rows[i-1].rps;
            const growth = prev_rps > 0 ? (r.rps - prev_rps) / prev_rps : 0;
            if (growth < THR_RPS_GROWTH && growth >= 0) {
                signals.push('RPSStall');
            }
        }

        if (signals.length >= 1 && saturation_vu === null) {
            saturation_vu = r.vus;
            saturation_reason = signals;
        }
    }

    // Format and print results
    console.log('');
    const columns = [
        { header: 'VUs', width: 6, align: 'right' },
        { header: 'RPS', width: 10, align: 'right' },
        { header: 'Resp(ms)', width: 10, align: 'right' },
        { header: 'CPU(%)', width: 8, align: 'right' },
        { header: 'EL(%)', width: 8, align: 'right' },
        { header: 'Delay(ms)', width: 10, align: 'right' },
        { header: 'Checks(%)', width: 10, align: 'right' },
        { header: 'Redis(ops/s)', width: 14, align: 'right' }
    ];

    const separator = '+' + columns.map(c => '-'.repeat(c.width + 2)).join('+') + '+';
    const formatCell = (value, col) => {
        const s = String(value);
        return col.align === 'right' ? s.padStart(col.width) : s.padEnd(col.width);
    };

    console.log(separator);
    console.log('| ' + columns.map(c => formatCell(c.header, { width: c.width, align: 'left' })).join(' | ') + ' |');
    console.log(separator);

    rows.forEach(r => {
        const values = [
            Math.round(r.vus),
            r.rps.toFixed(2),
            Math.round(r.resp),
            (r.cpu * 100).toFixed(1),
            (r.evloop * 100).toFixed(1),
            (r.evdelay * 1000).toFixed(1),
            (r.checks * 100).toFixed(2),
            Math.round(r.redis_ops)
        ];
        console.log('| ' + values.map((v, i) => formatCell(v, columns[i])).join(' | ') + ' |');
    });
    console.log(separator);
    console.log('');

    // Verdict
    if (saturation_vu === null) {
        console.log('NO saturation detected at any tested VU level (max: ' + Math.round(rows[rows.length-1].vus) + ' VUs).');
        console.log('Highest measured RPS: ' + max_rps.toFixed(2) + ' req/s at ' + Math.round(rows[rows.length-1].vus) + ' VUs.');
        console.log('Consider re-running with higher VU counts.');
    } else {
        const safe_vu = saturation_vu > 0 ? Math.max(1, Math.round(saturation_vu / 2)) : saturation_vu;
        console.log('SATURATION starting at: ' + Math.round(saturation_vu) + ' VUs');
        console.log('Signals: ' + saturation_reason.join(', '));
        console.log('Recommend safe limit: ' + safe_vu + ' VUs (50% of saturation point)');
        console.log('');
        console.log('Ratios at saturation:');
        const sat_row = rows.find(r => r.vus === saturation_vu);
        if (sat_row) {
            console.log('  CPU: ' + (sat_row.cpu * 100).toFixed(1) + '%');
            console.log('  Event Loop: ' + (sat_row.evloop * 100).toFixed(1) + '%');
            console.log('  Response Time: ' + Math.round(sat_row.resp) + 'ms');
            console.log('  Redis ops/s: ' + Math.round(sat_row.redis_ops));
        }
    }
    console.log('');
})();
ANALYZE
}

# ── Main ───────────────────────────────────────────────────────────────────────

main() {
    local vus_array=()

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --tests)
                TEST_FILES_RAW="$2"
                shift 2
                ;;
            --tests=*)
                TEST_FILES_RAW="${1#*=}"
                shift
                ;;
            *)
                vus_array+=("$1")
                shift
                ;;
        esac
    done
    
    # Use defaults if no args provided
    if [[ ${#vus_array[@]} -eq 0 ]]; then
        vus_array=(10 25 50 100 200 400)
    fi

    hr
    log "rest-wrapper Saturation Finder"
    hr
    
    check_prereqs
    
    if [[ "$START_STACK" == "true" ]]; then
        start_stack
    else
        ensure_stack_running
    fi

    resolve_docker_network
    ensure_rest_wrapper_on_network
    
    # Initialize results CSV
    printf 'vus,rps,resp_ms,cpu,evloop,evdelay_s,checks,redis_ops\n' > "$RESULTS_CSV"
    
    log ""
    log "Saturation Test Parameters:"
    log "  Thresholds: CPU=$THR_CPU EL=$THR_EVLOOP Delay=${THR_EVDELAY_S}s Resp=${THR_RESP_MS}ms Checks=$THR_CHECK_RATE"
    log "  Test files: ${K6_TEST_SCRIPTS[*]}"
    log "  Warmup: ${WARMUP_SECS}s, Settle: ${SETTLE_SECS}s"
    log "  VU Sweep: ${vus_array[*]}"
    log ""
    
    # Run tests at each VU level
    for vus in "${vus_array[@]}"; do
        log "Testing at ${vus} VUs..."
        
        flush_redis
        start_k6 "$vus"
        
        log "  Warmup for ${WARMUP_SECS}s..."
        sleep "$WARMUP_SECS"
        
        sample_metrics "$vus"
        
        stop_k6
        
        log "  Cooldown 15s..."
        sleep 15
        log ""
    done
    
    # Analyze and print results
    analyze_and_print "$RESULTS_CSV"
    
    # Capture max RPS for next iteration suggestion (bash 3.x compatible)
    local num_vus=${#vus_array[@]}
    if [[ $num_vus -gt 0 ]]; then
        local max_vu_idx=$((num_vus - 1))
        local max_vu="${vus_array[$max_vu_idx]}"
        log "Rerun with higher VUs: ./find-saturation.sh $((max_vu*2)) $((max_vu*4))"
    fi
    
    hr
}

main "$@"
