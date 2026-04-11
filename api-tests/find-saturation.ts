import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

type CommandResult = {
    code: number;
    stdout: string;
    stderr: string;
};

type Row = {
    vus: number;
    rps: number;
    resp: number;
    cpu: number;
    evloop: number;
    evdelay: number;
    checks: number;
    redis_ops: number;
};

type CliArgs = {
    vusArray: number[];
    testsRaw: string | null;
    showHelp: boolean;
};

const __filename = fileURLToPath(import.meta.url);
const SCRIPT_DIR = dirname(__filename);
const REPO_ROOT = resolve(SCRIPT_DIR, "..");

const THR_CPU = 0.8;
const THR_EVLOOP = 0.8;
const THR_EVDELAY_S = 0.05;
const THR_RESP_MS = 500;
const THR_CHECK_RATE = 0.99;
const THR_RPS_GROWTH = 0.05;

const GRAFANA_URL = process.env.GRAFANA_URL ?? "http://localhost:3000";
const GRAFANA_CREDS = process.env.GRAFANA_CREDS ?? "admin:admin";

const K6_IMAGE = "grafana/k6:latest";
const START_STACK = false;
const WARMUP_SECS = 60;
const SETTLE_SECS = 30;
let DOCKER_NETWORK = "grid-store-network";

let TEST_FILES_RAW = process.env.K6_TEST_FILES ?? "seq-single-write-range-read-tests";
let K6_TEST_SCRIPTS: string[] = [];
let K6_CONTAINERS: string[] = [];

const runTempDir = mkdtempSync(join(tmpdir(), "find-saturation-"));
const RESULTS_CSV = join(runTempDir, "results.csv");
const BUILD_LOG = join(runTempDir, "build.log");
const RECOVER_LOG = join(runTempDir, "recover.log");

function log(message = ""): void {
    const now = new Date();
    const t = now.toTimeString().slice(0, 8);
    console.log(`${t} | ${message}`);
}

function hr(): void {
    console.log("═".repeat(98));
}

function printHelp(): void {
    console.log("rest-wrapper Saturation Finder (TypeScript)");
    console.log("");
    console.log("Usage:");
    console.log("  node ./dist/api-tests/find-saturation.js [--tests file1,file2,...] [vus ...]");
    console.log("  npm run saturation:find -- [--tests file1,file2,...] [vus ...]");
    console.log("");
    console.log("Options:");
    console.log("  --tests <files>   Comma-separated k6 test files or names");
    console.log("  --tests=<files>   Same as above");
    console.log("  -h, --help        Show this help message");
    console.log("");
    console.log("Behavior:");
    console.log("  - Defaults VU sweep to: 10 25 50 100 200 400");
    console.log("  - Resolves test names to ./dist/api-tests/*.js");
    console.log("  - Auto-builds test artifacts when compiled k6 scripts are missing");
    console.log("  - Auto-detects Docker compose command (docker compose or docker-compose)");
    console.log("  - Samples Prometheus via Grafana datasource proxy");
    console.log("");
    console.log("Environment variables:");
    console.log("  GRAFANA_URL        Default: http://localhost:3000");
    console.log("  GRAFANA_CREDS      Default: admin:admin");
    console.log("  K6_TEST_FILES      Default: seq-single-write-range-read-tests");
    console.log("");
    console.log("Examples:");
    console.log("  npm run saturation:find");
    console.log("  npm run saturation:find -- 50 100 200 400");
    console.log("  npm run saturation:find -- --tests seq-single-write-range-read-tests,seq-bulk-tag-write-range-read-tests 100 200");
}

function sleep(ms: number): Promise<void> {
    return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

async function runCommand(command: string, args: string[], cwd?: string): Promise<CommandResult> {
    return await new Promise((resolveCmd) => {
        const child = spawn(command, args, {
            cwd,
            stdio: ["ignore", "pipe", "pipe"],
            env: process.env,
        });

        let stdout = "";
        let stderr = "";

        child.stdout.on("data", (chunk) => {
            stdout += chunk.toString();
        });
        child.stderr.on("data", (chunk) => {
            stderr += chunk.toString();
        });

        child.on("error", () => {
            resolveCmd({ code: 127, stdout, stderr: `${stderr}\ncommand not found: ${command}`.trim() });
        });

        child.on("close", (code) => {
            resolveCmd({ code: code ?? 1, stdout, stderr });
        });
    });
}

async function hasCommand(command: string, args = ["--version"]): Promise<boolean> {
    const result = await runCommand(command, args);
    return result.code === 0;
}

async function detectComposeCommand(): Promise<{ command: string; argsPrefix: string[] }> {
    if (await hasCommand("docker", ["compose", "version"])) {
        return { command: "docker", argsPrefix: ["compose"] };
    }
    if (await hasCommand("docker-compose", ["version"])) {
        return { command: "docker-compose", argsPrefix: [] };
    }
    throw new Error("docker compose or docker-compose is required");
}

async function composeRun(compose: { command: string; argsPrefix: string[] }, args: string[]): Promise<CommandResult> {
    return await runCommand(compose.command, [...compose.argsPrefix, ...args], SCRIPT_DIR);
}

function parseCreds(creds: string): { user: string; password: string } {
    const idx = creds.indexOf(":");
    if (idx < 0) {
        return { user: creds, password: "" };
    }
    return { user: creds.slice(0, idx), password: creds.slice(idx + 1) };
}

function basicAuth(creds: string): string {
    const { user, password } = parseCreds(creds);
    return `Basic ${Buffer.from(`${user}:${password}`).toString("base64")}`;
}

function numeric(v: unknown, fallback = 0): number {
    const n = typeof v === "number" ? v : Number(v);
    if (!Number.isFinite(n) || Number.isNaN(n)) {
        return fallback;
    }
    return n;
}

async function queryProm(query: string): Promise<number> {
    const url = `${GRAFANA_URL}/api/datasources/proxy/uid/prometheus/api/v1/query?query=${encodeURIComponent(query)}`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    try {
        const response = await fetch(url, {
            headers: { Authorization: basicAuth(GRAFANA_CREDS) },
            signal: controller.signal,
        });
        if (!response.ok) {
            return 0;
        }
        const data = await response.json();
        if (data?.status !== "success") {
            return 0;
        }
        const results = data?.data?.result;
        if (!Array.isArray(results) || results.length === 0) {
            return 0;
        }
        const bad = new Set(["NaN", "Inf", "+Inf", "-Inf"]);
        for (const item of results) {
            const value = item?.value?.[1];
            if (typeof value === "string" && !bad.has(value)) {
                return numeric(value, 0);
            }
        }
        return 0;
    } catch {
        return 0;
    } finally {
        clearTimeout(timeout);
    }
}

function resolveTestScriptPath(input: string): string {
    const trimmed = input.trim();
    if (!trimmed) {
        return "";
    }

    const asAbsolute = resolve(trimmed);
    if (existsSync(asAbsolute)) {
        return asAbsolute;
    }

    const scriptRelative = resolve(SCRIPT_DIR, trimmed);
    if (existsSync(scriptRelative)) {
        return scriptRelative;
    }

    const repoRelative = resolve(REPO_ROOT, trimmed);
    if (existsSync(repoRelative)) {
        return repoRelative;
    }

    if (trimmed.endsWith(".ts")) {
        return resolve(REPO_ROOT, "dist", "api-tests", `${basename(trimmed, ".ts")}.js`);
    }

    if (trimmed.endsWith(".js")) {
        return resolve(trimmed);
    }

    return resolve(REPO_ROOT, "dist", "api-tests", `${trimmed}.js`);
}

async function ensureK6TestScripts(): Promise<void> {
    const inputs = TEST_FILES_RAW.split(",").map((s) => s.trim()).filter(Boolean);
    K6_TEST_SCRIPTS = inputs.map(resolveTestScriptPath).filter(Boolean);
    if (K6_TEST_SCRIPTS.length === 0) {
        throw new Error("No valid test files specified via --tests/K6_TEST_FILES");
    }

    const missing = K6_TEST_SCRIPTS.filter((f) => !existsSync(f));
    if (missing.length > 0) {
        log("  One or more compiled k6 scripts missing; building project test artifacts...");
        const build = await runCommand("npm", ["run", "build-test"], REPO_ROOT);
        writeFileSync(BUILD_LOG, `${build.stdout}\n${build.stderr}`);
        if (build.code !== 0) {
            throw new Error(`Failed to build k6 scripts. Build log: ${BUILD_LOG}`);
        }
    }

    for (const script of K6_TEST_SCRIPTS) {
        if (!existsSync(script)) {
            throw new Error(`k6 script not found: ${script}. Build log: ${BUILD_LOG}`);
        }
    }
    log(`  ✓ Using k6 test scripts: ${K6_TEST_SCRIPTS.join(" ")}`);
}

async function checkPrereqs(): Promise<{ compose: { command: string; argsPrefix: string[] } }> {
    log("Checking prerequisites...");
    if (!(await hasCommand("node", ["-v"]))) {
        throw new Error("Node.js not found. Install Node.js 18+");
    }
    const nodeVersion = await runCommand("node", ["-v"]);
    const major = numeric((nodeVersion.stdout.trim().replace(/^v/, "").split(".")[0] ?? "0"), 0);
    if (major < 18) {
        throw new Error(`Node.js 18+ required (found ${nodeVersion.stdout.trim()})`);
    }
    if (!(await hasCommand("docker", ["--version"]))) {
        throw new Error("Docker not found");
    }
    if (!(await hasCommand("redis-cli", ["--version"]))) {
        log("WARNING: redis-cli not found (needed for FLUSHALL)");
    }

    const compose = await detectComposeCommand();
    await ensureK6TestScripts();
    log("  ✓ Prerequisites OK");
    return { compose };
}

async function startStack(compose: { command: string; argsPrefix: string[] }): Promise<void> {
    log("Starting Docker Compose stack...");
    let result = await composeRun(compose, ["-f", "app-compose.yaml", "up", "-d"]);

    if (result.code !== 0 && /incorrect label com\.docker\.compose\.network/.test(result.stdout + result.stderr)) {
        log(`  Found stale Docker network '${DOCKER_NETWORK}'; recreating it for compose`);
        await runCommand("docker", ["network", "rm", DOCKER_NETWORK]);
        result = await composeRun(compose, ["-f", "app-compose.yaml", "up", "-d"]);
    }

    if (result.code !== 0) {
        throw new Error(`Failed to start Docker Compose stack:\n${result.stdout}\n${result.stderr}`);
    }

    if (result.stdout.trim()) {
        console.log(result.stdout.trim());
    }
    await sleep(5000);
    log("  ✓ Stack started");
}

async function isStackRunning(): Promise<boolean> {
    const ps = await runCommand("docker", ["ps", "--format", "{{.Names}}"]);
    if (ps.code !== 0) {
        return false;
    }
    return /(^|\n).*rest-wrapper.*($|\n)/.test(ps.stdout)
        || /(^|\n).*otel.*($|\n)/.test(ps.stdout)
        || /(^|\n).*redis-meta.*($|\n)/.test(ps.stdout)
        || /(^|\n).*redis-data.*($|\n)/.test(ps.stdout);
}

async function ensureStackRunning(compose: { command: string; argsPrefix: string[] }): Promise<void> {
    if (await isStackRunning()) {
        log("  ✓ App stack already running");
        return;
    }
    log("  App stack not detected; auto-starting app-compose.yaml");
    await startStack(compose);
    for (let i = 0; i < 30; i++) {
        if (await isStackRunning()) {
            log("  ✓ App stack is now running");
            return;
        }
        await sleep(2000);
    }
    throw new Error("App stack did not become ready in time");
}

async function flushRedis(): Promise<void> {
    const local = await runCommand("redis-cli", ["-h", "127.0.0.1", "FLUSHALL"]);
    if (local.code === 0) {
        return;
    }
    await runCommand("docker", ["exec", "api-tests-redis-meta-1", "redis-cli", "FLUSHALL"]);
}

async function resolveDockerNetwork(): Promise<void> {
    const inspect = await runCommand("docker", ["network", "inspect", DOCKER_NETWORK]);
    if (inspect.code === 0) {
        return;
    }

    for (const candidate of ["api-tests-rest-wrapper-1", "rest-wrapper"]) {
        const exists = await runCommand("docker", ["inspect", candidate]);
        if (exists.code !== 0) {
            continue;
        }
        const net = await runCommand("docker", ["inspect", "-f", "{{range $k, $v := .NetworkSettings.Networks}}{{println $k}}{{end}}", candidate]);
        const detected = net.stdout.split(/\r?\n/).map((s) => s.trim()).find(Boolean);
        if (detected) {
            DOCKER_NETWORK = detected;
            log(`  Using detected Docker network: ${DOCKER_NETWORK}`);
            return;
        }
    }

    log(`  Docker network '${DOCKER_NETWORK}' not found; creating it`);
    const create = await runCommand("docker", ["network", "create", DOCKER_NETWORK]);
    if (create.code !== 0) {
        throw new Error(`Failed to create Docker network '${DOCKER_NETWORK}'`);
    }
}

async function ensureRestWrapperOnNetwork(compose: { command: string; argsPrefix: string[] }): Promise<void> {
    const check = await runCommand("docker", ["network", "inspect", DOCKER_NETWORK, "--format", "{{json .Containers}}"]);
    if (check.code === 0 && /api-tests-rest-wrapper-1|"rest-wrapper"/.test(check.stdout)) {
        return;
    }

    log(`  rest-wrapper not attached to '${DOCKER_NETWORK}'; recovering service network`);
    const recover = await composeRun(compose, ["-f", "app-compose.yaml", "up", "-d", "rest-wrapper"]);
    writeFileSync(RECOVER_LOG, `${recover.stdout}\n${recover.stderr}`);
    if (recover.code !== 0) {
        throw new Error(`Failed to recover rest-wrapper network attachment. Recovery log: ${RECOVER_LOG}`);
    }

    for (let i = 0; i < 20; i++) {
        const recheck = await runCommand("docker", ["network", "inspect", DOCKER_NETWORK, "--format", "{{json .Containers}}"]);
        if (recheck.code === 0 && /api-tests-rest-wrapper-1|"rest-wrapper"/.test(recheck.stdout)) {
            log(`  ✓ rest-wrapper attached to '${DOCKER_NETWORK}'`);
            return;
        }
        await sleep(1000);
    }

    throw new Error(`rest-wrapper is still not attached to '${DOCKER_NETWORK}'. Recovery log: ${RECOVER_LOG}`);
}

async function startK6(vus: number): Promise<void> {
    log(`  Starting k6 (VUs=${vus}, tests=${K6_TEST_SCRIPTS.length})...`);
    K6_CONTAINERS = [];

    for (let idx = 0; idx < K6_TEST_SCRIPTS.length; idx++) {
        const script = K6_TEST_SCRIPTS[idx];
        const containerName = `k6-saturation-${Date.now()}-${Math.floor(Math.random() * 10000)}-${idx}`;
        const run = await runCommand("docker", [
            "run",
            "-d",
            "--name",
            containerName,
            "--network",
            DOCKER_NETWORK,
            "-v",
            `${script}:/scripts/test.js:ro`,
            "-e",
            "TEST_URL=http://rest-wrapper:8080",
            "-e",
            `SCRIPT_VUS=${vus}`,
            "-e",
            "SCRIPT_ITERATIONS=999999",
            "-e",
            "SCRIPT_MAX_DURATION=20m",
            "-e",
            "SLEEP_DURATION=0",
            "-e",
            "K6_OTEL_GRPC_EXPORTER_INSECURE=true",
            "-e",
            "K6_OTEL_METRIC_PREFIX=k6_",
            "-e",
            "K6_OTEL_GRPC_EXPORTER_ENDPOINT=otel:4317",
            K6_IMAGE,
            "run",
            "--out",
            "opentelemetry",
            "/scripts/test.js",
        ]);

        if (run.code !== 0) {
            await stopK6();
            throw new Error(`Failed to start k6 container for script '${script}' on network '${DOCKER_NETWORK}'`);
        }
        K6_CONTAINERS.push(containerName);
    }
}

async function stopK6(): Promise<void> {
    log("  Stopping k6...");
    for (const container of K6_CONTAINERS) {
        await runCommand("docker", ["stop", container]);
        await runCommand("docker", ["rm", "-f", container]);
    }
    K6_CONTAINERS = [];
}

async function sampleMetrics(vus: number): Promise<void> {
    const win = `${SETTLE_SECS}s`;
    log(`  Sampling metrics (${win} window)...`);

    const rps = numeric(await queryProm(`sum(rate(http_server_duration_milliseconds_count{service_name=\"rest-wrapper\"}[${win}]))`));
    const respSum = numeric(await queryProm(`sum(rate(k6_http_req_duration_milliseconds_sum{service_name=\"k6\"}[${win}]))`));
    const respCount = numeric(await queryProm(`sum(rate(k6_http_req_duration_milliseconds_count{service_name=\"k6\"}[${win}]))`));
    const respVal = respCount > 0 ? respSum / respCount : 0;

    const cpu = numeric(await queryProm(`avg_over_time(sum(process_cpu_utilization{service_name=\"rest-wrapper\",process_cpu_state=~\"user|system\"})[${win}:])`));
    const evloop = numeric(await queryProm(`avg_over_time(nodejs_eventloop_utilization_ratio{service_name=\"rest-wrapper\"}[${win}])`));
    const evdelay = numeric(await queryProm(`avg_over_time(nodejs_eventloop_delay_p90_seconds{service_name=\"rest-wrapper\"}[${win}])`));
    const checksRaw = numeric(await queryProm(`sum(rate(k6_checks_total{service_name=\"k6\",condition=\"pass\"}[${win}])) / sum(rate(k6_checks_total{service_name=\"k6\"}[${win}]))`), 1);
    const checks = Number.isFinite(checksRaw) && !Number.isNaN(checksRaw) ? checksRaw : 1;
    const redisOps = numeric(await queryProm(`sum(rate(redis_commands_processed_total[${win}]))`));

    log(
        `  ✓ VUs=${vus} | RPS=${rps.toFixed(2)} | Resp=${Math.round(respVal)}ms | CPU=${(cpu * 100).toFixed(1)}% | EL=${(evloop * 100).toFixed(1)}% | Checks=${(checks * 100).toFixed(2)}% | Redis=${Math.round(redisOps)}ops/s`,
    );

    const line = [
        vus,
        Number.isFinite(rps) ? rps : 0,
        Number.isFinite(respVal) ? respVal : 0,
        Number.isFinite(cpu) ? cpu : 0,
        Number.isFinite(evloop) ? evloop : 0,
        Number.isFinite(evdelay) ? evdelay : 0,
        Number.isFinite(checks) ? checks : 1,
        Number.isFinite(redisOps) ? redisOps : 0,
    ].join(",");

    writeFileSync(RESULTS_CSV, `${readFileSync(RESULTS_CSV, "utf-8").trimEnd()}\n${line}\n`);
}

function parseRows(csvPath: string): Row[] {
    const csv = readFileSync(csvPath, "utf-8");
    const lines = csv.trim().split(/\r?\n/);
    return lines.slice(1).map((line) => {
        const vals = line.split(",");
        return {
            vus: Math.max(0, numeric(vals[0], 0)),
            rps: Math.max(0, numeric(vals[1], 0)),
            resp: Math.max(0, numeric(vals[2], 0)),
            cpu: Math.max(0, Math.min(1, numeric(vals[3], 0))),
            evloop: Math.max(0, Math.min(1, numeric(vals[4], 0))),
            evdelay: Math.max(0, numeric(vals[5], 0)),
            checks: Math.max(0, Math.min(1, numeric(vals[6], 1))),
            redis_ops: Math.max(0, numeric(vals[7], 0)),
        };
    }).filter((r) => r.vus > 0);
}

function formatCell(value: string | number, width: number, align: "left" | "right"): string {
    const s = String(value);
    return align === "right" ? s.padStart(width) : s.padEnd(width);
}

function analyzeAndPrint(csvPath: string): { maxVu: number } {
    log("Analyzing results...");
    const rows = parseRows(csvPath);
    if (rows.length === 0) {
        throw new Error("No valid data in results CSV");
    }

    let saturationVu: number | null = null;
    let saturationReason: string[] = [];
    let maxRps = 0;

    for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        maxRps = Math.max(maxRps, r.rps);
        const signals: string[] = [];

        if (r.cpu >= THR_CPU) signals.push("CPU");
        if (r.evloop >= THR_EVLOOP) signals.push("EventLoop");
        if (r.evdelay >= THR_EVDELAY_S) signals.push("ELDelay");
        if (r.resp >= THR_RESP_MS) signals.push("Latency");
        if (r.checks < THR_CHECK_RATE && r.checks > 0) signals.push("Checks");

        if (i > 0) {
            const prev = rows[i - 1].rps;
            const growth = prev > 0 ? (r.rps - prev) / prev : 0;
            if (growth < THR_RPS_GROWTH && growth >= 0) {
                signals.push("RPSStall");
            }
        }

        if (signals.length > 0 && saturationVu === null) {
            saturationVu = r.vus;
            saturationReason = signals;
        }
    }

    console.log("");
    const columns = [
        { header: "VUs", width: 6, align: "right" as const },
        { header: "RPS", width: 10, align: "right" as const },
        { header: "Resp(ms)", width: 10, align: "right" as const },
        { header: "CPU(%)", width: 8, align: "right" as const },
        { header: "EL(%)", width: 8, align: "right" as const },
        { header: "Delay(ms)", width: 10, align: "right" as const },
        { header: "Checks(%)", width: 10, align: "right" as const },
        { header: "Redis(ops/s)", width: 14, align: "right" as const },
    ];
    const separator = `+${columns.map((c) => "-".repeat(c.width + 2)).join("+")}+`;
    console.log(separator);
    console.log(`| ${columns.map((c) => formatCell(c.header, c.width, "left")).join(" | ")} |`);
    console.log(separator);
    for (const r of rows) {
        const values = [
            Math.round(r.vus),
            r.rps.toFixed(2),
            Math.round(r.resp),
            (r.cpu * 100).toFixed(1),
            (r.evloop * 100).toFixed(1),
            (r.evdelay * 1000).toFixed(1),
            (r.checks * 100).toFixed(2),
            Math.round(r.redis_ops),
        ];
        console.log(`| ${values.map((v, i) => formatCell(v, columns[i].width, columns[i].align)).join(" | ")} |`);
    }
    console.log(separator);
    console.log("");

    if (saturationVu === null) {
        const maxVus = Math.round(rows[rows.length - 1].vus);
        console.log(`NO saturation detected at any tested VU level (max: ${maxVus} VUs).`);
        console.log(`Highest measured RPS: ${maxRps.toFixed(2)} req/s at ${maxVus} VUs.`);
        console.log("Consider re-running with higher VU counts.");
    } else {
        const safeVu = Math.max(1, Math.round(saturationVu / 2));
        console.log(`SATURATION starting at: ${Math.round(saturationVu)} VUs`);
        console.log(`Signals: ${saturationReason.join(", ")}`);
        console.log(`Recommend safe limit: ${safeVu} VUs (50% of saturation point)`);
        console.log("");
        console.log("Ratios at saturation:");
        const satRow = rows.find((r) => r.vus === saturationVu);
        if (satRow) {
            console.log(`  CPU: ${(satRow.cpu * 100).toFixed(1)}%`);
            console.log(`  Event Loop: ${(satRow.evloop * 100).toFixed(1)}%`);
            console.log(`  Response Time: ${Math.round(satRow.resp)}ms`);
            console.log(`  Redis ops/s: ${Math.round(satRow.redis_ops)}`);
        }
    }
    console.log("");

    return { maxVu: rows[Math.max(0, rows.length - 1)].vus };
}

function parseArgs(argv: string[]): CliArgs {
    const vusArray: number[] = [];
    let testsRaw: string | null = null;
    let showHelp = false;

    for (let i = 0; i < argv.length; i++) {
        const arg = argv[i];
        if (arg === "-h" || arg === "--help") {
            showHelp = true;
            continue;
        }
        if (arg === "--tests") {
            testsRaw = argv[i + 1] ?? "";
            i++;
            continue;
        }
        if (arg.startsWith("--tests=")) {
            testsRaw = arg.slice("--tests=".length);
            continue;
        }
        const n = Number(arg);
        if (Number.isFinite(n) && n > 0) {
            vusArray.push(Math.floor(n));
        }
    }

    return { vusArray, testsRaw, showHelp };
}

async function main(): Promise<void> {
    const { vusArray, testsRaw, showHelp } = parseArgs(process.argv.slice(2));
    if (showHelp) {
        printHelp();
        return;
    }
    if (testsRaw != null) {
        TEST_FILES_RAW = testsRaw;
    }

    const effectiveVus = vusArray.length > 0 ? vusArray : [10, 25, 50, 100, 200, 400];

    hr();
    log("rest-wrapper Saturation Finder");
    hr();

    const { compose } = await checkPrereqs();

    if (START_STACK) {
        await startStack(compose);
    } else {
        await ensureStackRunning(compose);
    }

    await resolveDockerNetwork();
    await ensureRestWrapperOnNetwork(compose);

    writeFileSync(RESULTS_CSV, "vus,rps,resp_ms,cpu,evloop,evdelay_s,checks,redis_ops\n");

    log("");
    log("Saturation Test Parameters:");
    log(`  Thresholds: CPU=${THR_CPU} EL=${THR_EVLOOP} Delay=${THR_EVDELAY_S}s Resp=${THR_RESP_MS}ms Checks=${THR_CHECK_RATE}`);
    log(`  Test files: ${K6_TEST_SCRIPTS.join(" ")}`);
    log(`  Warmup: ${WARMUP_SECS}s, Settle: ${SETTLE_SECS}s`);
    log(`  VU Sweep: ${effectiveVus.join(" ")}`);
    log("");

    try {
        for (const vus of effectiveVus) {
            log(`Testing at ${vus} VUs...`);
            await flushRedis();
            await startK6(vus);
            log(`  Warmup for ${WARMUP_SECS}s...`);
            await sleep(WARMUP_SECS * 1000);
            await sampleMetrics(vus);
            await stopK6();
            log("  Cooldown 15s...");
            await sleep(15000);
            log("");
        }
    } finally {
        if (K6_CONTAINERS.length > 0) {
            await stopK6();
        }
    }

    const { maxVu } = analyzeAndPrint(RESULTS_CSV);
    if (maxVu > 0) {
        log(`Rerun with higher VUs: node ./dist/api-tests/find-saturation.js ${maxVu * 2} ${maxVu * 4}`);
    }
    hr();
}

main()
    .catch((error) => {
        console.error(`ERROR: ${(error as Error).message}`);
        process.exitCode = 1;
    })
    .finally(() => {
        try {
            rmSync(runTempDir, { recursive: true, force: true });
        } catch {
            // Ignore cleanup failures.
        }
    });