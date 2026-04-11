import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

type CommandResult = {
    code: number;
    stdout: string;
    stderr: string;
};

const __filename = fileURLToPath(import.meta.url);
const SCRIPT_DIR = dirname(__filename);

function findRepoRoot(startDir: string): string {
    let currentDir = resolve(startDir);
    while (true) {
        if (existsSync(join(currentDir, "package.json"))) {
            return currentDir;
        }
        const parentDir = dirname(currentDir);
        if (parentDir === currentDir) {
            throw new Error(`Unable to locate package.json from ${startDir}`);
        }
        currentDir = parentDir;
    }
}

const REPO_ROOT = findRepoRoot(process.cwd());
const APP_COMPOSE_FILE = resolve(REPO_ROOT, "api-tests", "app-compose.yaml");

const GRAFANA_URL = process.env.GRAFANA_URL ?? "http://localhost:3000";
const GRAFANA_CREDS = process.env.GRAFANA_CREDS ?? "admin:admin";

const [grafanaUser, grafanaPassword] = GRAFANA_CREDS.split(":");

function printHelp(): void {
    console.log("Saturation Diagnostics (TypeScript)");
    console.log("");
    console.log("Usage:");
    console.log("  node ./dist/api-tests/diagnose-saturation.js");
    console.log("  npm run saturation:diagnose");
    console.log("  npm run saturation:diagnose:help");
    console.log("");
    console.log("Options:");
    console.log("  -h, --help        Show this help message");
    console.log("");
    console.log("Checks performed:");
    console.log("  1. Grafana connectivity and Prometheus datasource");
    console.log("  2. Key Prometheus metric availability");
    console.log("  3. service_name label values");
    console.log("  4. Docker compose service status");
    console.log("  5. Sample metric query output");
    console.log("");
    console.log("Environment variables:");
    console.log("  GRAFANA_URL        Default: http://localhost:3000");
    console.log("  GRAFANA_CREDS      Default: admin:admin");
}

function log(message: string): void {
    const now = new Date();
    const time = now.toTimeString().slice(0, 8);
    console.log(`[${time}] ${message}`);
}

async function runCommand(command: string, args: string[]): Promise<CommandResult> {
    return await new Promise((resolve) => {
        const child = spawn(command, args, { cwd: REPO_ROOT, stdio: ["ignore", "pipe", "pipe"] });
        let stdout = "";
        let stderr = "";

        child.stdout.on("data", (chunk) => {
            stdout += chunk.toString();
        });

        child.stderr.on("data", (chunk) => {
            stderr += chunk.toString();
        });

        child.on("error", () => {
            resolve({ code: 127, stdout, stderr: `${stderr}\ncommand not found: ${command}`.trim() });
        });

        child.on("close", (code) => {
            resolve({ code: code ?? 1, stdout, stderr });
        });
    });
}

async function detectComposeCommand(): Promise<{ command: string; argsPrefix: string[] } | null> {
    const dockerCompose = await runCommand("docker", ["compose", "version"]);
    if (dockerCompose.code === 0) {
        return { command: "docker", argsPrefix: ["compose"] };
    }
    const legacyCompose = await runCommand("docker-compose", ["version"]);
    if (legacyCompose.code === 0) {
        return { command: "docker-compose", argsPrefix: [] };
    }
    return null;
}

function basicAuthHeader(user: string, password: string): string {
    return `Basic ${Buffer.from(`${user}:${password}`).toString("base64")}`;
}

async function fetchJson(url: string, timeoutMs = 5000): Promise<any> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
        const response = await fetch(url, {
            headers: { Authorization: basicAuthHeader(grafanaUser, grafanaPassword) },
            signal: controller.signal,
        });
        const text = await response.text();
        let json: any = {};
        try {
            json = text ? JSON.parse(text) : {};
        } catch {
            json = { raw: text };
        }
        return { ok: response.ok, status: response.status, body: json };
    } finally {
        clearTimeout(timer);
    }
}

async function querySimple(query: string): Promise<string> {
    const url = `${GRAFANA_URL}/api/datasources/proxy/uid/prometheus/api/v1/query?query=${encodeURIComponent(query)}`;
    try {
        const response = await fetchJson(url, 5000);
        const results = response.body?.data?.result ?? [];
        return String(Array.isArray(results) ? results.length : 0);
    } catch (error) {
        return `ERROR (${(error as Error).message})`;
    }
}

async function main(): Promise<void> {
    if (process.argv.includes("-h") || process.argv.includes("--help")) {
        printHelp();
        return;
    }

    log("Test 1: Grafana Connectivity");
    const datasourceUrl = `${GRAFANA_URL}/api/datasources`;
    const datasourceResponse = await fetchJson(datasourceUrl, 8000);

    if (datasourceResponse.status === 200) {
        log("  ✓ Grafana reachable (HTTP 200)");
        const bodyText = JSON.stringify(datasourceResponse.body);
        if (bodyText.includes("prometheus")) {
            log("  ✓ Prometheus datasource found");
        } else {
            log("  ✗ Prometheus datasource NOT found");
            log("    Available datasources:");
            const names = Array.isArray(datasourceResponse.body)
                ? datasourceResponse.body.map((d: any) => d?.name).filter(Boolean)
                : [];
            if (names.length === 0) {
                log("    (none)");
            } else {
                for (const name of names) {
                    log(`    - ${name}`);
                }
            }
        }
    } else {
        log(`  ✗ Grafana returned HTTP ${datasourceResponse.status}`);
        log(`    Check: GRAFANA_URL=${GRAFANA_URL}, GRAFANA_CREDS=${GRAFANA_CREDS}`);
        process.exit(1);
    }

    log("");
    log("Test 2: Available Metrics in Prometheus");
    const metrics = [
        "http_server_duration_milliseconds_count",
        "k6_http_req_duration_milliseconds_sum",
        "process_cpu_utilization",
        "nodejs_eventloop_utilization_ratio",
        "k6_checks_total",
    ];

    for (const metric of metrics) {
        const count = await querySimple(`count(${metric})`);
        log(`  ${metric}: ${count} series`);
    }

    log("");
    log("Test 3: Available Service Names");
    const labelsUrl = `${GRAFANA_URL}/api/datasources/proxy/uid/prometheus/api/v1/label/service_name/values`;
    const labelsResponse = await fetchJson(labelsUrl, 8000);
    const values = labelsResponse.body?.data;
    if (!Array.isArray(values) || values.length === 0) {
        console.log("  (no service_name labels found)");
    } else {
        for (const value of values) {
            console.log(`  - ${value}`);
        }
    }

    log("");
    log("Test 4: Docker Services Status");
    const compose = await detectComposeCommand();
    if (!compose) {
        log("  (docker compose/docker-compose not available)");
    } else {
        const ps = await runCommand(compose.command, [...compose.argsPrefix, "-f", APP_COMPOSE_FILE, "ps"]);
        if (ps.code === 0) {
            process.stdout.write(ps.stdout);
        } else {
            log("  (unable to run compose ps)");
        }
    }

    log("");
    log("Test 5: Sample Metric Query (last 30s)");
    const sampleQuery = "sum(rate(http_server_duration_milliseconds_count[30s]))";
    const sampleUrl = `${GRAFANA_URL}/api/datasources/proxy/uid/prometheus/api/v1/query?query=${encodeURIComponent(sampleQuery)}`;
    const sampleResponse = await fetchJson(sampleUrl, 8000);
    console.log("Response status:", sampleResponse.body?.status ?? "unknown");
    const sampleResults = sampleResponse.body?.data?.result ?? [];
    if (!Array.isArray(sampleResults) || sampleResults.length === 0) {
        console.log("Result: NO DATA (metric not found or no recent data)");
    } else {
        for (const result of sampleResults) {
            const value = result?.value?.[1] ?? "null";
            console.log("Result:", value, "Labels:", JSON.stringify(result?.metric ?? {}));
        }
    }

    log("");
    log("═══════════════════════════════════════════════════════════════════════════════");
    log("Diagnostics complete. Check output above for issues.");
    log("");
    log("Common fixes:");
    log("  1. Ensure Docker stack is running: docker compose -f api-tests/app-compose.yaml ps");
    log("  2. Ensure rest-wrapper is exporting metrics: docker logs rest-wrapper");
    log("  3. Check Grafana http://localhost:3000 (admin:admin)");
    log("  4. Verify Prometheus http://localhost:3000/api/datasources/proxy/uid/prometheus");
}

main().catch((error) => {
    console.error(`Fatal error: ${(error as Error).message}`);
    process.exit(1);
});