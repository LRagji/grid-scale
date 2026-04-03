import http from 'k6/http';
import { sleep, check } from 'k6';
import { Counter } from 'k6/metrics';

const gsWrittenSampleCounter = new Counter('gs_samples_written');
const gsReadSampleCounter = new Counter('gs_samples_read');
const gsTagIngestionCounter = new Counter('gs_tags_ingested');

interface IUpsertResponse {
    [key: string]: unknown;
}

interface IApiSample {
    tag: string;
    ts: number;
    pld: {
        nV: number;
        [key: string]: unknown;
    };
}

interface IQueryResponse {
    samples: IApiSample[],
    diagnostics: { [key: string]: unknown }
}

function fromEnvOrDefault(value: string | undefined, fallback: string): string {
    if (!value) {
        return fallback;
    }
    return value;
}

export const options = {
    scenarios: {
        sequential_scenario: {
            executor: 'per-vu-iterations',
            vus: Number(fromEnvOrDefault(__ENV.SCRIPT_VUS, '5')),
            iterations: Number(fromEnvOrDefault(__ENV.SCRIPT_ITERATIONS, '5')),
            maxDuration: fromEnvOrDefault(__ENV.SCRIPT_MAX_DURATION, '30m')
        }
    }
};

export function setup(): [string, unknown][] {
    const identity = `${fromEnvOrDefault(__ENV.IDENTITY_PREFIX, 'bulk-time')}-${Date.now()}-${Math.random()}`;
    const tagName = `${identity}-tag-perf-test-`;
    console.log(`Setup called for ${identity}`);
    return [
        ["id", identity],
        ["baseURL", fromEnvOrDefault(__ENV.TEST_URL, 'http://localhost:8080')],
        ["startTime", 0],
        ["tagName", tagName],
        ["sleepDuration", fromEnvOrDefault(__ENV.SLEEP_DURATION, '1')],
        ["bulk", fromEnvOrDefault(__ENV.BULK, '100')]
    ];
}

export default function (setupData: [string, unknown][]) {
    const context = new Map<string, unknown>(setupData);
    context.set("tagName", context.get("tagName") as string + __VU);
    const upsertURL = context.get("baseURL") + "/v1/series/upsert";
    const tagName = context.get("tagName");
    const startTime = context.get("startTime") as number;
    const currentIteration = __ITER;
    const bulkSize = Number(context.get("bulk") as string);
    const baseComputedTime = startTime + (currentIteration * bulkSize);
    const headers = { 'Content-Type': 'application/json' };
    const upsertPayload = [];

    for (let i = 0; i < bulkSize; i++) {
        const computedTime = baseComputedTime + i;
        const computedCurrentValue = Math.min(Math.random() * 100, computedTime);
        upsertPayload.push({
            "tag": tagName,
            "ts": computedTime,
            "pld": {
                "nV": computedCurrentValue
            }
        });
    }
    //Write & Check status
    const upsertResponse = http.put(upsertURL, JSON.stringify(upsertPayload), { headers }) as unknown as IUpsertResponse;
    check(upsertResponse, { "Upsert should return status is 201": (res) => res.status === 201 });

    //Read Cumulative & Check length and random sample value
    const computedEndTime = baseComputedTime + bulkSize;
    const queryURL = context.get("baseURL") + "/v1/series/fetch";
    const requestPayload = {
        query: {
            operator: "AND",
            conditions: [
                { dimension: "tag", operator: "in", value: [tagName] },
                { dimension: "time", operator: "between", value: [startTime, computedEndTime - 1] }
            ]
        }
    };
    const queryResponse = http.post(queryURL, JSON.stringify(requestPayload), { headers });
    const queryResponseBody = queryResponse.json() as unknown as IQueryResponse;
    const maxSamplesPerRequest = 1000;

    if (queryResponseBody.samples.length !== Math.min(computedEndTime - startTime, maxSamplesPerRequest)) {
        console.error(`Unexpected number of samples returned. Expected: ${Math.min(computedEndTime - startTime, maxSamplesPerRequest)}, Actual: ${queryResponseBody.samples.length}`);
        console.error(`Request Payload: ${JSON.stringify(requestPayload)}`);
    }

    check(queryResponse, {
        "Query should return status is 200 or 206": (res) => (res.status === 200 || res.status === 206),
        "Query should return elapsed time data points": (res) => queryResponseBody.samples.length === Math.min(computedEndTime - startTime, maxSamplesPerRequest),
        "Query should return correct tag in data points": (res) => queryResponseBody.samples.every(sample => sample.tag === tagName),
        "Query should return correct time range in data points": (res) => queryResponseBody.samples.every(sample => sample.ts >= startTime && sample.ts <= computedEndTime),
        "Query should return correct nV value in data points": (res) => queryResponseBody.samples.every(sample => sample.pld.nV <= computedEndTime && sample.pld.nV >= 0)
    });

    gsWrittenSampleCounter.add(upsertPayload.length);
    gsReadSampleCounter.add(queryResponseBody.samples.length);
    gsTagIngestionCounter.add(1);

    sleep(Number(context.get("sleepDuration") as string));
}

export function teardown(setupData: [string, unknown][]) {
    const context = new Map<string, unknown>(setupData);
    console.log(`Teardown called for ${context.get("id")}`);
}
