import http from 'k6/http';
import { sleep, check } from 'k6';
import { ISample } from '../src';

interface IUpsertResponse {
    [key: string]: unknown;
}

interface IQueryResponse {
    samples: ISample[],
    diagnostics: { [key: string]: unknown }
}

export const options = {
    vus: 10,
    duration: "30m",
};

export function setup(): [string, unknown][] {
    const identity = `${Date.now()}-${Math.random()}`;
    console.log(`Setup called for ${identity}`);
    return [
        ["id", identity],
        ["baseURL", __ENV.TEST_URL ?? 'http://localhost:8080'],
        ["startTime", 0],
        ["tagName", `${identity}-tag-perf-test-${__VU}`]
    ];
}

export default function (setupData: [string, unknown][]) {
    const context = new Map<string, unknown>(setupData);
    const upsertURL = context.get("baseURL") + "/v1/series/upsert";
    const tagName = context.get("tagName");
    const startTime = context.get("startTime") as number;
    const currentIteration = __ITER;
    const computedTime = startTime + currentIteration;
    const computedCurrentValue = Math.min(Math.random() * 100, computedTime);
    const headers = { 'Content-Type': 'application/json' };
    const upsertPayload = [
        {
            "tag": tagName,
            "ts": computedTime,
            "pld": {
                "nV": computedCurrentValue
            }
        }
    ]
    //Write & Check status
    const upsertResponse = http.put(upsertURL, JSON.stringify(upsertPayload), { headers }) as unknown as IUpsertResponse;
    check(upsertResponse, { "Upsert should return status is 201": (res) => res.status === 201 });

    //Read Cumulative & Check length and random sample value
    const queryURL = context.get("baseURL") + "/v1/series/fetch";
    const queryResponse = http.post(queryURL, JSON.stringify({
        "tagsFilter": {
            "in": [tagName]
        },
        "timeFilter": {
            "startInclusiveTime": startTime,
            "endExclusiveTime": computedTime + 1
        }
    }), { headers });
    const queryResponseBody = queryResponse.json() as unknown as IQueryResponse;
    const maxSamplesPerRequest = 1000;
    check(queryResponse, {
        "Query should return status is 200 or 206": (res) => (res.status === 200 || res.status === 206),
        "Query should return elapsed time data points": (res) => queryResponseBody.samples.length === Math.min(currentIteration + 1, maxSamplesPerRequest),
        "Query should return correct tag in data points": (res) => queryResponseBody.samples.every(sample => sample.tag === tagName),
        "Query should return correct time range in data points": (res) => queryResponseBody.samples.every(sample => sample.ts >= startTime && sample.ts <= computedTime),
        "Query should return correct nV value in data points": (res) => queryResponseBody.samples.every(sample => sample.pld.nV <= computedTime)
    });

    sleep(1);
}

export function teardown(setupData: [string, unknown][]) {
    const context = new Map<string, unknown>(setupData);
    console.log(`Teardown called for ${context.get("id")}`);
}
