import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { IDimensionalElement } from "../src/interfaces/i-dimensional-element.js";
import { IDimensionalQuery } from "../src/interfaces/i-dimensional-query.js";
import { IMetadata, IPolicy, IQContainer, IQContainerFactory } from "../src/interfaces/i-q-acc.js";
import { InMemoryTimeseriesQContainer } from "../src/cascading-data-containers/in-memory-timeseries-q-container.js";
import { LocalQAccumulator } from "../src/cascading-data-containers/local-q-accumulator.js";
import { PolicyEvaluator } from "../src/cascading-data-containers/policy-evaluator.js";

// Buffer large enough for tests: 64 KB
const DEFAULT_BUFFER_SIZE = 65_536;

function makeContainerMetadata(bufferSizeInBytes: number = DEFAULT_BUFFER_SIZE): IMetadata {
    return { bufferSizeInBytes };
}

function makeContainerFactory(): IQContainerFactory {
    const containers = new Map<string, IQContainer>();
    return {
        async createContainer(containerId: string, _meta: IMetadata): Promise<IQContainer> {
            const container = new InMemoryTimeseriesQContainer(containerId);
            containers.set(containerId, container);
            return container;
        },
        async fetchContainer(containerId: string): Promise<IQContainer | null> {
            return containers.get(containerId) ?? null;
        }
    };
}

function makeElement(tag: string, time: number, value: number, hash?: string): IDimensionalElement {
    return {
        globalIdentityHash: hash,
        dim: { tag, time },
        pld: { value }
    };
}

function makeTagTimeRangeQuery(tags: string[], startTime: number, endTime: number): IDimensionalQuery {
    return {
        query: {
            operator: "AND",
            conditions: [
                { dimension: "tag", operator: "in", value: tags },
                { dimension: "time", operator: "between", value: [startTime, endTime] }
            ]
        }
    };
}

function makeCountPolicy(threshold: number): IPolicy {
    return {
        name: "count-policy",
        evaluate: async (meta: IMetadata) => (meta["totalElementsCount"] as number) >= threshold
    };
}

function makeSizePolicy(thresholdBytes: number): IPolicy {
    return {
        name: "size-policy",
        evaluate: async (meta: IMetadata) => (meta["totalElementsSizeBytes"] as number) >= thresholdBytes
    };
}

function makeAgePolicy(maxAgeMs: number): IPolicy {
    return {
        name: "age-policy",
        evaluate: async (meta: IMetadata) => {
            const created = meta["containerCreationTimeInMs"] as number;
            return created > 0 && (Date.now() - created) >= maxAgeMs;
        }
    };
}

async function buildAccumulator(options: {
    bufferSizeInBytes?: number;
    policies?: IPolicy[];
    onFlushed?: (flushReasons: string[], flushedContainerIds: string[]) => Promise<void>;
}): Promise<LocalQAccumulator> {
    const { bufferSizeInBytes = DEFAULT_BUFFER_SIZE, policies = [], onFlushed } = options;

    const factory = makeContainerFactory();
    const evaluator = new PolicyEvaluator();
    for (const policy of policies) {
        evaluator.registerPolicy(policy);
    }

    const acc = new LocalQAccumulator(
        undefined,
        onFlushed ?? (async () => { })
    );
    await acc.initialize(factory, makeContainerMetadata(bufferSizeInBytes), evaluator);
    return acc;
}

describe("LocalQAccumulator + PolicyEvaluator + InMemoryTimeseriesQContainer", () => {

    describe("basic accumulate and query", () => {

        it("returns accumulated elements that match query", async () => {
            const acc = await buildAccumulator({});
            await acc.accumulate([
                makeElement("sensor-A", 100, 1.0),
                makeElement("sensor-B", 200, 2.0)
            ]);

            const results = await acc.query(makeTagTimeRangeQuery(["sensor-A"], 0, 500), 10);
            assert.equal(results.length, 1);
            assert.equal(results[0].dim["tag"], "sensor-A");
        });

        it("returns empty array when no elements match query", async () => {
            const acc = await buildAccumulator({});
            await acc.accumulate([makeElement("sensor-A", 100, 1.0)]);

            const results = await acc.query(makeTagTimeRangeQuery(["sensor-Z"], 0, 500), 10);
            assert.equal(results.length, 0);
        });

        it("deduplicates elements by globalIdentityHash, keeping last seen", async () => {
            const acc = await buildAccumulator({});
            await acc.accumulate([
                makeElement("sensor-A", 100, 1.0, "hash-1"),
                makeElement("sensor-A", 100, 9.9, "hash-1")
            ]);

            const results = await acc.query(makeTagTimeRangeQuery(["sensor-A"], 0, 500), 10);
            assert.equal(results.length, 1);
            assert.equal(results[0].pld.value, 9.9);
        });

        it("returns anonymous elements without deduplication", async () => {
            const acc = await buildAccumulator({});
            await acc.accumulate([
                makeElement("sensor-A", 100, 1.0),
                makeElement("sensor-A", 200, 2.0)
            ]);

            const results = await acc.query(makeTagTimeRangeQuery(["sensor-A"], 0, 500), 10);
            assert.equal(results.length, 2);
        });

        it("respects maxElementsCount cap on query results", async () => {
            const acc = await buildAccumulator({});
            const elements = Array.from({ length: 10 }, (_, i) => makeElement("sensor-A", i, i));
            await acc.accumulate(elements);

            const results = await acc.query(makeTagTimeRangeQuery(["sensor-A"], 0, 100), 3);
            assert.equal(results.length, 3);
        });

        it("throws when maxElementsCount is zero", async () => {
            const acc = await buildAccumulator({});
            await acc.accumulate([makeElement("sensor-A", 100, 1.0)]);
            await assert.rejects(
                () => acc.query(makeTagTimeRangeQuery(["sensor-A"], 0, 500), 0),
                /maxElementsCount must be greater than zero/
            );
        });

        it("handles accumulate with empty array without error", async () => {
            const acc = await buildAccumulator({});
            // accumulate with an empty array must not throw
            await assert.doesNotReject(() => acc.accumulate([]));
        });
    });

    describe("policy-driven flushing", () => {

        it("triggers flush and notifies caller when element count policy is satisfied", async () => {
            const flushedIds: string[] = [];
            const flushReasons: string[][] = [];
            const acc = await buildAccumulator({
                policies: [makeCountPolicy(3)],
                onFlushed: async (reasons, ids) => {
                    flushReasons.push(reasons);
                    flushedIds.push(...ids);
                }
            });

            await acc.accumulate([makeElement("s", 1, 1), makeElement("s", 2, 2), makeElement("s", 3, 3)]);

            assert.equal(flushedIds.length, 1);
            assert.match(flushedIds[0], /^[0-9a-f-]{36}$/); // UUID
            assert.deepEqual(flushReasons, [["count-policy"]]);
        });

        it("resets element count after flush so policy does not fire spuriously", async () => {
            const flushedIds: string[] = [];
            const acc = await buildAccumulator({
                policies: [makeCountPolicy(3)],
                onFlushed: async (_reasons, ids) => { flushedIds.push(...ids); }
            });

            // First batch: triggers flush
            await acc.accumulate([makeElement("s", 1, 1), makeElement("s", 2, 2), makeElement("s", 3, 3)]);
            assert.equal(flushedIds.length, 1);

            // Second batch: below threshold, should not trigger
            await acc.accumulate([makeElement("s", 4, 4)]);
            assert.equal(flushedIds.length, 1);
        });

        it("triggers flush when byte size policy threshold is reached", async () => {
            const flushedIds: string[] = [];
            const flushReasons: string[][] = [];
            // Use a very small byte threshold to ensure it fires
            const acc = await buildAccumulator({
                policies: [makeSizePolicy(50)],
                onFlushed: async (reasons, ids) => {
                    flushReasons.push(reasons);
                    flushedIds.push(...ids);
                }
            });

            await acc.accumulate([makeElement("sensor-A", 100, 1.0)]);
            // A single JSON-serialised element is well over 50 bytes
            assert.equal(flushedIds.length, 1);
            assert.deepEqual(flushReasons, [["size-policy"]]);
        });

        it("multiple policies: callback fires once when both policies are satisfied simultaneously", async () => {
            let callbackCount = 0;
            const observedReasons: string[][] = [];
            const acc = await buildAccumulator({
                policies: [makeCountPolicy(2), makeSizePolicy(10)],
                onFlushed: async (reasons, _ids) => {
                    callbackCount++;
                    observedReasons.push(reasons);
                }
            });

            await acc.accumulate([makeElement("s", 1, 1), makeElement("s", 2, 2)]);

            // Both policies satisfied in the same evaluatePolicies call, but flush occurs once
            assert.equal(callbackCount, 1);
            assert.deepEqual(observedReasons, [["count-policy", "size-policy"]]);
        });

        it("new elements after flush are queryable from the new container", async () => {
            const acc = await buildAccumulator({
                policies: [makeCountPolicy(2)],
                onFlushed: async () => { }
            });

            // Triggers flush after 2nd element
            await acc.accumulate([makeElement("s", 1, 1), makeElement("s", 2, 2)]);
            // Accumulate into fresh container
            await acc.accumulate([makeElement("s", 3, 99)]);

            const results = await acc.query(makeTagTimeRangeQuery(["s"], 0, 500), 10);
            // Only the element in the new container is visible
            assert.equal(results.length, 1);
            assert.equal(results[0].pld.value, 99);
        });
    });

    describe("PolicyEvaluator standalone", () => {

        it("calls action callback with matched policy names", async () => {
            const triggered: string[] = [];
            const actionMeta: unknown[] = [];
            const evaluator = new PolicyEvaluator();
            evaluator.initialize(async (names, meta) => {
                triggered.push(...names);
                actionMeta.push(meta);
            });
            evaluator.registerPolicy(makeCountPolicy(1));
            evaluator.registerPolicy(makeSizePolicy(999_999)); // will not fire

            const meta: IMetadata = { totalElementsCount: 1, totalElementsSizeBytes: 0 };
            const matched = await evaluator.evaluatePolicies(meta);

            assert.deepEqual(matched, ["count-policy"]);
            assert.deepEqual(triggered, ["count-policy"]);
            assert.deepEqual(actionMeta, [undefined]);
        });

        it("does not call action callback when no policy is satisfied", async () => {
            let called = false;
            const evaluator = new PolicyEvaluator();
            evaluator.initialize(async () => { called = true; });
            evaluator.registerPolicy(makeCountPolicy(100));

            const meta: IMetadata = { totalElementsCount: 1, totalElementsSizeBytes: 0 };
            const matched = await evaluator.evaluatePolicies(meta);

            assert.equal(matched.length, 0);
            assert.equal(called, false);
        });

        it("returns all matched policy names when multiple policies fire", async () => {
            const triggered: string[] = [];
            const evaluator = new PolicyEvaluator();
            evaluator.initialize(async (names) => { triggered.push(...names); });
            evaluator.registerPolicy(makeCountPolicy(1));
            evaluator.registerPolicy(makeSizePolicy(0));

            const meta: IMetadata = { totalElementsCount: 5, totalElementsSizeBytes: 100 };
            const matched = await evaluator.evaluatePolicies(meta);

            assert.deepEqual(matched, ["count-policy", "size-policy"]);
        });
    });

    describe("InMemoryTimeseriesQContainer standalone", () => {

        it("evicts oldest records when buffer capacity is exhausted", async () => {
            // Each record is exactly 50 bytes (4-byte header + 46-byte payload).
            // Buffer of 100 bytes holds exactly 2; writing a 3rd forces eviction of the 1st.
            const container = new InMemoryTimeseriesQContainer("evict-test");
            await container.initialize({ bufferSizeInBytes: 100 });

            const el1 = makeElement("A", 1, 1.0);
            const el2 = makeElement("B", 2, 2.0);
            const el3 = makeElement("C", 3, 3.0);

            await container.accumulate([el1, el2]);
            await container.accumulate([el3]); // should evict el1 if not enough room

            const allQuery: IDimensionalQuery = {
                query: { operator: "OR", conditions: [{ dimension: "tag", operator: "in", value: ["A", "B", "C"] }] }
            };
            const results = await container.query(allQuery, 10);
            // el1 should have been evicted; B and C remain (or at minimum A is gone)
            const tags = results.map(r => r.dim["tag"] as string);
            assert.ok(!tags.includes("A"), `Expected "A" to be evicted but got: ${tags.join(", ")}`);
        });

        it("throws when element is larger than buffer capacity", async () => {
            const container = new InMemoryTimeseriesQContainer("too-small");
            await container.initialize({ bufferSizeInBytes: 20 }); // tiny — smaller than any JSON record + 4-byte header

            await assert.rejects(
                () => container.accumulate([makeElement("sensor-A", 100, 1.0)]),
                /Element is too large for configured buffer/
            );
        });

        it("throws when initialized with invalid buffer size", async () => {
            const container = new InMemoryTimeseriesQContainer("bad-init");
            await assert.rejects(
                () => container.initialize({ bufferSizeInBytes: 4 }),
                /bufferSizeInBytes must be a finite number greater than 4 bytes/
            );
        });

        it("throws when used before initialization", async () => {
            const container = new InMemoryTimeseriesQContainer("uninit");
            await assert.rejects(
                () => container.accumulate([makeElement("s", 1, 1)]),
                /Container must be initialized before use/
            );
        });

        it("supports OR query across multiple tags", async () => {
            const container = new InMemoryTimeseriesQContainer("or-query-test");
            await container.initialize({ bufferSizeInBytes: DEFAULT_BUFFER_SIZE });

            await container.accumulate([
                makeElement("volt", 1, 120),
                makeElement("amp", 2, 15),
                makeElement("temp", 3, 72)
            ]);

            const query: IDimensionalQuery = {
                query: {
                    operator: "OR",
                    conditions: [
                        { dimension: "tag", operator: "eq", value: "volt" },
                        { dimension: "tag", operator: "eq", value: "amp" }
                    ]
                }
            };
            const results = await container.query(query, 10);
            assert.equal(results.length, 2);
            const tags = results.map(r => r.dim["tag"] as string).sort();
            assert.deepEqual(tags, ["amp", "volt"]);
        });

        it("wraps around circular buffer correctly and all remaining records are readable", async () => {
            // Each record is exactly 50 bytes. Buffer of 160 holds 3 (150 bytes used).
            // Writing a 4th (50 bytes) exceeds 160, evicts the 1st, and the payload
            // wraps around the end of the circular buffer.
            const container = new InMemoryTimeseriesQContainer("wrap-test");
            await container.initialize({ bufferSizeInBytes: 160 });

            for (let i = 1; i <= 4; i++) {
                await container.accumulate([makeElement("s", i, i)]);
            }

            const query: IDimensionalQuery = {
                query: { operator: "OR", conditions: [{ dimension: "tag", operator: "eq", value: "s" }] }
            };
            const results = await container.query(query, 10);
            // At least 2 of the most-recent records should survive; the oldest is gone
            assert.ok(results.length >= 2, `Expected ≥2 results after wrap, got ${results.length}`);
            const times = results.map(r => r.dim["time"] as number);
            assert.ok(!times.includes(1), `Expected time=1 to be evicted but got times: ${times.join(", ")}`);
        });
    });

    describe("LocalQAccumulator initialization guards", () => {

        it("throws when used before initialize is called", async () => {
            const acc = new LocalQAccumulator();
            await assert.rejects(
                () => acc.accumulate([makeElement("s", 1, 1)]),
                /LocalQAccumulator must be initialized before use/
            );
        });

        it("throws when containerFactory is null", async () => {
            const acc = new LocalQAccumulator();
            const evaluator = new PolicyEvaluator();
            await assert.rejects(
                () => acc.initialize(null as any, makeContainerMetadata(), evaluator),
                /containerFactory cannot be null/
            );
        });
    });
});
