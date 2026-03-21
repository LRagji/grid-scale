import assert from "node:assert/strict";

import { after, before, beforeEach, describe, it } from "node:test";
import { GenericContainer, StartedTestContainer } from "testcontainers";
import { createClient } from "redis";
import Redis, { Cluster } from "ioredis";
import { IRedisClientPool, IORedisClientPool, RedisClientPool } from "redis-abstraction";

import { ISortedElement, RBook, RDriver, RKeyBuilder, RWal } from "../src/index.js";
import { NodeRedisTestDriver } from "./node-redis-test-driver.js";
import { Utilities } from "../src/utilities.js";

describe(`RWal Integration with ${process.env.REDIS_DRIVER}`, () => {
    let container: StartedTestContainer;
    let pool: IRedisClientPool;

    async function flushAllData(): Promise<void> {
        const token = pool.generateUniqueToken("FlushAll");
        try {
            await pool.acquire(token);
            await pool.run(token, ["FLUSHALL"]);
        }
        finally {
            await pool.release(token);
        }
    }

    before(async () => {
        container = await new GenericContainer("redis:7-alpine")
            .withExposedPorts(6379)
            .start();

        const host = container.getHost();
        const port = container.getMappedPort(6379);
        const singleNodeRedisConnectionString = `redis://${host}:${port}`;

        const selectedDrivers = process.env.REDIS_DRIVER

        switch (selectedDrivers) {
            case "ioredis":
                //IORedis
                //Function which can decompose the connection string into different components like hostname,password etc.
                const parseRedisConnectionString = (connectionString) => {
                    //Used to parse the connection string and return components of the same 
                    //Refer:ioredis/built/utils/index.js parseURL function for more details
                    //This is just a mock implementation, you can enhance it as per your needs.
                    return {
                        password: ""
                    };
                }
                const connectionInjector = () => IORedisClientPool.IORedisClientClusterFactory([singleNodeRedisConnectionString], Redis as any, Cluster as any, parseRedisConnectionString);
                pool = new IORedisClientPool(connectionInjector);
                break;
            case "node-test-driver":
                // Test Adapter with node-redis
                pool = new NodeRedisTestDriver(singleNodeRedisConnectionString);
                break;
            case "node-redis":
            default:
                //Node-redis
                const connectionInjector2 = () => createClient({ url: singleNodeRedisConnectionString });
                pool = new RedisClientPool<any>(connectionInjector2);
                break;
        }
    });

    beforeEach(async () => {
        await flushAllData();
    });

    async function createWal(
        timeToleranceInMs = 60_000,
        timeWindowInMs = 24 * 60 * 60 * 1000,
        sizeWindowInBytes = 300 * 1024 * 1024,
        writeWindow = 1_000_000,
        sizeEstimator: (elements: ISortedElement[]) => number = Utilities.roughSizeEstimator
    ): Promise<RWal> {
        const driver = new RDriver(pool, timeToleranceInMs);
        await driver.initialize();
        const book = new RBook(driver, timeWindowInMs, sizeWindowInBytes, writeWindow, 100, new RKeyBuilder());
        return new RWal(book, sizeEstimator);
    }

    after(async () => {
        await pool.shutdown();
        await container.stop();
    });

    describe("initialization", () => {

        it("passes initialize when host and redis times are tolerance aligned", async () => {
            await assert.doesNotReject(createWal());
        });
    });

    describe("upsert and query behavior", () => {

        it("writes and queries sorted timestamp single-tag samples", async () => {
            const wal = await createWal();

            const input: ISortedElement[] = [
                { gk: "alpha", elementRank: 2, sn: 0, pld: { nV: 22 } },
                { gk: "alpha", elementRank: 1, sn: 0, pld: { nV: 11 } }
            ];

            await wal.append(input.map((_) => structuredClone(_)));

            const result = await wal.queryByRank(["alpha"], 0, 10, 100);
            const simplified = result
                .map((sample) => ({ gk: sample.gk, elementRank: sample.elementRank, nV: sample.pld.nV }));

            assert.deepEqual(simplified, [
                { gk: "alpha", elementRank: 1, nV: 11 },
                { gk: "alpha", elementRank: 2, nV: 22 }
            ]);
        });

        it("updates a tag with the same timestamp and returns the latest value on query", async () => {
            const wal = await createWal();

            await wal.append([
                { gk: "sensor-1", elementRank: 100, sn: 0, pld: { nV: 50 } }
            ]);

            let result = await wal.queryByRank(["sensor-1"], 0, 500, 100);
            assert.equal(result.length, 1);
            assert.equal(result[0].pld.nV, 50);

            await wal.append([
                { gk: "sensor-1", elementRank: 100, sn: 0, pld: { nV: 75 } }
            ]);

            result = await wal.queryByRank(["sensor-1"], 0, 500, 100);
            assert.equal(result.length, 1);
            assert.equal(result[0].gk, "sensor-1");
            assert.equal(result[0].elementRank, 100);
            assert.equal(result[0].pld.nV, 75);
        });

        it("handles multiple sequential updates on same tag and timestamp", async () => {
            const wal = await createWal();

            const updates = [10, 25, 50, 100];

            for (const value of updates) {
                await wal.append([
                    { gk: "counter", elementRank: 99, sn: 0, pld: { nV: value } }
                ]);
            }

            const result = await wal.queryByRank(["counter"], 0, 500, 100);
            assert.equal(result.length, 1);
            assert.equal(result[0].pld.nV, 100);
        });

        it("updates multiple tags with same timestamp and verifies latest values", async () => {
            const wal = await createWal();

            await wal.append([
                { gk: "A", elementRank: 50, sn: 0, pld: { nV: 1 } },
                { gk: "B", elementRank: 50, sn: 0, pld: { nV: 2 } }
            ]);

            await wal.append([
                { gk: "A", elementRank: 50, sn: 0, pld: { nV: 10 } },
                { gk: "B", elementRank: 50, sn: 0, pld: { nV: 20 } }
            ]);

            const result = await wal.queryByRank(["A", "B"], 0, 500, 100);
            const resultMap = result
                .reduce((acc, element) => {
                    acc[element.gk] = element.pld.nV;
                    return acc;
                }, {} as Record<string, number>);

            assert.equal(resultMap["A"], 10);
            assert.equal(resultMap["B"], 20);
        });

        it("deduplicates duplicate tag names in query input", async () => {
            const wal = await createWal();

            await wal.append([{ gk: "dupTag", elementRank: 1, sn: 0, pld: { nV: 7 } }]);

            const result = await wal.queryByRank(["dupTag", "dupTag", "dupTag"], 0, 10, 100);
            assert.equal(result.length, 1);
            assert.equal(result[0].gk, "dupTag");
            assert.equal(result[0].pld.nV, 7);
        });

        it("returns combined results for multiple tags", async () => {
            const wal = await createWal();

            await wal.append([
                { gk: "A", elementRank: 1, sn: 0, pld: { nV: 101 } },
                { gk: "B", elementRank: 2, sn: 0, pld: { nV: 202 } },
                { gk: "A", elementRank: 3, sn: 0, pld: { nV: 303 } }
            ]);

            const result = await wal.queryByRank(["A", "B"], 0, 10, 100);
            const view = result
                .map((_) => `${_.gk}:${_.elementRank}:${_.pld.nV}`)
                .sort();

            assert.deepEqual(view, ["A:1:101", "A:3:303", "B:2:202"]);
        });

        it("picks latest update for same tag and timestamp across pages", async () => {
            const wal = await createWal(10001, 20000, 1, 100000, (_) => 1);

            await wal.append([{ gk: "same", elementRank: 5, sn: 0, pld: { nV: 1 } }]);
            await wal.append([{ gk: "same", elementRank: 5, sn: 0, pld: { nV: 999 } }]);

            const result = await wal.queryByRank(["same"], 0, 10, 1);
            assert.equal(result.length, 1);
            assert.equal(result[0].pld.nV, 999);
        });

        // it("enforces max pages in book by evicting oldest pages", async () => {
        //     const keyBuilder = new RedisKeyBuilder("it-book-limit");
        //     const wal = new RedisWAL(pool, 10001n, 20000n, 10n, 100000n, keyBuilder, Utilities.roughSizeEstimator, 2);

        //     await wal.upsertBulkSamples([{ tag: "P", ts: 1, pld: { nV: 1 } }]);
        //     await wal.upsertBulkSamples([{ tag: "P", ts: 2, pld: { nV: 2 } }]);
        //     await wal.upsertBulkSamples([{ tag: "P", ts: 3, pld: { nV: 3 } }]);

        //     const token = pool.generateUniqueToken("InspectBook");
        //     let pages: string[] = [];
        //     try {
        //         await pool.acquire(token);
        //         pages = await pool.run(token, ["ZRANGE", keyBuilder.bookKey(), "0", "-1"]);
        //     }
        //     finally {
        //         await pool.release(token);
        //     }

        //     assert.equal(pages.length, 2);
        // });

        // it("supports custom size estimator to force page size partitioning", async () => {
        //     const keyBuilder = new RedisKeyBuilder("it-size-window");
        //     const forcedEstimator = () => 10n;
        //     const wal = new RedisWAL(pool, 10001n, 20000n, 10n, 100000n, keyBuilder, forcedEstimator, 100);

        //     await wal.upsertBulkSamples([{ tag: "S", ts: 1, pld: { nV: 10 } }]);
        //     await wal.upsertBulkSamples([{ tag: "S", ts: 2, pld: { nV: 20 } }]);

        //     const token = pool.generateUniqueToken("InspectSizePages");
        //     let totalSize = 0n;
        //     let totalWrites = 0n;
        //     try {
        //         await pool.acquire(token);
        //         const counterKey = "it-size-window:counter";
        //         const sizeCounterBitLocation = 21 * 8; // 168
        //         const writeCounterBitLocation = sizeCounterBitLocation + 63; // 231
        //         const values = await pool.run(token, ["BITFIELD", counterKey, "GET", "u63", `${sizeCounterBitLocation}`, "GET", "u63", `${writeCounterBitLocation}`]);
        //         totalSize = BigInt(values[0] || 0);
        //         totalWrites = BigInt(values[1] || 0);
        //     }
        //     finally {
        //         await pool.release(token);
        //     }

        //     assert.equal(totalSize, 20n);
        //     assert.equal(totalWrites, 2n);
        // });

        // it("supports custom write window partitioning", async () => {
        //     const keyBuilder = new RedisKeyBuilder("it-write-window");
        //     const wal = new RedisWAL(pool, 1n, 1000n, 100000n, 1n, keyBuilder);

        //     await wal.upsertBulkSamples([{ tag: "W", ts: 1, pld: { nV: 1 } }]);
        //     await wal.upsertBulkSamples([{ tag: "W", ts: 2, pld: { nV: 2 } }]);

        //     const token = pool.generateUniqueToken("InspectWritePages");
        //     let totalSize = 0n;
        //     let totalWrites = 0n;
        //     try {
        //         await pool.acquire(token);
        //         const counterKey = "it-write-window:counter";
        //         const sizeCounterBitLocation = 21 * 8; // 168
        //         const writeCounterBitLocation = sizeCounterBitLocation + 63; // 231
        //         const values = await pool.run(token, ["BITFIELD", counterKey, "GET", "u63", `${sizeCounterBitLocation}`, "GET", "u63", `${writeCounterBitLocation}`]);
        //         totalSize = BigInt(values[0] || 0);
        //         totalWrites = BigInt(values[1] || 0);
        //     }
        //     finally {
        //         await pool.release(token);
        //     }

        //     assert.ok(totalSize > 0n);
        //     assert.equal(totalWrites, 2n);
        // });
    });
});
