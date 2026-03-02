import assert from "node:assert/strict";

import { after, before, beforeEach, describe, it } from "node:test";
import { GenericContainer, StartedTestContainer } from "testcontainers";
import { createClient } from "redis";
import Redis, { Cluster } from "ioredis";
import { IRedisClientPool, IORedisClientPool, RedisClientPool } from "redis-abstraction";
import { setTimeout as delay } from 'node:timers/promises';

import { RedisWAL } from "../src/index.js";
import { RedisKeyBuilder } from "../src/redis-key-builder.js";
import { ISample } from "../src/interfaces/i-sample.js";
import { Utilities } from "../src/utilities.js";
import { NodeRedisTestDriver } from "./node-redis-test-driver.js";

describe(`RedisWAL Integration with ${process.env.REDIS_DRIVER}`, () => {
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

    after(async () => {
        await pool.shutdown();
        await container.stop();
    });

    describe("initialization", () => {

        it("passes initialize when host and redis times are tolerance aligned", async () => {
            const wal = new RedisWAL(pool);
            await assert.doesNotReject(wal.initialize());
        });
    });

    describe("upsert and query behavior", () => {

        it("writes and queries sorted timestamp single-tag samples", async () => {
            const wal = new RedisWAL(pool);

            const input: ISample[] = [
                { tag: "alpha", ts: 2, pld: { nV: 22 } },
                { tag: "alpha", ts: 1, pld: { nV: 11 } }
            ];

            await wal.upsertBulkSamples(input.map((_) => structuredClone(_)));

            const result = await wal.queryRange(["alpha"], 0n, 10n, 100);
            const simplified = result
                .map((sample) => ({ tag: sample.tag, ts: sample.ts, nV: sample.pld.nV }));

            assert.deepEqual(simplified, [
                { tag: "alpha", ts: 1, nV: 11 },
                { tag: "alpha", ts: 2, nV: 22 }
            ]);
        });

        it("deduplicates duplicate tag names in query input", async () => {
            const wal = new RedisWAL(pool);

            await wal.upsertBulkSamples([{ tag: "dupTag", ts: 1, pld: { nV: 7 } }]);

            const result = await wal.queryRange(["dupTag", "dupTag", "dupTag"], 0n, 10n, 100);
            assert.equal(result.length, 1);
            assert.equal(result[0].tag, "dupTag");
            assert.equal(result[0].pld.nV, 7);
        });

        it("returns combined results for multiple tags", async () => {
            const wal = new RedisWAL(pool);

            await wal.upsertBulkSamples([
                { tag: "A", ts: 1, pld: { nV: 101 } },
                { tag: "B", ts: 2, pld: { nV: 202 } },
                { tag: "A", ts: 3, pld: { nV: 303 } }
            ]);

            const result = await wal.queryRange(["A", "B"], 0n, 10n, 100);
            const view = result
                .map((_) => `${_.tag}:${_.ts}:${_.pld.nV}`)
                .sort();

            assert.deepEqual(view, ["A:1:101", "A:3:303", "B:2:202"]);
        });

        it("picks latest update for same tag and timestamp across pages", async () => {
            const wal = new RedisWAL(pool, 1n, 1n, 100000n, 100000n);

            await wal.upsertBulkSamples([{ tag: "same", ts: 5, pld: { nV: 1 } }]);
            await delay(10); //Ensure the second upsert goes to a different page
            await wal.upsertBulkSamples([{ tag: "same", ts: 5, pld: { nV: 999 } }]);

            const result = await wal.queryRange(["same"], 0n, 10n, 100);
            assert.equal(result.length, 1);
            assert.equal(result[0].pld.nV, 999);
        });

        it("enforces max pages in book by evicting oldest pages", async () => {
            const keyBuilder = new RedisKeyBuilder("it-book-limit");
            const wal = new RedisWAL(pool, 1n, 1n, 100000n, 100000n, keyBuilder, Utilities.roughSizeEstimator, 2);

            await wal.upsertBulkSamples([{ tag: "P", ts: 1, pld: { nV: 1 } }]);
            await delay(5);
            await wal.upsertBulkSamples([{ tag: "P", ts: 2, pld: { nV: 2 } }]);
            await delay(5);
            await wal.upsertBulkSamples([{ tag: "P", ts: 3, pld: { nV: 3 } }]);

            const token = pool.generateUniqueToken("InspectBook");
            let pages: string[] = [];
            try {
                await pool.acquire(token);
                pages = await pool.run(token, ["ZRANGE", keyBuilder.bookKey(), "0", "-1"]);
            }
            finally {
                await pool.release(token);
            }

            assert.equal(pages.length, 2);
        });

        it("supports custom size estimator to force page size partitioning", async () => {
            const keyBuilder = new RedisKeyBuilder("it-size-window");
            const forcedEstimator = () => 10n;
            const wal = new RedisWAL(pool, 1n, 1000n, 10n, 100000n, keyBuilder, forcedEstimator, 100);

            await wal.upsertBulkSamples([{ tag: "S", ts: 1, pld: { nV: 10 } }]);
            await wal.upsertBulkSamples([{ tag: "S", ts: 2, pld: { nV: 20 } }]);

            const token = pool.generateUniqueToken("InspectSizePages");
            let counterKeys: string[] = [];
            let totalSize = 0n;
            let totalWrites = 0n;
            try {
                await pool.acquire(token);
                counterKeys = await pool.run(token, ["KEYS", "it-size-window:counter:*"]);
                for (const counterKey of counterKeys) {
                    const values = await pool.run(token, ["BITFIELD", counterKey, "GET", "u63", "#0", "GET", "u63", "#1"]);
                    totalSize += BigInt(values[0]);
                    totalWrites += BigInt(values[1]);
                }
            }
            finally {
                await pool.release(token);
            }

            assert.equal(totalSize, 20n);
            assert.equal(totalWrites, 2n);
        });

        it("supports custom write window partitioning", async () => {
            const keyBuilder = new RedisKeyBuilder("it-write-window");
            const wal = new RedisWAL(pool, 1n, 1000n, 100000n, 1n, keyBuilder);

            await wal.upsertBulkSamples([{ tag: "W", ts: 1, pld: { nV: 1 } }]);
            await wal.upsertBulkSamples([{ tag: "W", ts: 2, pld: { nV: 2 } }]);

            const token = pool.generateUniqueToken("InspectWritePages");
            let counterKeys: string[] = [];
            let totalSize = 0n;
            let totalWrites = 0n;
            try {
                await pool.acquire(token);
                counterKeys = await pool.run(token, ["KEYS", "it-write-window:counter:*"]);
                for (const counterKey of counterKeys) {
                    const values = await pool.run(token, ["BITFIELD", counterKey, "GET", "u63", "#0", "GET", "u63", "#1"]);
                    totalSize += BigInt(values[0]);
                    totalWrites += BigInt(values[1]);
                }
            }
            finally {
                await pool.release(token);
            }

            assert.ok(totalSize > 0n);
            assert.equal(totalWrites, 2n);
        });
    });
});
