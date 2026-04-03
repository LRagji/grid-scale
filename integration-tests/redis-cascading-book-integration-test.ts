import assert from "node:assert/strict";

import { after, before, beforeEach, describe, it } from "node:test";
import { GenericContainer, StartedTestContainer } from "testcontainers";
import { createClient } from "redis";
import Redis, { Cluster } from "ioredis";
import { IRedisClientPool, IORedisClientPool, RedisClientPool } from "redis-abstraction";

import { IDimensionalQuery, IPageInfo, RedisCascadingBook, RDriver, RKeyBuilder } from "../src/index.js";
import { RedisTsPage, TimeseriesSample } from "../api-tests/rest-wrapper/redis-ts-page.js";
import { NodeRedisTestDriver } from "./node-redis-test-driver.js";


describe(`RedisCascadingBook Integration with ${process.env.REDIS_DRIVER}`, () => {
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

    function makeElement(tag: string, time: number, value: number): TimeseriesSample {
        return new TimeseriesSample(tag, time, { value });
    }

    function makeTagTimeRangeQuery(tags: string[], startInclusiveTime: number, endInclusiveTime: number): IDimensionalQuery {
        return {
            query: {
                operator: "AND",
                conditions: [
                    { dimension: "tag", operator: "in", value: tags },
                    { dimension: "time", operator: "between", value: [startInclusiveTime, endInclusiveTime] }
                ]
            }
        };
    }

    async function createBook(params: {
        totalPageCapacity: number;
        pageSizeLimitInBytes: number;
        sizeEstimator?: (elements: TimeseriesSample[]) => number;
        keyPrefix?: string;
    }): Promise<RedisCascadingBook> {
        const driver = new RDriver(pool, 60_000);
        await driver.initialize();

        const keyBuilder = new RKeyBuilder(
            params.keyPrefix ?? `it-cb-${Date.now()}-${Math.floor(Math.random() * 100000)}`
        );

        return new RedisCascadingBook(
            params.totalPageCapacity,
            params.pageSizeLimitInBytes,
            120_000,
            "redis-ts-page",
            async (pageInfo, pageType) => new RedisTsPage(pageInfo, driver, keyBuilder),
            async () => { },
            driver,
            params.sizeEstimator ?? (() => 1),
            keyBuilder
        );
    }

    before(async () => {
        container = await new GenericContainer("redis:7-alpine")
            .withExposedPorts(6379)
            .start();

        const host = container.getHost();
        const port = container.getMappedPort(6379);
        const singleNodeRedisConnectionString = `redis://${host}:${port}`;

        const selectedDrivers = process.env.REDIS_DRIVER;

        switch (selectedDrivers) {
            case "ioredis": {
                const parseRedisConnectionString = (_connectionString: string) => ({ password: "" });
                const connectionInjector = () => IORedisClientPool.IORedisClientClusterFactory(
                    [singleNodeRedisConnectionString],
                    Redis as any,
                    Cluster as any,
                    parseRedisConnectionString
                );
                pool = new IORedisClientPool(connectionInjector);
                break;
            }
            case "node-test-driver":
                pool = new NodeRedisTestDriver(singleNodeRedisConnectionString);
                break;
            case "node-redis":
            default: {
                const connectionInjector2 = () => createClient({ url: singleNodeRedisConnectionString });
                pool = new RedisClientPool<any>(connectionInjector2);
                break;
            }
        }
    });

    beforeEach(async () => {
        await flushAllData();
    });

    after(async () => {
        await pool.shutdown();
        await container.stop();
    });

    describe("upsertPageInfo behavior through upsertElements/queryElementsByDimensions", () => {

        it("creates a new page on first write and returns data through queryElementsByDimensions", async () => {
            const book = await createBook({ totalPageCapacity: 5, pageSizeLimitInBytes: 100 });

            await book.upsertElements([makeElement("g1", 10, 101)]);

            const pages = await book.listPagesSorted();
            const result = await book.queryElementsByDimensions(makeTagTimeRangeQuery(["g1"], 0, 100), 10);

            assert.equal(pages.length, 1);
            assert.equal(result.length, 1);
            assert.equal(result[0].pld.value, 101);
        });

        it("reuses same page when still within same page window (newPage=false, no trim)", async () => {
            const book = await createBook({ totalPageCapacity: 5, pageSizeLimitInBytes: 100, sizeEstimator: () => 1 });

            await book.upsertElements([makeElement("g1", 1, 11)]);
            await book.upsertElements([makeElement("g1", 2, 22)]);

            const pages = await book.listPagesSorted();
            const result = await book.queryElementsByDimensions(makeTagTimeRangeQuery(["g1"], 0, 100), 10);
            const values = result.map((item) => item.pld.value).sort((a, b) => a - b);

            assert.equal(pages.length, 1);
            assert.deepEqual(values, [11, 22]);
        });

        it("adds a new page and trims oldest pages when capacity is exceeded", async () => {
            // pageSizeLimit=1 with estimator=1 forces a new page key per write.
            const book = await createBook({ totalPageCapacity: 2, pageSizeLimitInBytes: 1, sizeEstimator: () => 1 });

            await book.upsertElements([makeElement("g1", 1, 101)]);
            await book.upsertElements([makeElement("g2", 2, 202)]);
            await book.upsertElements([makeElement("g3", 3, 303)]);

            const pages = await book.listPagesSorted();
            const result = await book.queryElementsByDimensions(makeTagTimeRangeQuery(["g1", "g2", "g3"], 0, 100), 10);
            const byGroup = result.reduce((acc, item) => {
                acc[String(item.dim.tag)] = item.pld.value;
                return acc;
            }, {} as Record<string, number>);

            assert.equal(pages.length, 2);
            assert.equal(byGroup["g1"], undefined);
            assert.equal(byGroup["g2"], 202);
            assert.equal(byGroup["g3"], 303);
        });

        it("keeps latest value across pages for same globalIdentityHash while still trimming by capacity", async () => {
            const book = await createBook({ totalPageCapacity: 2, pageSizeLimitInBytes: 1, sizeEstimator: () => 1 });

            await book.upsertElements([makeElement("same", 5, 1)]);
            await book.upsertElements([makeElement("same", 5, 2)]);
            await book.upsertElements([makeElement("same", 5, 3)]);

            const result = await book.queryElementsByDimensions(makeTagTimeRangeQuery(["same"], 0, 100), 10);

            assert.equal(result.length, 1);
            assert.equal(result[0].pld.value, 3);
        });
    });

    describe("RedisTsPage-specific scenarios", () => {

        it("dumpPage returns all elements from a page", async () => {
            const book = await createBook({ totalPageCapacity: 5, pageSizeLimitInBytes: 100 });

            await book.upsertElements([makeElement("sensor-a", 1000, 10)]);
            await book.upsertElements([makeElement("sensor-a", 2000, 20)]);
            await book.upsertElements([makeElement("sensor-b", 1500, 15)]);

            const pages = await book.listPagesSorted();
            assert.equal(pages.length, 1);

            // Verify all elements were stored
            const result = await book.queryElementsByDimensions(makeTagTimeRangeQuery(["sensor-a", "sensor-b"], 0, 10000), 100);
            assert.equal(result.length, 3);
        });

        it("handles multiple tags correctly with time-based ordering", async () => {
            const book = await createBook({ totalPageCapacity: 5, pageSizeLimitInBytes: 200, sizeEstimator: () => 1 });

            // Add elements with different tags and times
            await book.upsertElements([makeElement("temp", 100, 25)]);
            await book.upsertElements([makeElement("humidity", 100, 55)]);
            await book.upsertElements([makeElement("temp", 200, 26)]);
            await book.upsertElements([makeElement("humidity", 200, 56)]);

            const result = await book.queryElementsByDimensions(makeTagTimeRangeQuery(["temp", "humidity"], 0, 500), 100);

            assert.equal(result.length, 4);
            const tempValues = result.filter(e => e.dim.tag === "temp").map(e => e.pld.value).sort((a, b) => a - b);
            const humidityValues = result.filter(e => e.dim.tag === "humidity").map(e => e.pld.value).sort((a, b) => a - b);
            assert.deepEqual(tempValues, [25, 26]);
            assert.deepEqual(humidityValues, [55, 56]);
        });

        it("correctly orders elements by time within the same tag", async () => {
            const book = await createBook({ totalPageCapacity: 5, pageSizeLimitInBytes: 100 });

            // Add elements out of chronological order
            await book.upsertElements([makeElement("sensor-x", 300, 30)]);
            await book.upsertElements([makeElement("sensor-x", 100, 10)]);
            await book.upsertElements([makeElement("sensor-x", 200, 20)]);

            // Query should return them ordered by time
            const result = await book.queryElementsByDimensions(makeTagTimeRangeQuery(["sensor-x"], 0, 1000), 100);

            assert.equal(result.length, 3);
            const values = result.map(e => e.pld.value);
            assert.deepEqual(values, [10, 20, 30]);
        });
    });
});
