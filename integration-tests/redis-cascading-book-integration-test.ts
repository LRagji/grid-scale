import assert from "node:assert/strict";

import { after, before, beforeEach, describe, it } from "node:test";
import { GenericContainer, StartedTestContainer } from "testcontainers";
import { createClient } from "redis";
import Redis, { Cluster } from "ioredis";
import { IRedisClientPool, IORedisClientPool, RedisClientPool } from "redis-abstraction";

import { IDimensionalElement, IPage, IPageInfo, RedisCascadingBook, RDriver, RKeyBuilder } from "../src/index.js";
import { NodeRedisTestDriver } from "./node-redis-test-driver.js";

class RedisIntegrationPage implements IPage {
    public readonly pageType: string;

    constructor(
        public readonly info: IPageInfo,
        pageType: string,
        private readonly pool: IRedisClientPool
    ) {
        this.pageType = pageType;
    }

    public async upsertElements(elements: IDimensionalElement[], _sequenceStart: number): Promise<void> {
        const token = this.pool.generateUniqueToken("PageUpsert");
        try {
            await this.pool.acquire(token);
            for (const element of elements) {
                const groupKey = String(element.dim.group);
                const rank = Number(element.dim.rank);
                const redisGroupKey = this.redisGroupKey(groupKey);
                const serialized = JSON.stringify(element);
                await this.pool.run(token, ["ZADD", redisGroupKey, rank.toString(), serialized]);
                await this.pool.run(token, ["SADD", this.redisGroupIndexKey(), groupKey]);
            }
        }
        finally {
            await this.pool.release(token);
        }
    }

    public async queryElementsByDimensions(): Promise<IDimensionalElement[]> {
        throw new Error("Not implemented for this integration page. Use fetchElementsByRange.");
    }

    public async dumpPage(): Promise<IDimensionalElement[]> {
        const token = this.pool.generateUniqueToken("PageDump");
        try {
            await this.pool.acquire(token);
            const groups = await this.pool.run(token, ["SMEMBERS", this.redisGroupIndexKey()]) as string[];
            const results: IDimensionalElement[] = [];
            for (const groupKey of groups) {
                const values = await this.pool.run(token, ["ZRANGE", this.redisGroupKey(groupKey), "0", "-1"]) as string[];
                for (const serialized of values) {
                    results.push(JSON.parse(serialized) as IDimensionalElement);
                }
            }
            return results;
        }
        finally {
            await this.pool.release(token);
        }
    }

    public async fetchElementsByRange(groupKeys: string[], startInclusiveRank: number, endExclusiveRank: number, maxElementsPerGroup: number): Promise<IDimensionalElement[]> {
        const token = this.pool.generateUniqueToken("PageFetchByRange");
        try {
            await this.pool.acquire(token);
            const results: IDimensionalElement[] = [];
            for (const groupKey of groupKeys) {
                const redisGroupKey = this.redisGroupKey(groupKey);
                const values = await this.pool.run(token, [
                    "ZRANGEBYSCORE",
                    redisGroupKey,
                    startInclusiveRank.toString(),
                    `(${endExclusiveRank.toString()}`,
                    "LIMIT",
                    "0",
                    maxElementsPerGroup.toString()
                ]) as string[];
                for (const serialized of values) {
                    results.push(JSON.parse(serialized) as IDimensionalElement);
                }
            }
            return results;
        }
        finally {
            await this.pool.release(token);
        }
    }

    private redisGroupKey(groupKey: string): string {
        return `${this.info.pageKey}:group:${groupKey}`;
    }

    private redisGroupIndexKey(): string {
        return `${this.info.pageKey}:groups`;
    }
}

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

    function makeElement(group: string, rank: number, value: number, globalIdentityHash = `${group}:${rank}`): IDimensionalElement {
        return {
            globalIdentityHash,
            dim: { group, rank, sensor: group },
            pld: { value }
        };
    }

    async function createBook(params: {
        totalPageCapacity: number;
        pageSizeLimitInBytes: number;
        sizeEstimator?: (elements: IDimensionalElement[]) => number;
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
            "redis-integration-page",
            async (pageInfo, pageType) => new RedisIntegrationPage(pageInfo, pageType, pool),
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

    describe("upsertPageInfo behavior through upsertElements/queryByRank", () => {

        it("creates a new page on first write and returns data through queryByRank", async () => {
            const book = await createBook({ totalPageCapacity: 5, pageSizeLimitInBytes: 100 });

            await book.upsertElements([makeElement("g1", 10, 101)]);

            const pages = await book.listPages();
            const result = await book.queryByRank(["g1"], 0, 100, 10);

            assert.equal(pages.length, 1);
            assert.equal(result.length, 1);
            assert.equal(result[0].pld.value, 101);
        });

        it("reuses same page when still within same page window (newPage=false, no trim)", async () => {
            const book = await createBook({ totalPageCapacity: 5, pageSizeLimitInBytes: 100, sizeEstimator: () => 1 });

            await book.upsertElements([makeElement("g1", 1, 11)]);
            await book.upsertElements([makeElement("g1", 2, 22)]);

            const pages = await book.listPages();
            const result = await book.queryByRank(["g1"], 0, 100, 10);
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

            const pages = await book.listPages();
            const result = await book.queryByRank(["g1", "g2", "g3"], 0, 100, 10);
            const byGroup = result.reduce((acc, item) => {
                acc[String(item.dim.group)] = item.pld.value;
                return acc;
            }, {} as Record<string, number>);

            assert.equal(pages.length, 2);
            assert.equal(byGroup["g1"], undefined);
            assert.equal(byGroup["g2"], 202);
            assert.equal(byGroup["g3"], 303);
        });

        it("keeps latest value across pages for same globalIdentityHash while still trimming by capacity", async () => {
            const book = await createBook({ totalPageCapacity: 2, pageSizeLimitInBytes: 1, sizeEstimator: () => 1 });

            await book.upsertElements([makeElement("same", 5, 1, "same-sensor")]);
            await book.upsertElements([makeElement("same", 5, 2, "same-sensor")]);
            await book.upsertElements([makeElement("same", 5, 3, "same-sensor")]);

            const result = await book.queryByRank(["same"], 0, 100, 10);

            assert.equal(result.length, 1);
            assert.equal(result[0].pld.value, 3);
        });
    });
});
