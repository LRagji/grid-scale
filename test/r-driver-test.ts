import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import sinon from "sinon";
import type { IRedisClientPool } from "redis-abstraction";

import { RedisKeywords } from "../src/interfaces/i-r-driver.js";
import { RDriver } from "../src/utilities/r-driver.js";
import { ConvenienceMethods } from "../src/utilities/convenience-methods.js";

type RedisPoolStub = IRedisClientPool & {
    initialize: sinon.SinonStub;
    generateUniqueToken: sinon.SinonStub;
    acquire: sinon.SinonStub;
    pipeline: sinon.SinonStub;
    run: sinon.SinonStub;
    release: sinon.SinonStub;
};

function createRedisPoolStub(): RedisPoolStub {
    return {
        initialize: sinon.stub().resolves(),
        generateUniqueToken: sinon.stub().returns("token-1"),
        acquire: sinon.stub().resolves(),
        pipeline: sinon.stub().resolves("pipeline-result"),
        run: sinon.stub().resolves(["0", "0"]),
        release: sinon.stub().resolves(),
    } as unknown as RedisPoolStub;
}

function redisTimeResponse(timeInMs: number): [string, string] {
    const seconds = Math.floor(timeInMs / 1000);
    const microseconds = (timeInMs % 1000) * 1000;
    return [seconds.toString(), microseconds.toString()];
}

async function createInitializedDriver(timeToleranceInMs = 60_000, hostTime = 1735689600123) {
    const redisPool = createRedisPoolStub();
    redisPool.run.resolves(redisTimeResponse(hostTime));
    sinon.stub(Date, "now").returns(hostTime);

    const driver = new RDriver(redisPool, timeToleranceInMs);
    await driver.initialize();

    return { driver, redisPool };
}

afterEach(() => {
    sinon.restore();
});

describe("RDriver", () => {

    describe("constructor", () => {

        it("uses the default time tolerance when none is provided", () => {
            const redisPool = createRedisPoolStub();
            const driver = new RDriver(redisPool);

            assert.equal(driver.timeToleranceInMs, 60_000);
            assert.equal(driver.initialized, -1);
        });

        it("accepts the lower valid boundary of 1001 ms", () => {
            const redisPool = createRedisPoolStub();

            const driver = new RDriver(redisPool, 1001);

            assert.equal(driver.timeToleranceInMs, 1001);
        });

        it("accepts the upper valid boundary of u48In3", () => {
            const redisPool = createRedisPoolStub();

            const driver = new RDriver(redisPool, ConvenienceMethods.u48In3);

            assert.equal(driver.timeToleranceInMs, ConvenienceMethods.u48In3);
        });

        it("throws when time tolerance is exactly 1000 ms", () => {
            const redisPool = createRedisPoolStub();

            assert.throws(
                () => new RDriver(redisPool, 1000),
                /Time tolerance must be between 1 second and/i
            );
        });

        it("throws when time tolerance is less than 1000 ms", () => {
            const redisPool = createRedisPoolStub();

            assert.throws(
                () => new RDriver(redisPool, 999),
                /Time tolerance must be between 1 second and/i
            );
        });

        it("throws when time tolerance exceeds u48In3", () => {
            const redisPool = createRedisPoolStub();

            assert.throws(
                () => new RDriver(redisPool, ConvenienceMethods.u48In3 + 1),
                /Time tolerance must be between 1 second and/i
            );
        });
    });

    describe("initialize", () => {

        it("initializes the redis pool and verifies aligned time", async () => {
            const hostTime = 1735689600123;
            const redisPool = createRedisPoolStub();
            redisPool.run.resolves(redisTimeResponse(hostTime));
            sinon.stub(Date, "now").returns(hostTime);

            const driver = new RDriver(redisPool, 60_000);

            await assert.doesNotReject(driver.initialize());

            assert.equal(redisPool.initialize.calledOnce, true);
            assert.equal(redisPool.generateUniqueToken.calledOnceWithExactly("TimeToleranceCheck"), true);
            assert.equal(redisPool.acquire.calledOnceWithExactly("token-1"), true);
            assert.equal(redisPool.run.calledOnceWithExactly("token-1", [RedisKeywords.TIME]), true);
            assert.equal(redisPool.release.calledOnceWithExactly("token-1"), true);
            assert.equal(driver.initialized, 0);
        });

        it("throws and sets initialized to -3 when time tolerance check fails", async () => {
            const hostTime = 1735689600123;
            const redisPool = createRedisPoolStub();
            redisPool.run.resolves(redisTimeResponse(hostTime + 60_000));
            sinon.stub(Date, "now").returns(hostTime);

            const driver = new RDriver(redisPool, 60_000);

            await assert.rejects(
                driver.initialize(),
                /Time tolerance check failed/i
            );

            assert.equal(driver.initialized, -3);
            assert.equal(redisPool.initialize.calledOnce, true);
            assert.equal(redisPool.generateUniqueToken.calledOnceWithExactly("TimeToleranceCheck"), true);
            assert.equal(redisPool.acquire.calledOnceWithExactly("token-1"), true);
            assert.equal(redisPool.run.calledOnceWithExactly("token-1", [RedisKeywords.TIME]), true);
            assert.equal(redisPool.release.calledOnceWithExactly("token-1"), true);
        });

        it("propagates redis initialize failures and keeps initialized at -1", async () => {
            const redisPool = createRedisPoolStub();
            redisPool.initialize.rejects(new Error("connect failed"));

            const driver = new RDriver(redisPool, 60_000);

            await assert.rejects(driver.initialize(), /connect failed/i);

            assert.equal(driver.initialized, -1);
            assert.equal(redisPool.run.called, false);
            assert.equal(redisPool.release.called, false);
        });

        it("propagates time-check driver failures and still releases the token", async () => {
            const hostTime = 1735689600123;
            const redisPool = createRedisPoolStub();
            redisPool.run.rejects(new Error("time fetch failed"));
            sinon.stub(Date, "now").returns(hostTime);

            const driver = new RDriver(redisPool, 60_000);

            await assert.rejects(driver.initialize(), /time fetch failed/i);

            assert.equal(driver.initialized, 0);
            assert.equal(redisPool.release.calledOnceWithExactly("token-1"), true);
        });
    });

    describe("usingRedisDriver", () => {

        it("throws when called before initialization", async () => {
            const redisPool = createRedisPoolStub();
            const driver = new RDriver(redisPool, 60_000);

            await assert.rejects(
                driver.usingRedisDriver([[RedisKeywords.TIME]], "BeforeInit", "run"),
                /Redis driver is not initialized\. Current state: -1/i
            );
        });

        it("uses pipeline mode by default", async () => {
            const { driver, redisPool } = await createInitializedDriver();
            const commands = [[RedisKeywords.ZADD, "key", "1", "value"]];

            const result = await driver.usingRedisDriver<string>(commands, "PipelineDefault");

            assert.equal(result, "pipeline-result");
            assert.equal(redisPool.generateUniqueToken.calledTwice, true);
            assert.equal(redisPool.generateUniqueToken.secondCall.calledWithExactly("PipelineDefault"), true);
            assert.equal(redisPool.acquire.calledTwice, true);
            assert.equal(redisPool.acquire.secondCall.calledWithExactly("token-1"), true);
            assert.equal(redisPool.pipeline.calledOnceWithExactly("token-1", commands, false), true);
            assert.equal(redisPool.run.calledOnce, true);
            assert.equal(redisPool.release.calledTwice, true);
            assert.equal(redisPool.release.secondCall.calledWithExactly("token-1"), true);
        });

        it("uses pipeline mode when explicitly requested", async () => {
            const { driver, redisPool } = await createInitializedDriver();
            const commands = [[RedisKeywords.ZRANGE, "key", "0", "-1"]];

            const result = await driver.usingRedisDriver<string>(commands, "PipelineExplicit", "pipeline");

            assert.equal(result, "pipeline-result");
            assert.equal(redisPool.pipeline.calledOnceWithExactly("token-1", commands, false), true);
            assert.equal(redisPool.release.calledTwice, true);
            assert.equal(redisPool.release.secondCall.calledWithExactly("token-1"), true);
        });

        it("uses run mode with the first command only", async () => {
            const { driver, redisPool } = await createInitializedDriver();
            redisPool.run.onSecondCall().resolves("run-result");
            const commands = [
                [RedisKeywords.TIME],
                [RedisKeywords.ZRANGE, "ignored", "0", "1"]
            ];

            const result = await driver.usingRedisDriver<string>(commands, "RunMode", "run");

            assert.equal(result, "run-result");
            assert.equal(redisPool.run.calledTwice, true);
            assert.equal(redisPool.run.secondCall.calledWithExactly("token-1", commands[0]), true);
            assert.equal(redisPool.pipeline.called, false);
            assert.equal(redisPool.release.calledTwice, true);
            assert.equal(redisPool.release.secondCall.calledWithExactly("token-1"), true);
        });

        it("releases the token when pipeline execution fails", async () => {
            const { driver, redisPool } = await createInitializedDriver();
            redisPool.pipeline.rejects(new Error("pipeline failed"));

            await assert.rejects(
                driver.usingRedisDriver([[RedisKeywords.ZADD, "key", "1", "value"]], "PipelineFailure", "pipeline"),
                /pipeline failed/i
            );

            assert.equal(redisPool.release.calledTwice, true);
            assert.equal(redisPool.release.secondCall.calledWithExactly("token-1"), true);
        });

        it("releases the token when run execution fails", async () => {
            const { driver, redisPool } = await createInitializedDriver();
            redisPool.run.onSecondCall().rejects(new Error("run failed"));

            await assert.rejects(
                driver.usingRedisDriver([[RedisKeywords.TIME]], "RunFailure", "run"),
                /run failed/i
            );

            assert.equal(redisPool.release.calledTwice, true);
            assert.equal(redisPool.release.secondCall.calledWithExactly("token-1"), true);
        });

        it("releases the token when acquire fails", async () => {
            const { driver, redisPool } = await createInitializedDriver();
            redisPool.acquire.onSecondCall().rejects(new Error("acquire failed"));

            await assert.rejects(
                driver.usingRedisDriver([[RedisKeywords.TIME]], "AcquireFailure", "run"),
                /acquire failed/i
            );

            assert.equal(redisPool.release.calledTwice, true);
            assert.equal(redisPool.release.secondCall.calledWithExactly("token-1"), true);
            assert.equal(redisPool.run.calledOnce, true);
            assert.equal(redisPool.pipeline.called, false);
        });

        it("throws with state -3 after initialize fails the public tolerance check path", async () => {
            const hostTime = 1735689600123;
            const redisPool = createRedisPoolStub();
            redisPool.run.onFirstCall().resolves(redisTimeResponse(hostTime + 60_000));
            sinon.stub(Date, "now").returns(hostTime);

            const driver = new RDriver(redisPool, 60_000);

            await assert.rejects(driver.initialize(), /Time tolerance check failed/i);
            await assert.rejects(
                driver.usingRedisDriver([[RedisKeywords.TIME]], "AfterToleranceFailure", "run"),
                /Redis driver is not initialized\. Current state: -3/i
            );
        });
    });

    describe("harmonizedTimeInMs", () => {

        it("floors the provided time to the configured tolerance window", () => {
            const redisPool = createRedisPoolStub();
            const driver = new RDriver(redisPool, 60_000);

            assert.equal(driver.harmonizedTimeInMs(1735689665123), 1735689660000);
        });

        it("uses Date.now when no time is provided", () => {
            const redisPool = createRedisPoolStub();
            const driver = new RDriver(redisPool, 10_000);
            sinon.stub(Date, "now").returns(12345);

            assert.equal(driver.harmonizedTimeInMs(), 10000);
        });
    });
});