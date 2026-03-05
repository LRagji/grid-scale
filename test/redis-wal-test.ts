import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { IRedisClientPool } from "redis-abstraction";

import { RedisWAL } from "../src/index.js";
import sinon from "sinon";
import { Utilities } from "../src/utilities.js";

class SinonRedisClientPoolMock implements IRedisClientPool {
    public [Symbol.asyncDispose] = sinon.stub<[], Promise<void>>().resolves();
    public initialize = sinon.stub<[], Promise<void>>().resolves();
    public acquire = sinon.stub<[string], Promise<void>>().resolves();
    public release = sinon.stub<[string], Promise<void>>().resolves();
    public shutdown = sinon.stub<[], Promise<void>>().resolves();
    public run = sinon.stub<[string, string[]], Promise<any>>().resolves(undefined);
    public pipeline = sinon.stub<[string, string[][], boolean], Promise<any>>().resolves([]);
    public script = sinon.stub<[string, string, string[], string[]], Promise<any>>().resolves(undefined);
    public generateUniqueToken = sinon.stub<[string], string>().returns("token-1");
}

const pool: IRedisClientPool = new SinonRedisClientPoolMock();

describe("RedisWAL static tests", () => {

    it("modMinus floors numbers to the nearest divisor block", () => {
        assert.equal(Utilities.modMinus(1234n, 10n), 1230n);
        assert.equal(Utilities.modMinus(1234n, 100n), 1200n);
    });

    //TODO: Fix this estimation logic
    // it("estimateBulkSamplesBytesUpper handles empty and populated arrays", () => {
    //     assert.equal(Utilities.estimateBulkSamplesBytesUpper([]), 2n);

    //     const samples: ISample[] = [
    //         { tag: "A", ts: 1, pld: { nV: 1 } },
    //         { tag: "AB", ts: 2, pld: { nV: 2 } }
    //     ];
    //     const correctSize = new TextEncoder().encode(JSON.stringify(samples)).byteLength;
    //     const actualSize = Number(Utilities.estimateBulkSamplesBytesUpper(samples));
    //     assert.equal(Math.abs(actualSize - correctSize) < (0.1 * correctSize), true, `Expected size to be within 10% of actual size. Actual: ${actualSize}, Correct: ${correctSize}`);
    // });
});


describe("query validation errors", () => {

    it("throws for empty tags", async () => {
        const wal = new RedisWAL(pool, 1001n, 2000n, 100000n, 100000n);
        await assert.rejects(wal.queryRange([], 0n, 1n), /At least one tag must be specified/i);
    });

    it("throws for negative times", async () => {
        const wal = new RedisWAL(pool, 1001n, 2000n, 100000n, 100000n);
        await assert.rejects(wal.queryRange(["a"], -1n, 1n), /must be non-negative/i);

        await assert.rejects(wal.queryRange(["a"], 0n, -1n), /must be non-negative/i);
    });

    it("throws when end is less than start", async () => {
        const wal = new RedisWAL(pool, 1001n, 2000n, 100000n, 100000n);
        await assert.rejects(wal.queryRange(["a"], 10n, 1n), /End time must be greater than or equal to start time/i);
    });

    it("throws when range difference is zero", async () => {
        const wal = new RedisWAL(pool, 1001n, 2000n, 100000n, 100000n);
        await assert.rejects(wal.queryRange(["a"], 10n, 10n), /difference between end time and start time must be greater than 0/i);
    });

    it("throws when more than 10 tags are provided", async () => {
        const wal = new RedisWAL(pool, 1001n, 2000n, 100000n, 100000n);
        const tags = Array.from({ length: 11 }, (_, i) => `tag-${i}`);
        await assert.rejects(wal.queryRange(tags, 0n, 1n), /maximum of 10 tags can be specified/i);
    });
});

