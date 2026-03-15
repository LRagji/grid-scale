import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { RBook, RWal } from "../src/index.js";
import { Utilities } from "../src/utilities.js";

const createWal = () => {
    const book = {
        fetchAvailablePagesWithRanks: async () => new Map()
    } as unknown as RBook;
    return new RWal(book);
};

describe("RWal static tests", () => {

    it("modMinus floors numbers to the nearest divisor block", () => {
        assert.equal(Utilities.modMinus(1234, 10), 1230);
        assert.equal(Utilities.modMinus(1234, 100), 1200);
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

    it("throws for empty group keys", async () => {
        const wal = createWal();
        await assert.rejects(wal.queryByRank([], 0, 1), /At least one group key must be specified/i);
    });

    it("throws for negative ranks", async () => {
        const wal = createWal();
        await assert.rejects(wal.queryByRank(["a"], -1, 1), /must be non-negative/i);

        await assert.rejects(wal.queryByRank(["a"], 0, -1), /must be non-negative/i);
    });

    it("throws when end is less than start", async () => {
        const wal = createWal();
        await assert.rejects(wal.queryByRank(["a"], 10, 1), /End rank must be greater than or equal to start rank/i);
    });

    it("throws when range difference is zero", async () => {
        const wal = createWal();
        await assert.rejects(wal.queryByRank(["a"], 10, 10), /difference between end rank and start rank must be greater than 0/i);
    });

    it("throws when more than 10 group keys are provided", async () => {
        const wal = createWal();
        const groupKeys = Array.from({ length: 11 }, (_, i) => `group-${i}`);
        await assert.rejects(wal.queryByRank(groupKeys, 0, 1), /maximum of 10 group keys can be specified/i);
    });
});

