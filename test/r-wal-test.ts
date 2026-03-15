import assert from "node:assert/strict";
import { describe, it } from "node:test";
import sinon from "sinon";


import { RBook, RWal } from "../src/index.js";

const createWal = () => {
    const book = {
        fetchAvailablePagesWithRanks: sinon.stub<[], Promise<Map<any, number>>>().resolves(new Map())
    } as unknown as RBook;
    return new RWal(book);
};

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

