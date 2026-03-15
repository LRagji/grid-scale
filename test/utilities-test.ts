import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { Utilities } from "../src/utilities.js";


describe("Utilities static tests", () => {

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