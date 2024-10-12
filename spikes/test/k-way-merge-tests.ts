import assert from "node:assert";
import { describe, it } from "node:test";

import { kWayMerge } from "../src/merge/k-way-merge.js";


describe("K-Way merging", () => {

    it("Should merge distinct N array", () => {
        const arr1 = [1, 3, 5, 7, 9];
        const arr2 = [2, 4, 6, 8, 10];
        const arr3 = [11, 13, 15, 17, 19];

        const ascendingProcessFunction = (elements: (number | null)[]) => {
            let minIndex = -1;
            let minValue = Number.MAX_SAFE_INTEGER;
            for (let i = 0; i < elements.length; i++) {
                if (!Number.isNaN(elements[i] as number) && elements[i] !== null && elements[i] < minValue) {
                    minValue = elements[i] as number;
                    minIndex = i;
                }
            }
            return { yieldIndex: minIndex, purgeIndexes: [] };
        }

        const resultIterator = kWayMerge<number>([arr1.values(), arr2.values(), arr3.values()], ascendingProcessFunction);

        assert.deepStrictEqual(Array.from(resultIterator), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19]);
    });

    it("Should merge N array with repeat elements", () => {
        const arr1 = [-100, 1, 3, 5, 7, 9];
        const arr2 = [-100, 2, 4, 6, 8, 10];
        const arr3 = [-100, 11, 13, 15, 17, 19];

        const ascendingProcessFunction = (elements: (number | null)[]) => {
            let minIndex = -1;
            let minValue = Number.MAX_SAFE_INTEGER;
            for (let i = 0; i < elements.length; i++) {
                if (!Number.isNaN(elements[i] as number) && elements[i] !== null && elements[i] < minValue) {
                    minValue = elements[i] as number;
                    minIndex = i;
                }
            }
            return { yieldIndex: minIndex, purgeIndexes: [] };
        }

        const resultIterator = kWayMerge<number>([arr1.values(), arr2.values(), arr3.values()], ascendingProcessFunction);

        assert.deepStrictEqual(Array.from(resultIterator), [-100, -100, -100, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19]);
    });

    it("Should exit on incorrect index returned.", () => {
        const arr1 = [null, - 100, 1, 3, 5, 7, 9];
        const arr2 = [-100, 2, 4, 6, 8, 10];
        const arr3 = [-100, 11, 13, 15, 17, 19];

        const ascendingProcessFunction = (elements: (number | null)[]) => {
            return { yieldIndex: -1, purgeIndexes: [] };
        }

        const resultIterator = kWayMerge<number>([arr1.values(), arr2.values(), arr3.values()], ascendingProcessFunction);

        assert.throws(() => Array.from(resultIterator), { message: "Frame processing was in-decisive, either it is pointing to empty iterator or incorrect value." });
    });

    it("Should merge N array by excluding incorrect values", () => {
        const arr1 = [-100, 1, null, 3, 5, 7, undefined, 9];
        const arr2 = [-100, 2, null, 4, 6, 8, 10];
        const arr3 = [Number.NaN, -100, 11, 13, 15, 17, 19];

        const ascendingProcessFunction = (elements: (number | null)[]) => {
            let minIndex = -1;
            const dropIndexes = new Array<number>();
            let minValue = Number.MAX_SAFE_INTEGER;
            for (let i = 0; i < elements.length; i++) {
                if (Number.isNaN(elements[i] as number) || elements[i] == null) {
                    dropIndexes.push(i);
                    continue;
                }
                if (elements[i] < minValue) {
                    minValue = elements[i] as number;
                    minIndex = i;
                }
            }
            return { yieldIndex: minIndex, purgeIndexes: dropIndexes };
        }

        const resultIterator = kWayMerge<number>([arr1.values(), arr2.values(), arr3.values()], ascendingProcessFunction);

        assert.deepStrictEqual(Array.from(resultIterator), [-100, -100, -100, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19]);
    });

});