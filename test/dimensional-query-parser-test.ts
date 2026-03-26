import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { evaluateDimensionalQuery, filterByDimensionalQuery } from "../src/index.js";
import type { IDimensionalElement, IDimensionalQuery } from "../src/index.js";

const elements: IDimensionalElement[] = [
    { dim: { country: "India", status: "active", age: 30, score: 90 }, pld: { id: "e1" } },
    { dim: { country: "USA", status: "inactive", age: 40, score: 70 }, pld: { id: "e2" } },
    { dim: { country: "Japan", status: "active", age: 22, score: 88 }, pld: { id: "e3" } }
];

describe("dimensional-query-parser", () => {
    it("evaluates nested AND/OR queries", () => {
        const query: IDimensionalQuery = {
            query: {
                operator: "AND",
                conditions: [
                    { dimension: "country", operator: "in", value: ["India", "USA"] },
                    {
                        operator: "OR",
                        conditions: [
                            { dimension: "status", operator: "eq", value: "active" },
                            { dimension: "age", operator: "between", value: [35, 45] }
                        ]
                    }
                ]
            }
        };

        assert.equal(evaluateDimensionalQuery(elements[0], query), true);
        assert.equal(evaluateDimensionalQuery(elements[1], query), true);
        assert.equal(evaluateDimensionalQuery(elements[2], query), false);
    });

    it("supports numeric comparison operators", () => {
        const query: IDimensionalQuery = {
            query: {
                operator: "AND",
                conditions: [
                    { dimension: "age", operator: "gt", value: 25 },
                    { dimension: "score", operator: "lt", value: 95 }
                ]
            }
        };

        const result = filterByDimensionalQuery(elements, query);
        assert.deepEqual(result.map(e => e.pld.id), ["e1", "e2"]);
    });

    it("supports max result cap", () => {
        const query: IDimensionalQuery = {
            query: {
                operator: "OR",
                conditions: [
                    { dimension: "status", operator: "eq", value: "active" },
                    { dimension: "country", operator: "eq", value: "USA" }
                ]
            }
        };

        const result = filterByDimensionalQuery(elements, query, 2);
        assert.equal(result.length, 2);
    });

    it("returns false for missing dimensions or mismatched value types", () => {
        const missingDimQuery: IDimensionalQuery = {
            query: {
                operator: "AND",
                conditions: [
                    { dimension: "missing", operator: "eq", value: "x" }
                ]
            }
        };

        const mismatchedTypeQuery: IDimensionalQuery = {
            query: {
                operator: "AND",
                conditions: [
                    { dimension: "age", operator: "eq", value: "30" }
                ]
            }
        };

        assert.equal(evaluateDimensionalQuery(elements[0], missingDimQuery), false);
        assert.equal(evaluateDimensionalQuery(elements[0], mismatchedTypeQuery), false);
    });
});
