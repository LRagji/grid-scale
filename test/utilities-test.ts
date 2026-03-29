import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { ConvenienceMethods } from "../src/utilities/convenience-methods.js";


describe("Utilities static tests", () => {

    describe("static constants", () => {

        it("u48Max matches the expected 48-bit maximum", () => {
            assert.equal(ConvenienceMethods.u48Max, Number("0xFFFFFFFFFFFF"));
        });

        it("u48In3 stays derived from u48Max", () => {
            assert.equal(ConvenienceMethods.u48In3, ConvenienceMethods.u48Max / 3);
        });
    });

    describe("modMinus", () => {

        it("floors numbers to the nearest divisor block", () => {
            assert.equal(ConvenienceMethods.modMinus(1234, 10), 1230);
            assert.equal(ConvenienceMethods.modMinus(1234, 100), 1200);
        });

        it("returns the same value when already aligned to the divisor", () => {
            assert.equal(ConvenienceMethods.modMinus(1200, 100), 1200);
            assert.equal(ConvenienceMethods.modMinus(48, 12), 48);
        });

        it("returns zero when the input value is zero", () => {
            assert.equal(ConvenienceMethods.modMinus(0, 7), 0);
        });

        it("handles divisors larger than the value by returning zero", () => {
            assert.equal(ConvenienceMethods.modMinus(12, 100), 0);
        });
    });

    describe("roughSizeEstimator", () => {

        it("returns the exact UTF-8 byte size for an empty array", () => {
            assert.equal(ConvenienceMethods.roughSizeEstimator([]), 2);
        });

        it("returns a positive byte size for null input", () => {
            assert.equal(ConvenienceMethods.roughSizeEstimator(null as any) > 0, true);
        });

        it("matches the serialized byte size for legacy WAL samples", () => {
            const samples = [
                { gk: "A", elementRank: 1, sn: 10, pld: { nV: 1 } },
                { gk: "AB", elementRank: 2, sn: 11, pld: { nV: 2, extra: "ok" } }
            ];

            const expectedSize = new TextEncoder().encode(JSON.stringify(samples)).byteLength;

            assert.equal(ConvenienceMethods.roughSizeEstimator(samples), expectedSize);
        });

        it("matches the serialized byte size for dimensional elements", () => {
            const elements = [
                { dim: { sensor: "A", region: "west", rank: 1 }, pld: { nV: 10, state: true } },
                { dim: { sensor: "B", region: "east", rank: 2 }, pld: { nV: 12, tags: ["x", "y"] } }
            ];

            const expectedSize = new TextEncoder().encode(JSON.stringify(elements)).byteLength;

            assert.equal(ConvenienceMethods.roughSizeEstimator(elements), expectedSize);
        });

        it("matches the serialized byte size for mixed primitives and nested payloads", () => {
            const elements = [
                { dim: { enabled: true, retries: 3, name: "alpha" }, pld: { value: null, list: [1, "two", false] } },
                { dim: { enabled: false, retries: 0, name: "beta" }, pld: { nested: { ok: true }, amount: 10.5 } }
            ];

            const expectedSize = new TextEncoder().encode(JSON.stringify(elements)).byteLength;

            assert.equal(ConvenienceMethods.roughSizeEstimator(elements), expectedSize);
        });

        it("counts multibyte UTF-8 characters correctly", () => {
            const unicodeElements = [
                { dim: { city: "São Paulo", label: "café" }, pld: { text: "naïve" } },
                { dim: { city: "東京", label: "雪" }, pld: { text: "😊" } }
            ];

            const serialized = JSON.stringify(unicodeElements);
            const unicodeExpectedSize = new TextEncoder().encode(serialized).byteLength;

            assert.equal(ConvenienceMethods.roughSizeEstimator(unicodeElements), unicodeExpectedSize);
            assert.equal(unicodeExpectedSize > serialized.length, true);
        });

    });

    describe("hashElement", () => {

        it("returns a deterministic hash for the same element", () => {
            const element = { dim: { sensor: "A", region: "west", rank: 1 }, pld: { nV: 1 } };

            assert.equal(ConvenienceMethods.hashElement(element), ConvenienceMethods.hashElement(element));
        });

        it("returns the same hash regardless of dimension key order", () => {
            const first = { dim: { sensor: "A", region: "west", rank: 1 }, pld: { nV: 1 } };
            const second = { dim: { rank: 1, region: "west", sensor: "A" }, pld: { nV: 999 } };

            assert.equal(ConvenienceMethods.hashElement(first), ConvenienceMethods.hashElement(second));
        });

        it("returns the same hash for text dimension keys inserted in different orders", () => {
            const first = { dim: { beta: "two", alpha: "one", gamma: "three" }, pld: { nV: 1 } };
            const second = { dim: { gamma: "three", beta: "two", alpha: "one" }, pld: { nV: 999 } };

            assert.equal(ConvenienceMethods.hashElement(first), ConvenienceMethods.hashElement(second));
        });

        it("returns the same hash for numeric-like dimension keys inserted in different orders", () => {
            const first = { dim: { "10": "ten", "2": "two", "1": "one" }, pld: { nV: 1 } };
            const second = { dim: { "2": "two", "1": "one", "10": "ten" }, pld: { nV: 999 } };

            assert.equal(ConvenienceMethods.hashElement(first), ConvenienceMethods.hashElement(second));
        });

        it("ignores payload changes and hashes only dimensions", () => {
            const first = { dim: { metric: "temp", node: "n1" }, pld: { nV: 10, status: "ok" } };
            const second = { dim: { metric: "temp", node: "n1" }, pld: { nV: 99, status: "failed", detail: { retry: true } } };

            assert.equal(ConvenienceMethods.hashElement(first), ConvenienceMethods.hashElement(second));
        });

        it("returns different hashes for different dimensions", () => {
            const first = { dim: { metric: "temp", node: "n1" }, pld: { nV: 10 } };
            const second = { dim: { metric: "temp", node: "n2" }, pld: { nV: 10 } };

            assert.notEqual(ConvenienceMethods.hashElement(first), ConvenienceMethods.hashElement(second));
        });

        it("returns 0 for an element with no dimensions", () => {
            const element = { dim: {}, pld: { nV: 1 } };

            assert.equal(ConvenienceMethods.hashElement(element), "0");
        });

        it("normalizes numeric and string dimension values through string conversion", () => {
            const numericValue = { dim: { rank: 1 }, pld: { nV: 1 } };
            const stringValue = { dim: { rank: "1" }, pld: { nV: 1 } };

            assert.equal(ConvenienceMethods.hashElement(numericValue), ConvenienceMethods.hashElement(stringValue));
        });
    });
});