import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import sinon from "sinon";

import { RedisCascadingBook } from "../src/index.js";
import type { IDimensionalElement, IPage, IPageInfo, IRDriver, IKeyBuilder } from "../src/index.js";

// ── constants ─────────────────────────────────────────────────────────────────
const VALID_CAPACITY = 10;
const VALID_PAGE_SIZE_BYTES = 100;
const VALID_ACTIVE_TIME_MS = 5_000;
const VALID_PAGE_TYPE = "test-page";

// ── element fixtures ──────────────────────────────────────────────────────────
const elemSensorA_v1: IDimensionalElement = { dim: { sensor: "A" }, pld: { value: 1 } };
const elemSensorA_v2: IDimensionalElement = { dim: { sensor: "A" }, pld: { value: 2 } };
const elemSensorB: IDimensionalElement = { dim: { sensor: "B" }, pld: { value: 3 } };
const elemSensorC: IDimensionalElement = { dim: { sensor: "C" }, pld: { value: 4 } };

// ── type aliases ──────────────────────────────────────────────────────────────
type DriverStub = IRDriver & {
    usingRedisDriver: sinon.SinonStub;
    harmonizedTimeInMs: sinon.SinonStub;
    initialize: sinon.SinonStub;
};

type PageStub = IPage & {
    fetchElementsByRange: sinon.SinonStub;
    upsertElements: sinon.SinonStub;
};

// ── stub factories ────────────────────────────────────────────────────────────
function makePage(): PageStub {
    return {
        info: { pageKey: "unused", startTime: 0, startSize: 0, startSerialNumber: 0 },
        pageType: VALID_PAGE_TYPE,
        upsertElements: sinon.stub().resolves(),
        queryElementsByDimensions: sinon.stub().resolves([]),
        dumpPage: sinon.stub().resolves([]),
        fetchElementsByRange: sinon.stub().resolves([])
    } as unknown as PageStub;
}

function makeDriver(): DriverStub {
    return {
        timeToleranceInMs: 100,
        initialize: sinon.stub().resolves(),
        usingRedisDriver: sinon.stub(),
        harmonizedTimeInMs: sinon.stub().returns(10_000)
    } as unknown as DriverStub;
}

function makeKeyBuilder(): IKeyBuilder {
    return {
        counterKey: sinon.stub().returns("counter-key"),
        pageKey: sinon.stub().callsFake((t: string, s: string, w: string) => `page:${t}:${s}:${w}`),
        bookKey: sinon.stub().returns("book-key"),
        dimensionKey: sinon.stub().callsFake((pk: string, tn: string) => `${pk}:${tn}`),
        pageDimensionsDict: sinon.stub().callsFake((pk: string) => `${pk}:groups`)
    };
}

function makePageInfo(pageKey: string, startTime: number): IPageInfo {
    return { pageKey, startTime, startSize: 0, startSerialNumber: 0 };
}

function makeBook(overrides: Partial<{
    redisDriver: DriverStub;
    keyBuilder: IKeyBuilder;
    pageFactory: (pageInfo: IPageInfo, pageType: string) => Promise<PageStub>;
    pagesReconcileCallback: (newPageInfo: IPageInfo | undefined, evictedPageInfo: IPageInfo[]) => Promise<void>;
    hashFunction: (element: IDimensionalElement) => string;
}> = {}) {
    const redisDriver = overrides.redisDriver ?? makeDriver();
    const pagesReconcileCallback = overrides.pagesReconcileCallback
        ?? sinon.stub<[IPageInfo | undefined, IPageInfo[]], Promise<void>>().resolves();
    const keyBuilder = overrides.keyBuilder ?? makeKeyBuilder();
    const dummyPage = makePage();
    const pageFactory = overrides.pageFactory
        ?? sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(dummyPage);

    const book = overrides.hashFunction
        ? new RedisCascadingBook(
            VALID_CAPACITY, VALID_PAGE_SIZE_BYTES, VALID_ACTIVE_TIME_MS, VALID_PAGE_TYPE,
            pageFactory, pagesReconcileCallback, redisDriver, () => 1, keyBuilder,
            overrides.hashFunction)
        : new RedisCascadingBook(
            VALID_CAPACITY, VALID_PAGE_SIZE_BYTES, VALID_ACTIVE_TIME_MS, VALID_PAGE_TYPE,
            pageFactory, pagesReconcileCallback, redisDriver, () => 1, keyBuilder);

    return { book, redisDriver, pageFactory: pageFactory as sinon.SinonStub };
}

/** Stubs the ZRANGE call that listPages uses under the hood. */
function stubListPages(redisDriver: DriverStub, pageInfos: IPageInfo[]): void {
    redisDriver.usingRedisDriver.resolves(pageInfos.map(pi => JSON.stringify(pi)));
}

/** Stable sort for asserting element arrays without depending on insertion order. */
function sortElements(elems: IDimensionalElement[]): IDimensionalElement[] {
    return [...elems].sort((a, b) => JSON.stringify(a.dim).localeCompare(JSON.stringify(b.dim)));
}

// ── tests ─────────────────────────────────────────────────────────────────────
afterEach(() => {
    sinon.restore();
});

describe("RedisCascadingBook.queryByRank", () => {

    // ── INPUT VALIDATION ──────────────────────────────────────────────────────

    it("throws when groupKeys array is empty", async () => {
        const { book } = makeBook();
        await assert.rejects(
            book.queryByRank([], 0, 10),
            /At least one group key must be specified/i
        );
    });

    it("throws when startInclusiveRank is negative", async () => {
        const { book } = makeBook();
        await assert.rejects(
            book.queryByRank(["g1"], -1, 10),
            /Start rank and end rank must be non-negative/i
        );
    });

    it("throws when endExclusiveRank is negative", async () => {
        const { book } = makeBook();
        await assert.rejects(
            book.queryByRank(["g1"], 0, -1),
            /Start rank and end rank must be non-negative/i
        );
    });

    it("throws when endExclusiveRank is less than startInclusiveRank", async () => {
        const { book } = makeBook();
        await assert.rejects(
            book.queryByRank(["g1"], 5, 3),
            /End rank must be greater than or equal to start rank/i
        );
    });

    it("throws when startInclusiveRank equals endExclusiveRank producing a zero-length range", async () => {
        const { book } = makeBook();
        await assert.rejects(
            book.queryByRank(["g1"], 5, 5),
            /difference between end rank and start rank must be greater than 0/i
        );
    });

    it("throws when more than 10 group keys are provided", async () => {
        const { book } = makeBook();
        const tooManyKeys = Array.from({ length: 11 }, (_, i) => `group-${i}`);
        await assert.rejects(
            book.queryByRank(tooManyKeys, 0, 10),
            /maximum of 10 group keys/i
        );
    });

    it("throws when maxElementsPerGroup is zero", async () => {
        const { book } = makeBook();
        await assert.rejects(
            book.queryByRank(["g1"], 0, 10, 0),
            /Max elements must be between 1 and 10000/i
        );
    });

    it("throws when maxElementsPerGroup is negative", async () => {
        const { book } = makeBook();
        await assert.rejects(
            book.queryByRank(["g1"], 0, 10, -1),
            /Max elements must be between 1 and 10000/i
        );
    });

    it("throws when maxElementsPerGroup exceeds 10000", async () => {
        const { book } = makeBook();
        await assert.rejects(
            book.queryByRank(["g1"], 0, 10, 10_001),
            /Max elements must be between 1 and 10000/i
        );
    });

    // ── SINGLE-PAGE DATA RETRIEVAL ────────────────────────────────────────────

    it("returns an empty array when no pages are stored", async () => {
        const { book, redisDriver } = makeBook();
        stubListPages(redisDriver, []);

        const result = await book.queryByRank(["g1"], 0, 10);

        assert.deepEqual(result, []);
        assert.equal(
            redisDriver.usingRedisDriver.calledOnceWithExactly(
                [["zrange", "book-key", "0", "-1"]],
                "FetchAllPagesWithRanks",
                "run"
            ),
            true
        );
    });

    it("returns all elements from a single page", async () => {
        const page = makePage();
        page.fetchElementsByRange.resolves([elemSensorA_v1, elemSensorB]);
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(page);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000)]);

        const result = await book.queryByRank(["g1"], 0, 10);

        assert.deepEqual(sortElements(result), sortElements([elemSensorA_v1, elemSensorB]));
        assert.equal(page.fetchElementsByRange.calledOnceWithExactly(["g1"], 0, 10, 1000), true);
    });

    it("returns an empty array when the page returns no matching elements", async () => {
        const page = makePage(); // fetchElementsByRange already resolves []
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(page);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000)]);

        const result = await book.queryByRank(["g1"], 0, 10);

        assert.deepEqual(result, []);
    });

    it("deduplicates group keys before passing them to the page's fetchElementsByRange", async () => {
        const page = makePage();
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(page);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000)]);

        await book.queryByRank(["g1", "g1", "g2", "g2"], 0, 10);

        assert.equal(page.fetchElementsByRange.calledOnceWithExactly(["g1", "g2"], 0, 10, 1000), true);
    });

    it("passes startInclusiveRank and endExclusiveRank verbatim to each page", async () => {
        const page = makePage();
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(page);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000)]);

        await book.queryByRank(["g1"], 3, 17);

        assert.equal(page.fetchElementsByRange.calledOnceWithExactly(["g1"], 3, 17, 1000), true);
    });

    it("uses the default maxElementsPerGroup of 1000 when not supplied", async () => {
        const page = makePage();
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(page);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000)]);

        await book.queryByRank(["g1"], 0, 10); // 4th arg omitted

        assert.equal(page.fetchElementsByRange.firstCall.args[3], 1000);
    });

    it("passes an explicit maxElementsPerGroup to the page's fetchElementsByRange", async () => {
        const page = makePage();
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(page);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000)]);

        await book.queryByRank(["g1"], 0, 10, 42);

        assert.equal(page.fetchElementsByRange.calledOnceWithExactly(["g1"], 0, 10, 42), true);
    });

    // ── MULTI-PAGE AGGREGATION ────────────────────────────────────────────────

    it("returns distinct elements from multiple pages when their dim hashes do not overlap", async () => {
        // elemSensorA and elemSensorB have different dim hashes — both must appear in result
        const page1 = makePage(); page1.fetchElementsByRange.resolves([elemSensorA_v1]);
        const page2 = makePage(); page2.fetchElementsByRange.resolves([elemSensorB]);
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(page1);
        pageFactory.onSecondCall().resolves(page2);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        const result = await book.queryByRank(["g1"], 0, 10);

        assert.deepEqual(sortElements(result), sortElements([elemSensorA_v1, elemSensorB]));
    });

    it("later page value replaces earlier page value when both share the same dimension hash (update scenario)", async () => {
        // elemSensorA_v1 and elemSensorA_v2 share dim { sensor: "A" } → identical hash
        // page2 is chronologically later so its value must win
        const page1 = makePage(); page1.fetchElementsByRange.resolves([elemSensorA_v1]);
        const page2 = makePage(); page2.fetchElementsByRange.resolves([elemSensorA_v2]);
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(page1);
        pageFactory.onSecondCall().resolves(page2);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        const result = await book.queryByRank(["g1"], 0, 10);

        assert.deepEqual(result, [elemSensorA_v2]);
    });

    it("retains an earlier page's value for dimensions absent from the later page", async () => {
        // page1: A (old), B  — page2: A (new)  — B must be kept from page1
        const page1 = makePage(); page1.fetchElementsByRange.resolves([elemSensorA_v1, elemSensorB]);
        const page2 = makePage(); page2.fetchElementsByRange.resolves([elemSensorA_v2]);
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(page1);
        pageFactory.onSecondCall().resolves(page2);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        const result = await book.queryByRank(["g1"], 0, 10);

        assert.deepEqual(sortElements(result), sortElements([elemSensorA_v2, elemSensorB]));
    });

    it("accumulates new dimensions introduced by later pages alongside prior retained values", async () => {
        // page1: A (old)  — page2: A (new), B  — result must be: A (new), B
        const page1 = makePage(); page1.fetchElementsByRange.resolves([elemSensorA_v1]);
        const page2 = makePage(); page2.fetchElementsByRange.resolves([elemSensorA_v2, elemSensorB]);
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(page1);
        pageFactory.onSecondCall().resolves(page2);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        const result = await book.queryByRank(["g1"], 0, 10);

        assert.deepEqual(sortElements(result), sortElements([elemSensorA_v2, elemSensorB]));
    });

    it("correctly resolves three-page chain: mixed updates and new dimensions across all pages", async () => {
        // page1: A_v1, B_v1   page2: A_v2, C   page3: B_updated
        // Expected: A from page2, B from page3, C from page2
        const elemB_updated: IDimensionalElement = { dim: { sensor: "B" }, pld: { value: 99 } };
        const page1 = makePage(); page1.fetchElementsByRange.resolves([elemSensorA_v1, elemSensorB]);
        const page2 = makePage(); page2.fetchElementsByRange.resolves([elemSensorA_v2, elemSensorC]);
        const page3 = makePage(); page3.fetchElementsByRange.resolves([elemB_updated]);
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(page1);
        pageFactory.onSecondCall().resolves(page2);
        pageFactory.onThirdCall().resolves(page3);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [
            makePageInfo("pk1", 1000),
            makePageInfo("pk2", 2000),
            makePageInfo("pk3", 3000)
        ]);

        const result = await book.queryByRank(["g1"], 0, 10);

        assert.deepEqual(sortElements(result), sortElements([elemSensorA_v2, elemB_updated, elemSensorC]));
    });

    it("sends the same deduplicated group keys to every page when querying multiple pages", async () => {
        const page1 = makePage(); page1.fetchElementsByRange.resolves([elemSensorA_v1]);
        const page2 = makePage(); page2.fetchElementsByRange.resolves([elemSensorB]);
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(page1);
        pageFactory.onSecondCall().resolves(page2);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        await book.queryByRank(["gr-x", "gr-y", "gr-x"], 0, 10); // "gr-x" appears twice

        assert.equal(page1.fetchElementsByRange.calledOnceWithExactly(["gr-x", "gr-y"], 0, 10, 1000), true);
        assert.equal(page2.fetchElementsByRange.calledOnceWithExactly(["gr-x", "gr-y"], 0, 10, 1000), true);
    });

    it("replaces the entire element group when a later page returns a different-sized array for the same hash", async () => {
        // Two elements on page1 share the same hash because we inject a custom hashFunction.
        // page2 returns only one element for that same hash — the whole group is replaced.
        const collidingHash = (_el: IDimensionalElement) => "collide";

        const page1 = makePage(); page1.fetchElementsByRange.resolves([elemSensorA_v1, elemSensorB]); // 2 elems → "collide"
        const page2 = makePage(); page2.fetchElementsByRange.resolves([elemSensorC]);                  // 1 elem  → "collide"
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(page1);
        pageFactory.onSecondCall().resolves(page2);
        const { book, redisDriver } = makeBook({ pageFactory, hashFunction: collidingHash });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        const result = await book.queryByRank(["g1"], 0, 10);

        // page2's single element takes over the entire bucket for "collide"
        assert.deepEqual(result, [elemSensorC]);
    });

    // ── NULL / SKIPPED PAGES ──────────────────────────────────────────────────

    it("skips a page when the page factory resolves with null and continues with remaining pages", async () => {
        // page1 factory returns null (e.g. evicted/missing) — its elements must not appear
        const goodPage = makePage(); goodPage.fetchElementsByRange.resolves([elemSensorA_v1]);
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(null as unknown as PageStub);  // null → skip
        pageFactory.onSecondCall().resolves(goodPage);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        const result = await book.queryByRank(["g1"], 0, 10);

        assert.deepEqual(result, [elemSensorA_v1]);
        assert.equal(goodPage.fetchElementsByRange.calledOnce, true);
    });

    it("returns an empty array when all pages resolve to null", async () => {
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.resolves(null as unknown as PageStub); // every call → null
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        const result = await book.queryByRank(["g1"], 0, 10);

        assert.deepEqual(result, []);
    });

    // ── FAILURE PROPAGATION ───────────────────────────────────────────────────

    it("propagates failures that occur while listing pages", async () => {
        const { book, redisDriver } = makeBook();
        redisDriver.usingRedisDriver.rejects(new Error("redis connection refused"));

        await assert.rejects(
            book.queryByRank(["g1"], 0, 10),
            /redis connection refused/i
        );
    });

    it("propagates failures thrown by the page factory", async () => {
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>()
            .rejects(new Error("factory exploded"));
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000)]);

        await assert.rejects(
            book.queryByRank(["g1"], 0, 10),
            /factory exploded/i
        );
    });

    it("propagates failures thrown by fetchElementsByRange on a page", async () => {
        const page = makePage();
        page.fetchElementsByRange.rejects(new Error("page read failed"));
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(page);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000)]);

        await assert.rejects(
            book.queryByRank(["g1"], 0, 10),
            /page read failed/i
        );
    });

    it("propagates the first failure when multiple pages fail concurrently", async () => {
        const page1 = makePage(); page1.fetchElementsByRange.rejects(new Error("page1 failed"));
        const page2 = makePage(); page2.fetchElementsByRange.rejects(new Error("page2 failed"));
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(page1);
        pageFactory.onSecondCall().resolves(page2);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        // Promise.all rejects with the first settled rejection
        await assert.rejects(
            book.queryByRank(["g1"], 0, 10),
            /page\d failed/i
        );
    });
});
