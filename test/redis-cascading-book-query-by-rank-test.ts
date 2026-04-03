import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import sinon from "sinon";

import { RedisCascadingBook } from "../src/index.js";
import type { IDimensionalElement, IDimensionalQuery, IPage, IPageInfo, IRDriver, IKeyBuilder } from "../src/index.js";

// -- constants ---------------------------------------------------------------
const VALID_CAPACITY = 10;
const VALID_PAGE_SIZE_BYTES = 100;
const VALID_ACTIVE_TIME_MS = 5_000;
const VALID_PAGE_TYPE = "test-page";

// -- element fixtures --------------------------------------------------------
const elemSensorA_v1: IDimensionalElement = { globalIdentityHash: "sensor-a", dim: { sensor: "A" }, pld: { value: 1 } };
const elemSensorA_v2: IDimensionalElement = { globalIdentityHash: "sensor-a", dim: { sensor: "A" }, pld: { value: 2 } };
const elemSensorB: IDimensionalElement = { globalIdentityHash: "sensor-b", dim: { sensor: "B" }, pld: { value: 3 } };
const elemSensorC: IDimensionalElement = { globalIdentityHash: "sensor-c", dim: { sensor: "C" }, pld: { value: 4 } };
const elemAnonymousA_v1: IDimensionalElement = { dim: { sensor: "anon-A" }, pld: { value: 11 } };
const elemAnonymousA_v2: IDimensionalElement = { dim: { sensor: "anon-A" }, pld: { value: 12 } };

// -- query fixtures ----------------------------------------------------------
const querySensorA: IDimensionalQuery = {
    query: { operator: "AND", conditions: [{ dimension: "sensor", operator: "eq", value: "A" }] }
};
const queryAllSensors: IDimensionalQuery = {
    query: { operator: "OR", conditions: [{ dimension: "sensor", operator: "noteq", value: "__never__" }] }
};

// -- type aliases ------------------------------------------------------------
type DriverStub = IRDriver & {
    usingRedisDriver: sinon.SinonStub;
    harmonizedTimeInMs: sinon.SinonStub;
    initialize: sinon.SinonStub;
};

type PageStub = IPage & {
    queryElementsByDimensions: sinon.SinonStub;
    upsertElements: sinon.SinonStub;
};

// -- stub factories ----------------------------------------------------------
function makePage(): PageStub {
    return {
        info: { pageKey: "unused", startTime: 0, startSize: 0, startSerialNumber: 0 },
        pageType: VALID_PAGE_TYPE,
        upsertElements: sinon.stub().resolves(),
        queryElementsByDimensions: sinon.stub().resolves([]),
        dumpPage: sinon.stub().resolves([]),
        fetchElementsByRange: sinon.stub().resolves([]),
        purgePage: sinon.stub().resolves()
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
}> = {}) {
    const redisDriver = overrides.redisDriver ?? makeDriver();
    const pagesReconcileCallback = overrides.pagesReconcileCallback
        ?? sinon.stub<[IPageInfo | undefined, IPageInfo[]], Promise<void>>().resolves();
    const keyBuilder = overrides.keyBuilder ?? makeKeyBuilder();
    const dummyPage = makePage();
    const pageFactory = overrides.pageFactory
        ?? sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(dummyPage);

    const book = new RedisCascadingBook(
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

// -- tests -------------------------------------------------------------------
afterEach(() => {
    sinon.restore();
});

describe("RedisCascadingBook.queryElementsByDimensions (migrated from queryByRank tests)", () => {

    // -- INPUT VALIDATION -----------------------------------------------------

    it("throws when maxElementsCount is zero", async () => {
        const { book } = makeBook();
        await assert.rejects(
            book.queryElementsByDimensions(querySensorA, 0),
            /Max elements count must be between 1 and 10000/i
        );
    });

    it("throws when maxElementsCount is negative", async () => {
        const { book } = makeBook();
        await assert.rejects(
            book.queryElementsByDimensions(querySensorA, -1),
            /Max elements count must be between 1 and 10000/i
        );
    });

    it("throws when maxElementsCount exceeds 10000", async () => {
        const { book } = makeBook();
        await assert.rejects(
            book.queryElementsByDimensions(querySensorA, 10_001),
            /Max elements count must be between 1 and 10000/i
        );
    });

    // -- SINGLE-PAGE DATA RETRIEVAL ------------------------------------------

    it("returns an empty array when no pages are stored", async () => {
        const { book, redisDriver } = makeBook();
        stubListPages(redisDriver, []);

        const result = await book.queryElementsByDimensions(querySensorA, 1000);

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
        page.queryElementsByDimensions.resolves([elemSensorA_v1, elemSensorB]);
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(page);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000)]);

        const result = await book.queryElementsByDimensions(queryAllSensors, 1000);

        assert.deepEqual(sortElements(result), sortElements([elemSensorA_v1, elemSensorB]));
        assert.equal(page.queryElementsByDimensions.calledOnceWithExactly(queryAllSensors, 1000), true);
    });

    it("returns an empty array when the page returns no matching elements", async () => {
        const page = makePage();
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(page);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000)]);

        const result = await book.queryElementsByDimensions(querySensorA, 1000);

        assert.deepEqual(result, []);
    });

    it("passes the query object verbatim to each page", async () => {
        const page = makePage();
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(page);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000)]);

        await book.queryElementsByDimensions(querySensorA, 42);

        assert.equal(page.queryElementsByDimensions.calledOnceWithExactly(querySensorA, 42), true);
    });

    it("uses the default maxElementsCount of 1000 when not supplied", async () => {
        const page = makePage();
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(page);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000)]);

        await book.queryElementsByDimensions(querySensorA);

        assert.equal(page.queryElementsByDimensions.firstCall.args[1], 1000);
    });

    it("passes an explicit maxElementsCount to page.queryElementsByDimensions", async () => {
        const page = makePage();
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(page);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000)]);

        await book.queryElementsByDimensions(querySensorA, 42);

        assert.equal(page.queryElementsByDimensions.calledOnceWithExactly(querySensorA, 42), true);
    });

    // -- MULTI-PAGE AGGREGATION ----------------------------------------------

    it("returns distinct elements from multiple pages when their global identity hashes do not overlap", async () => {
        const page1 = makePage(); page1.queryElementsByDimensions.resolves([elemSensorA_v1]);
        const page2 = makePage(); page2.queryElementsByDimensions.resolves([elemSensorB]);
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(page1);
        pageFactory.onSecondCall().resolves(page2);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        const result = await book.queryElementsByDimensions(queryAllSensors, 1000);

        assert.deepEqual(sortElements(result), sortElements([elemSensorA_v1, elemSensorB]));
    });

    it("later page value replaces earlier page value when both share the same globalIdentityHash", async () => {
        const page1 = makePage(); page1.queryElementsByDimensions.resolves([elemSensorA_v1]);
        const page2 = makePage(); page2.queryElementsByDimensions.resolves([elemSensorA_v2]);
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(page1);
        pageFactory.onSecondCall().resolves(page2);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        const result = await book.queryElementsByDimensions(queryAllSensors, 1000);

        assert.deepEqual(result, [elemSensorA_v2]);
    });

    it("retains an earlier page's value for dimensions absent from the later page", async () => {
        // page1: A (old), B  -- page2: A (new)  -- B must be kept from page1
        const page1 = makePage(); page1.queryElementsByDimensions.resolves([elemSensorA_v1, elemSensorB]);
        const page2 = makePage(); page2.queryElementsByDimensions.resolves([elemSensorA_v2]);
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(page1);
        pageFactory.onSecondCall().resolves(page2);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        const result = await book.queryElementsByDimensions(queryAllSensors, 1000);

        assert.deepEqual(sortElements(result), sortElements([elemSensorA_v2, elemSensorB]));
    });

    it("accumulates new dimensions introduced by later pages alongside prior retained values", async () => {
        // page1: A (old)  -- page2: A (new), B  -- result must be: A (new), B
        const page1 = makePage(); page1.queryElementsByDimensions.resolves([elemSensorA_v1]);
        const page2 = makePage(); page2.queryElementsByDimensions.resolves([elemSensorA_v2, elemSensorB]);
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(page1);
        pageFactory.onSecondCall().resolves(page2);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        const result = await book.queryElementsByDimensions(queryAllSensors, 1000);

        assert.deepEqual(sortElements(result), sortElements([elemSensorA_v2, elemSensorB]));
    });

    it("correctly resolves three-page chain: mixed updates and new dimensions across all pages", async () => {
        // page1: A_v1, B_v1   page2: A_v2, C   page3: B_updated
        // Expected: A from page2, B from page3, C from page2
        const elemB_updated: IDimensionalElement = { globalIdentityHash: "sensor-b", dim: { sensor: "B" }, pld: { value: 99 } };
        const page1 = makePage(); page1.queryElementsByDimensions.resolves([elemSensorA_v1, elemSensorB]);
        const page2 = makePage(); page2.queryElementsByDimensions.resolves([elemSensorA_v2, elemSensorC]);
        const page3 = makePage(); page3.queryElementsByDimensions.resolves([elemB_updated]);
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

        const result = await book.queryElementsByDimensions(queryAllSensors, 1000);

        assert.deepEqual(sortElements(result), sortElements([elemSensorA_v2, elemB_updated, elemSensorC]));
    });

    it("sends the same query object and maxElementsCount to every page when querying multiple pages", async () => {
        const page1 = makePage(); page1.queryElementsByDimensions.resolves([elemSensorA_v1]);
        const page2 = makePage(); page2.queryElementsByDimensions.resolves([elemSensorB]);
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(page1);
        pageFactory.onSecondCall().resolves(page2);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        await book.queryElementsByDimensions(querySensorA, 1000);

        assert.equal(page1.queryElementsByDimensions.calledOnceWithExactly(querySensorA, 1000), true);
        assert.equal(page2.queryElementsByDimensions.calledOnceWithExactly(querySensorA, 1000), true);
    });

    it("accumulates all elements from multiple pages when globalIdentityHash is missing", async () => {
        // Without an identity hash we cannot deduplicate -- every element must be kept from every page.
        const page1 = makePage(); page1.queryElementsByDimensions.resolves([elemAnonymousA_v1, elemSensorB]);
        const page2 = makePage(); page2.queryElementsByDimensions.resolves([elemAnonymousA_v2, elemSensorC]);
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(page1);
        pageFactory.onSecondCall().resolves(page2);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        const result = await book.queryElementsByDimensions(queryAllSensors, 1000);

        // elemAnonymousA_v1 AND elemAnonymousA_v2 are both kept; elemSensorB and elemSensorC are distinct-hash elements
        assert.deepEqual(sortElements(result), sortElements([elemAnonymousA_v1, elemAnonymousA_v2, elemSensorB, elemSensorC]));
    });

    // -- NULL / SKIPPED PAGES -------------------------------------------------

    it("skips a page when the page factory resolves with null and continues with remaining pages", async () => {
        // page1 factory returns null (e.g. evicted/missing) -- its elements must not appear
        const goodPage = makePage(); goodPage.queryElementsByDimensions.resolves([elemSensorA_v1]);
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(null as unknown as PageStub);
        pageFactory.onSecondCall().resolves(goodPage);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        const result = await book.queryElementsByDimensions(queryAllSensors, 1000);

        assert.deepEqual(result, [elemSensorA_v1]);
        assert.equal(goodPage.queryElementsByDimensions.calledOnce, true);
    });

    it("returns an empty array when all pages resolve to null", async () => {
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.resolves(null as unknown as PageStub);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        const result = await book.queryElementsByDimensions(queryAllSensors, 1000);

        assert.deepEqual(result, []);
    });

    // -- FAILURE PROPAGATION --------------------------------------------------

    it("propagates failures that occur while listing pages", async () => {
        const { book, redisDriver } = makeBook();
        redisDriver.usingRedisDriver.rejects(new Error("redis connection refused"));

        await assert.rejects(
            book.queryElementsByDimensions(querySensorA, 1000),
            /redis connection refused/i
        );
    });

    it("propagates failures thrown by the page factory", async () => {
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>()
            .rejects(new Error("factory exploded"));
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000)]);

        await assert.rejects(
            book.queryElementsByDimensions(querySensorA, 1000),
            /factory exploded/i
        );
    });

    it("propagates failures thrown by queryElementsByDimensions on a page", async () => {
        const page = makePage();
        page.queryElementsByDimensions.rejects(new Error("page read failed"));
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(page);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000)]);

        await assert.rejects(
            book.queryElementsByDimensions(querySensorA, 1000),
            /page read failed/i
        );
    });

    it("propagates the first failure when multiple pages fail concurrently", async () => {
        const page1 = makePage(); page1.queryElementsByDimensions.rejects(new Error("page1 failed"));
        const page2 = makePage(); page2.queryElementsByDimensions.rejects(new Error("page2 failed"));
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(page1);
        pageFactory.onSecondCall().resolves(page2);
        const { book, redisDriver } = makeBook({ pageFactory });
        stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

        await assert.rejects(
            book.queryElementsByDimensions(querySensorA, 1000),
            /page\d failed/i
        );
    });
});
