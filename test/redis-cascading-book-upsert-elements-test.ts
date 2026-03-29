import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import sinon from "sinon";

import { RedisCascadingBook } from "../src/index.js";
import type { IDimensionalElement, IPage, IPageInfo, IRDriver, IKeyBuilder } from "../src/index.js";
import { Utilities } from "../src/utilities.js";

const VALID_CAPACITY = 10;
const VALID_PAGE_SIZE_BYTES = 100;
const VALID_ACTIVE_TIME_MS = 5_000;
const VALID_PAGE_TYPE = "test-page";
const HARMONIZED_TIME = 10_000;
const HOST_TIME = 17_356_896_001_23;

type DriverStub = IRDriver & {
    usingRedisDriver: sinon.SinonStub;
    harmonizedTimeInMs: sinon.SinonStub;
    initialize: sinon.SinonStub;
};

type PageStub = IPage & { //So that we can mock this type instead of interface.
    upsertElements: sinon.SinonStub;
};

function makeElements(): IDimensionalElement[] {
    return [
        { dim: { sensor: "A", rank: 1 }, pld: { value: 10 } },
        { dim: { sensor: "B", rank: 2 }, pld: { value: 11 } }
    ];
}

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
        harmonizedTimeInMs: sinon.stub().returns(HARMONIZED_TIME)
    } as unknown as DriverStub;
}

function makeKeyBuilder(): IKeyBuilder {
    return {
        counterKey: sinon.stub().returns("counter-key"),
        pageKey: sinon.stub().callsFake((timeKeyPart: string, sizeKeyPart: string, writeKeyPart: string) => `page:${timeKeyPart}:${sizeKeyPart}:${writeKeyPart}`),
        bookKey: sinon.stub().returns("book-key"),
        dimensionKey: sinon.stub().callsFake((pageKey: string, tagName: string) => `${pageKey}:${tagName}`),
        pageDimensionsDict: sinon.stub().callsFake((pageKey: string) => `${pageKey}:groups`)
    };
}

function makePageFactory(page: PageStub) {
    return sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(page);
}

function makeReconcileCallback() {
    return sinon.stub<[IPageInfo | undefined, IPageInfo[]], Promise<void>>().resolves();
}

function makeBook(overrides: Partial<{
    redisDriver: DriverStub;
    sizeEstimator: (elements: IDimensionalElement[]) => number;
    keyBuilder: IKeyBuilder;
    page: PageStub;
    pageFactory: (pageInfo: IPageInfo, pageType: string) => Promise<PageStub>;
    pagesReconcileCallback: (newPageInfo: IPageInfo | undefined, evictedPageInfo: IPageInfo[]) => Promise<void>;
}> = {}) {
    const redisDriver = overrides.redisDriver ?? makeDriver();
    const page = overrides.page ?? makePage();
    const pageFactory = overrides.pageFactory ?? makePageFactory(page);
    const pagesReconcileCallback = overrides.pagesReconcileCallback ?? makeReconcileCallback();
    const keyBuilder = overrides.keyBuilder ?? makeKeyBuilder();
    const sizeEstimator = overrides.sizeEstimator ?? (() => 50);

    const book = new RedisCascadingBook(
        VALID_CAPACITY,
        VALID_PAGE_SIZE_BYTES,
        VALID_ACTIVE_TIME_MS,
        VALID_PAGE_TYPE,
        pageFactory,
        pagesReconcileCallback,
        redisDriver,
        sizeEstimator,
        keyBuilder
    );

    return { book, redisDriver, page, pageFactory, pagesReconcileCallback, keyBuilder, sizeEstimator };
}

afterEach(() => {
    sinon.restore();
});

describe("RedisCascadingBook.upsertElements", () => {

    it("throws when the elements array is empty", async () => {
        const { book } = makeBook();

        await assert.rejects(
            book.upsertElements([]),
            /Number of elements must be between 1 and/i
        );
    });

    it("throws when the elements count exceeds u48In3", async () => {
        const { book } = makeBook();
        const oversizedElements = { length: Utilities.u48In3 + 1 } as unknown as IDimensionalElement[];

        await assert.rejects(
            book.upsertElements(oversizedElements),
            /Number of elements must be between 1 and/i
        );
    });

    it("throws when the estimated size is zero", async () => {
        const { book } = makeBook({ sizeEstimator: () => 0 });

        await assert.rejects(
            book.upsertElements(makeElements()),
            /Estimated size must be between 1 and/i
        );
    });

    it("throws when the estimated size is negative", async () => {
        const { book } = makeBook({ sizeEstimator: () => -1 });

        await assert.rejects(
            book.upsertElements(makeElements()),
            /Estimated size must be between 1 and/i
        );
    });

    it("throws when the estimated size exceeds u48In3", async () => {
        const { book } = makeBook({ sizeEstimator: () => Utilities.u48In3 + 1 });

        await assert.rejects(
            book.upsertElements(makeElements()),
            /Estimated size must be between 1 and/i
        );
    });

    it("upserts elements into the writable page and reconciles when a new page is added", async () => {
        const page = makePage();
        const { book, redisDriver, pageFactory, pagesReconcileCallback } = makeBook({ page });
        const elements = makeElements();
        const expectedPageInfo: IPageInfo = {
            pageKey: "page:10000:200:0",
            startTime: 10_000,
            startSize: 200,
            startSerialNumber: 0
        };

        redisDriver.usingRedisDriver.onFirstCall().resolves([
            "OK",
            ["10000", "250", "7"],
            1
        ]);
        redisDriver.usingRedisDriver.onSecondCall().resolves([1, []]);
        sinon.stub(Date, "now").returns(HOST_TIME);

        await assert.doesNotReject(book.upsertElements(elements));

        assert.equal(redisDriver.harmonizedTimeInMs.calledOnceWithExactly(HOST_TIME), true);
        assert.equal(redisDriver.usingRedisDriver.calledTwice, true);
        assert.equal(redisDriver.usingRedisDriver.firstCall.args[1], "IncrementCounter");
        assert.equal(redisDriver.usingRedisDriver.firstCall.args[2], "pipeline");
        assert.equal(redisDriver.usingRedisDriver.secondCall.args[1], "UpdateBookForNewPage");
        assert.equal(redisDriver.usingRedisDriver.secondCall.args[2], "pipeline");
        assert.equal((pageFactory as sinon.SinonStub).calledOnceWithExactly(expectedPageInfo, VALID_PAGE_TYPE), true);
        assert.equal((pagesReconcileCallback as sinon.SinonStub).calledOnceWithExactly(expectedPageInfo, []), true);
        assert.equal(page.upsertElements.calledOnceWithExactly(elements, 5), true);
    });

    it("reconciles trimmed pages when no new page is added", async () => {
        const page = makePage();
        const { book, redisDriver, pagesReconcileCallback } = makeBook({ page });
        const elements = makeElements();
        const trimmedPage: IPageInfo = {
            pageKey: "page:5000:0:0",
            startTime: 5_000,
            startSize: 0,
            startSerialNumber: 0
        };

        redisDriver.usingRedisDriver.onFirstCall().resolves([
            "OK",
            ["10000", "250", "7"],
            1
        ]);
        redisDriver.usingRedisDriver.onSecondCall().resolves([0, [JSON.stringify(trimmedPage)]]);
        sinon.stub(Date, "now").returns(HOST_TIME);

        await book.upsertElements(elements);

        assert.equal((pagesReconcileCallback as sinon.SinonStub).calledOnceWithExactly(undefined, [trimmedPage]), true);
        assert.equal(page.upsertElements.calledOnceWithExactly(elements, 5), true);
    });

    it("does not reconcile when neither a new page nor trimmed pages are produced", async () => {
        const page = makePage();
        const { book, redisDriver, pagesReconcileCallback } = makeBook({ page });
        const elements = makeElements();

        redisDriver.usingRedisDriver.onFirstCall().resolves([
            "OK",
            ["10000", "250", "7"],
            1
        ]);
        redisDriver.usingRedisDriver.onSecondCall().resolves([0, []]);
        sinon.stub(Date, "now").returns(HOST_TIME);

        await book.upsertElements(elements);

        assert.equal((pagesReconcileCallback as sinon.SinonStub).called, false);
        assert.equal(page.upsertElements.calledOnceWithExactly(elements, 5), true);
    });

    it("uses the default size estimator when one is not provided", async () => {
        const redisDriver = makeDriver();
        const page = makePage();
        const pageFactory = makePageFactory(page);
        const pagesReconcileCallback = makeReconcileCallback();
        const keyBuilder = makeKeyBuilder();
        const book = new RedisCascadingBook(
            VALID_CAPACITY,
            VALID_PAGE_SIZE_BYTES,
            VALID_ACTIVE_TIME_MS,
            VALID_PAGE_TYPE,
            pageFactory,
            pagesReconcileCallback,
            redisDriver,
            undefined,
            keyBuilder
        );

        redisDriver.usingRedisDriver.onFirstCall().resolves([
            "OK",
            ["10000", "1", "1"],
            1
        ]);
        redisDriver.usingRedisDriver.onSecondCall().resolves([0, []]);
        sinon.stub(Date, "now").returns(HOST_TIME);

        await book.upsertElements([{ dim: { sensor: "A" }, pld: { value: 1 } }]);

        assert.equal(page.upsertElements.calledOnceWithExactly([{ dim: { sensor: "A" }, pld: { value: 1 } }], 0), true);
    });

    it("propagates page upsert failures after the writeable page is resolved", async () => {
        const page = makePage();
        page.upsertElements.rejects(new Error("page write failed"));
        const { book, redisDriver } = makeBook({ page });
        const elements = makeElements();

        redisDriver.usingRedisDriver.onFirstCall().resolves([
            "OK",
            ["10000", "250", "7"],
            1
        ]);
        redisDriver.usingRedisDriver.onSecondCall().resolves([0, []]);
        sinon.stub(Date, "now").returns(HOST_TIME);

        await assert.rejects(
            book.upsertElements(elements),
            /page write failed/i
        );

        assert.equal(redisDriver.harmonizedTimeInMs.calledOnceWithExactly(HOST_TIME), true);
        assert.equal(redisDriver.usingRedisDriver.calledTwice, true);
        assert.equal(redisDriver.usingRedisDriver.firstCall.args[1], "IncrementCounter");
        assert.equal(redisDriver.usingRedisDriver.firstCall.args[2], "pipeline");
        assert.equal(redisDriver.usingRedisDriver.secondCall.args[1], "UpdateBookForNewPage");
        assert.equal(redisDriver.usingRedisDriver.secondCall.args[2], "pipeline");

        // Even though page upsert fails, sequence counter should already be incremented and passed through.
        assert.equal(page.upsertElements.calledOnceWithExactly(elements, 5), true);
    });

    it("throws when IncrementCounter pipeline response shape is invalid", async () => {
        const page = makePage();
        const { book, redisDriver } = makeBook({ page });
        sinon.stub(Date, "now").returns(HOST_TIME);

        // Missing BITFIELD payload array at response[1]
        redisDriver.usingRedisDriver.onFirstCall().resolves(["OK"] as any);

        await assert.rejects(
            book.upsertElements(makeElements()),
            /Invalid IncrementCounter response shape/i
        );
        assert.equal(page.upsertElements.called, false);
    });

    it("throws when IncrementCounter returns non-numeric counter values", async () => {
        const page = makePage();
        const { book, redisDriver } = makeBook({ page });
        sinon.stub(Date, "now").returns(HOST_TIME);

        // null in counter slots should fail integer parsing
        redisDriver.usingRedisDriver.onFirstCall().resolves(["OK", ["10000", null, "7"], 1] as any);

        await assert.rejects(
            book.upsertElements(makeElements()),
            /Invalid redis integer response for IncrementCounter\.size/i
        );
        assert.equal(page.upsertElements.called, false);
    });

    it("throws when UpdateBookForNewPage response shape is invalid", async () => {
        const page = makePage();
        const { book, redisDriver } = makeBook({ page });
        sinon.stub(Date, "now").returns(HOST_TIME);

        redisDriver.usingRedisDriver.onFirstCall().resolves(["OK", ["10000", "250", "7"], 1]);
        redisDriver.usingRedisDriver.onSecondCall().resolves("not-an-array" as any);

        await assert.rejects(
            book.upsertElements(makeElements()),
            /Invalid UpdateBookForNewPage response shape/i
        );
        assert.equal(page.upsertElements.called, false);
    });

    it("accepts string numeric new-page flags from UpdateBookForNewPage", async () => {
        const page = makePage();
        const { book, redisDriver, pagesReconcileCallback } = makeBook({ page });
        const elements = makeElements();
        sinon.stub(Date, "now").returns(HOST_TIME);

        redisDriver.usingRedisDriver.onFirstCall().resolves(["OK", ["10000", "250", "7"], 1]);
        redisDriver.usingRedisDriver.onSecondCall().resolves(["1", []]);

        await book.upsertElements(elements);

        assert.equal((pagesReconcileCallback as sinon.SinonStub).calledOnce, true);
        assert.equal(page.upsertElements.calledOnce, true);
    });

    it("treats non-array trimmed page payloads as empty list", async () => {
        const page = makePage();
        const { book, redisDriver, pagesReconcileCallback } = makeBook({ page });
        const elements = makeElements();
        sinon.stub(Date, "now").returns(HOST_TIME);

        redisDriver.usingRedisDriver.onFirstCall().resolves(["OK", ["10000", "250", "7"], 1]);
        // new page flag false, but trimmed payload malformed; code should coerce to []
        redisDriver.usingRedisDriver.onSecondCall().resolves([0, "malformed-trimmed-payload"] as any);

        await book.upsertElements(elements);

        assert.equal((pagesReconcileCallback as sinon.SinonStub).called, false);
        assert.equal(page.upsertElements.calledOnce, true);
    });

    // ── UPDATE SCENARIOS ──────────────────────────────────────────────────────

    it("second upsert (update) on the same page uses the incremented sequence number", async () => {
        // Simulates writing v1 of elements, then writing v2 (an update) — both land on the
        // same page window so pageStartSerialNumber stays 0, but sequenceStartNumber advances.
        const page = makePage();
        const { book, redisDriver } = makeBook({ page });
        const v1Elements = makeElements();
        const v2Elements: IDimensionalElement[] = [
            { dim: { sensor: "A", rank: 1 }, pld: { value: 99 } }, // updated payload
            { dim: { sensor: "B", rank: 2 }, pld: { value: 100 } }
        ];
        sinon.stub(Date, "now").returns(HOST_TIME);

        // ── First write (v1) ──
        // BITFIELD: time=10000, sizeCounter=50, writeCounter=2 → previousSerial=0, seq=0
        redisDriver.usingRedisDriver.onCall(0).resolves(["OK", ["10000", "50", "2"], 1]);
        redisDriver.usingRedisDriver.onCall(1).resolves([1, []]); // new page added

        // ── Second write (v2) ──
        // Counters are cumulative: sizeCounter=100 (+50), writeCounter=4 (+2)
        // previousSize=50, pageStartSize=modMinus(50,100)=0 → same page
        // previousSerial=2, pageStartSerial=modMinus(2,u48In3)=0 → same page
        // sequenceStartNumber = 2
        redisDriver.usingRedisDriver.onCall(2).resolves(["OK", ["10000", "100", "4"], 1]);
        redisDriver.usingRedisDriver.onCall(3).resolves([0, []]); // already exists, no new page

        await book.upsertElements(v1Elements);
        await book.upsertElements(v2Elements);

        assert.equal(page.upsertElements.callCount, 2);
        assert.equal(page.upsertElements.firstCall.args[1], 0);  // sequenceStart for v1
        assert.equal(page.upsertElements.secondCall.args[1], 2); // sequenceStart for v2
        assert.deepEqual(page.upsertElements.firstCall.args[0], v1Elements);
        assert.deepEqual(page.upsertElements.secondCall.args[0], v2Elements);
    });

    it("update that lands on a new page triggers reconcile with the new page info", async () => {
        // First write lands on page window A; second write (the update) opens a new time window
        // (page window B) — reconcile must fire with pageInfo for B.
        const page = makePage();
        const { book, redisDriver, pagesReconcileCallback } = makeBook({ page });
        const v1Elements = makeElements();
        const v2Elements: IDimensionalElement[] = [
            { dim: { sensor: "A", rank: 1 }, pld: { value: 99 } },
            { dim: { sensor: "B", rank: 2 }, pld: { value: 100 } }
        ];
        sinon.stub(Date, "now").returns(HOST_TIME);

        // ── First write: page window starting at time 5000 ──
        redisDriver.usingRedisDriver.onCall(0).resolves(["OK", ["5000", "50", "2"], 1]);
        redisDriver.usingRedisDriver.onCall(1).resolves([1, []]); // new page

        // ── Second write: Redis key has expired → new time window starting at 10000 ──
        // sizeCounter=50 (+50 from new window start), writeCounter=4 (+2)
        // previousSize=0, pageStartSize=modMinus(0,100)=0
        // previousSerial=2, pageStartSerial=modMinus(2,u48In3)=0
        redisDriver.usingRedisDriver.onCall(2).resolves(["OK", ["10000", "50", "4"], 1]);
        redisDriver.usingRedisDriver.onCall(3).resolves([1, []]); // another new page

        await book.upsertElements(v1Elements);
        await book.upsertElements(v2Elements);

        assert.equal((pagesReconcileCallback as sinon.SinonStub).callCount, 2);
        // First reconcile: new page at time 5000
        const firstReconcileArg = (pagesReconcileCallback as sinon.SinonStub).firstCall.args[0] as IPageInfo;
        assert.equal(firstReconcileArg.startTime, 5000);
        // Second reconcile: new page at time 10000
        const secondReconcileArg = (pagesReconcileCallback as sinon.SinonStub).secondCall.args[0] as IPageInfo;
        assert.equal(secondReconcileArg.startTime, 10000);
        assert.equal(page.upsertElements.secondCall.args[1], 2); // sequenceStart for v2 within new window
    });

    it("update that causes page capacity trim evicts the oldest page and notifies via reconcile", async () => {
        // Capacity is VALID_CAPACITY (10 pages). Simulate the second write pushing the book over
        // capacity so that trimmedPages contains one evicted entry.
        const page = makePage();
        const evictedPage: IPageInfo = { pageKey: "page:0:0:0", startTime: 0, startSize: 0, startSerialNumber: 0 };
        const { book, redisDriver, pagesReconcileCallback } = makeBook({ page });
        const v1Elements = makeElements();
        const v2Elements: IDimensionalElement[] = [
            { dim: { sensor: "A", rank: 1 }, pld: { value: 99 } },
            { dim: { sensor: "B", rank: 2 }, pld: { value: 100 } }
        ];
        sinon.stub(Date, "now").returns(HOST_TIME);

        // First write — normal new page, no trim
        redisDriver.usingRedisDriver.onCall(0).resolves(["OK", ["10000", "50", "2"], 1]);
        redisDriver.usingRedisDriver.onCall(1).resolves([1, []]);

        // Second write (update) — page already existed (ZADD returns 0) but trim fires
        redisDriver.usingRedisDriver.onCall(2).resolves(["OK", ["10000", "100", "4"], 1]);
        redisDriver.usingRedisDriver.onCall(3).resolves([0, [JSON.stringify(evictedPage)]]);

        await book.upsertElements(v1Elements);
        await book.upsertElements(v2Elements);

        assert.equal((pagesReconcileCallback as sinon.SinonStub).callCount, 2);
        // Second reconcile: no new page (undefined) but one trimmed page
        const secondReconcileNewPage = (pagesReconcileCallback as sinon.SinonStub).secondCall.args[0];
        const secondReconcileTrimmed = (pagesReconcileCallback as sinon.SinonStub).secondCall.args[1] as IPageInfo[];
        assert.equal(secondReconcileNewPage, undefined);
        assert.deepEqual(secondReconcileTrimmed, [evictedPage]);
    });

    it("update propagates failure of the second IncrementCounter call and does not write to page", async () => {
        const page = makePage();
        const { book, redisDriver } = makeBook({ page });
        sinon.stub(Date, "now").returns(HOST_TIME);

        // First write succeeds
        redisDriver.usingRedisDriver.onCall(0).resolves(["OK", ["10000", "50", "2"], 1]);
        redisDriver.usingRedisDriver.onCall(1).resolves([0, []]);
        // Update (second write) — IncrementCounter fails (e.g. redis overflow or disconnect)
        redisDriver.usingRedisDriver.onCall(2).rejects(new Error("counter overflow"));

        await book.upsertElements(makeElements());
        await assert.rejects(
            book.upsertElements(makeElements()),
            /counter overflow/i
        );

        // page.upsertElements must only have been called for the first successful write
        assert.equal(page.upsertElements.callCount, 1);
    });

    it("update propagates failure of the second UpdateBookForNewPage call and does not write to page", async () => {
        const page = makePage();
        const { book, redisDriver } = makeBook({ page });
        sinon.stub(Date, "now").returns(HOST_TIME);

        // First write succeeds
        redisDriver.usingRedisDriver.onCall(0).resolves(["OK", ["10000", "50", "2"], 1]);
        redisDriver.usingRedisDriver.onCall(1).resolves([0, []]);
        // Update: IncrementCounter succeeds, UpdateBookForNewPage fails
        redisDriver.usingRedisDriver.onCall(2).resolves(["OK", ["10000", "100", "4"], 1]);
        redisDriver.usingRedisDriver.onCall(3).rejects(new Error("bookkeeping failed"));

        await book.upsertElements(makeElements());
        await assert.rejects(
            book.upsertElements(makeElements()),
            /bookkeeping failed/i
        );

        // page.upsertElements must only have been called for the first successful write
        assert.equal(page.upsertElements.callCount, 1);
    });
});