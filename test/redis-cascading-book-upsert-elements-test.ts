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
        groupKey: sinon.stub().callsFake((pageKey: string, tagName: string) => `${pageKey}:${tagName}`),
        groupListKey: sinon.stub().callsFake((pageKey: string) => `${pageKey}:groups`)
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

    const book = new RedisCascadingBook<PageStub>(
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
        const book = new RedisCascadingBook<PageStub>(
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
});