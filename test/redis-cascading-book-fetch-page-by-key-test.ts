import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import sinon from "sinon";

import { RedisCascadingBook } from "../src/index.js";
import type { IPage, IPageInfo, IRDriver, IKeyBuilder } from "../src/index.js";

const VALID_CAPACITY = 10;
const VALID_PAGE_SIZE_BYTES = 100;
const VALID_ACTIVE_TIME_MS = 5_000;
const VALID_PAGE_TYPE = "test-page";

type DriverStub = IRDriver & {
    usingRedisDriver: sinon.SinonStub;
    harmonizedTimeInMs: sinon.SinonStub;
    initialize: sinon.SinonStub;
};

type PageStub = IPage & {
    upsertElements: sinon.SinonStub;
    fetchElementsByRange: sinon.SinonStub;
};

function makePage(overrides: Partial<IPageInfo> = {}): PageStub {
    return {
        info: {
            pageKey: overrides.pageKey ?? "page-default",
            startTime: overrides.startTime ?? 1_000,
            startSize: overrides.startSize ?? 0,
            startSerialNumber: overrides.startSerialNumber ?? 0
        },
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

function makePageInfo(overrides: Partial<IPageInfo> = {}): IPageInfo {
    return {
        pageKey: overrides.pageKey ?? "page-1",
        startTime: overrides.startTime ?? 10_000,
        startSize: overrides.startSize ?? 200,
        startSerialNumber: overrides.startSerialNumber ?? 5
    };
}

function makeBook(overrides: Partial<{
    redisDriver: DriverStub;
    keyBuilder: IKeyBuilder;
    pageFactory: (pageInfo: IPageInfo, pageType: string) => Promise<PageStub>;
    pagesReconcileCallback: (newPageInfo: IPageInfo | undefined, evictedPageInfo: IPageInfo[]) => Promise<void>;
}> = {}) {
    const redisDriver = overrides.redisDriver ?? makeDriver();
    const pageFactory = overrides.pageFactory
        ?? sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(makePage());
    const pagesReconcileCallback = overrides.pagesReconcileCallback
        ?? sinon.stub<[IPageInfo | undefined, IPageInfo[]], Promise<void>>().resolves();
    const keyBuilder = overrides.keyBuilder ?? makeKeyBuilder();

    const book = new RedisCascadingBook(
        VALID_CAPACITY,
        VALID_PAGE_SIZE_BYTES,
        VALID_ACTIVE_TIME_MS,
        VALID_PAGE_TYPE,
        pageFactory,
        pagesReconcileCallback,
        redisDriver,
        () => 1,
        keyBuilder
    );

    return {
        book,
        redisDriver,
        pageFactory: pageFactory as sinon.SinonStub,
        pagesReconcileCallback: pagesReconcileCallback as sinon.SinonStub
    };
}

afterEach(() => {
    sinon.restore();
});

describe("RedisCascadingBook.fetchPageByKey", () => {
    it("returns the page produced by pageFactory", async () => {
        const expectedPage = makePage({ pageKey: "page-returned" });
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(expectedPage);
        const { book } = makeBook({ pageFactory });
        const pageInfo = makePageInfo();

        const result = await book.fetchPageByKey(pageInfo);

        assert.equal(result, expectedPage);
    });

    it("forwards pageInfo and pageType to pageFactory exactly", async () => {
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(makePage());
        const { book } = makeBook({ pageFactory });
        const pageInfo = makePageInfo({
            pageKey: "page:with:special",
            startTime: 17_356_896_001,
            startSize: 999,
            startSerialNumber: 123
        });

        await book.fetchPageByKey(pageInfo);

        assert.equal(pageFactory.calledOnceWithExactly(pageInfo, VALID_PAGE_TYPE), true);
    });

    it("calls pageFactory once per invocation for sequential fetches", async () => {
        const page1 = makePage({ pageKey: "p1" });
        const page2 = makePage({ pageKey: "p2" });
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
        pageFactory.onFirstCall().resolves(page1);
        pageFactory.onSecondCall().resolves(page2);

        const { book } = makeBook({ pageFactory });
        const pageInfo1 = makePageInfo({ pageKey: "p1" });
        const pageInfo2 = makePageInfo({ pageKey: "p2", startTime: 20_000 });

        const result1 = await book.fetchPageByKey(pageInfo1);
        const result2 = await book.fetchPageByKey(pageInfo2);

        assert.equal(pageFactory.callCount, 2);
        assert.equal(pageFactory.firstCall.calledWithExactly(pageInfo1, VALID_PAGE_TYPE), true);
        assert.equal(pageFactory.secondCall.calledWithExactly(pageInfo2, VALID_PAGE_TYPE), true);
        assert.equal(result1, page1);
        assert.equal(result2, page2);
    });

    it("does not call Redis driver when fetching page by key", async () => {
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(makePage());
        const { book, redisDriver } = makeBook({ pageFactory });

        await book.fetchPageByKey(makePageInfo());

        assert.equal(redisDriver.usingRedisDriver.called, false);
    });

    it("does not call reconcile callback when fetching page by key", async () => {
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(makePage());
        const pagesReconcileCallback = sinon.stub<[IPageInfo | undefined, IPageInfo[]], Promise<void>>().resolves();
        const { book } = makeBook({ pageFactory, pagesReconcileCallback });

        await book.fetchPageByKey(makePageInfo());

        assert.equal(pagesReconcileCallback.called, false);
    });

    it("propagates rejection when pageFactory rejects", async () => {
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>()
            .rejects(new Error("page not found"));
        const { book } = makeBook({ pageFactory });

        await assert.rejects(
            book.fetchPageByKey(makePageInfo()),
            /page not found/i
        );
    });

    it("propagates synchronous throw from pageFactory", async () => {
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>()
            .throws(new Error("factory crashed synchronously"));
        const { book } = makeBook({ pageFactory });

        await assert.rejects(
            book.fetchPageByKey(makePageInfo()),
            /factory crashed synchronously/i
        );
    });

    it("returns null transparently if pageFactory resolves null", async () => {
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>()
            .resolves(null as unknown as PageStub);
        const { book } = makeBook({ pageFactory });

        const result = await book.fetchPageByKey(makePageInfo());

        assert.equal(result, null);
    });

    it("returns undefined transparently if pageFactory resolves undefined", async () => {
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>()
            .resolves(undefined as unknown as PageStub);
        const { book } = makeBook({ pageFactory });

        const result = await book.fetchPageByKey(makePageInfo());

        assert.equal(result, undefined);
    });

    it("supports fetching pages with boundary-sized page info values", async () => {
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(makePage({ pageKey: "max-page" }));
        const { book } = makeBook({ pageFactory });
        const maxU48 = 281_474_976_710_655;
        const boundaryPageInfo = makePageInfo({
            pageKey: "page:max",
            startTime: maxU48,
            startSize: maxU48,
            startSerialNumber: maxU48
        });

        const result = await book.fetchPageByKey(boundaryPageInfo);

        assert.equal(result.info.pageKey, "max-page");
        assert.equal(pageFactory.calledOnceWithExactly(boundaryPageInfo, VALID_PAGE_TYPE), true);
    });
});
