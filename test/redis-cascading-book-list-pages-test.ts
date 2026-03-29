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

function makePageInfo(pageKey: string, startTime: number, startSize: number, startSerialNumber: number): IPageInfo {
    return { pageKey, startTime, startSize, startSerialNumber };
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

    return { book, redisDriver, keyBuilder, pageFactory: pageFactory as sinon.SinonStub, pagesReconcileCallback: pagesReconcileCallback as sinon.SinonStub };
}

function serializePages(pages: IPageInfo[]): string[] {
    return pages.map((p) => JSON.stringify(p));
}

afterEach(() => {
    sinon.restore();
});

describe("RedisCascadingBook.listPages", () => {
    it("returns empty array when Redis returns no pages", async () => {
        const { book, redisDriver } = makeBook();
        redisDriver.usingRedisDriver.resolves([]);

        const result = await book.listPages();

        assert.deepEqual(result, []);
        assert.equal(redisDriver.usingRedisDriver.calledOnceWithExactly(
            [["zrange", "book-key", "0", "-1"]],
            "FetchAllPagesWithRanks",
            "run"
        ), true);
    });

    it("calls keyBuilder.bookKey exactly once", async () => {
        const keyBuilder = makeKeyBuilder();
        const { book, redisDriver } = makeBook({ keyBuilder });
        redisDriver.usingRedisDriver.resolves([]);

        await book.listPages();

        assert.equal((keyBuilder.bookKey as sinon.SinonStub).calledOnce, true);
    });

    it("parses a single serialized page and returns it", async () => {
        const { book, redisDriver } = makeBook();
        const page = makePageInfo("p1", 1000, 10, 1);
        redisDriver.usingRedisDriver.resolves(serializePages([page]));

        const result = await book.listPages();

        assert.deepEqual(result, [page]);
    });

    it("sorts by startTime ascending when startTime differs", async () => {
        const { book, redisDriver } = makeBook();
        const p1 = makePageInfo("p-late", 3000, 0, 0);
        const p2 = makePageInfo("p-early", 1000, 999, 999);
        const p3 = makePageInfo("p-mid", 2000, 500, 500);
        redisDriver.usingRedisDriver.resolves(serializePages([p1, p2, p3]));

        const result = await book.listPages();

        assert.deepEqual(result.map((p) => p.pageKey), ["p-early", "p-mid", "p-late"]);
    });

    it("uses startSize as tie-breaker when startTime is equal", async () => {
        const { book, redisDriver } = makeBook();
        const p1 = makePageInfo("p-size-90", 1000, 90, 5);
        const p2 = makePageInfo("p-size-10", 1000, 10, 500);
        const p3 = makePageInfo("p-size-50", 1000, 50, 1);
        redisDriver.usingRedisDriver.resolves(serializePages([p1, p2, p3]));

        const result = await book.listPages();

        assert.deepEqual(result.map((p) => p.pageKey), ["p-size-10", "p-size-50", "p-size-90"]);
    });

    it("uses startSerialNumber as tie-breaker when startTime and startSize are equal", async () => {
        const { book, redisDriver } = makeBook();
        const p1 = makePageInfo("p-serial-9", 1000, 10, 9);
        const p2 = makePageInfo("p-serial-1", 1000, 10, 1);
        const p3 = makePageInfo("p-serial-5", 1000, 10, 5);
        redisDriver.usingRedisDriver.resolves(serializePages([p1, p2, p3]));

        const result = await book.listPages();

        assert.deepEqual(result.map((p) => p.pageKey), ["p-serial-1", "p-serial-5", "p-serial-9"]);
    });

    it("applies all comparator levels in one mixed dataset", async () => {
        const { book, redisDriver } = makeBook();
        const pages = [
            makePageInfo("t2-s5-sn4", 2000, 5, 4),
            makePageInfo("t1-s9-sn9", 1000, 9, 9),
            makePageInfo("t1-s1-sn8", 1000, 1, 8),
            makePageInfo("t1-s1-sn2", 1000, 1, 2),
            makePageInfo("t2-s1-sn1", 2000, 1, 1)
        ];
        redisDriver.usingRedisDriver.resolves(serializePages(pages));

        const result = await book.listPages();

        assert.deepEqual(result.map((p) => p.pageKey), [
            "t1-s1-sn2",
            "t1-s1-sn8",
            "t1-s9-sn9",
            "t2-s1-sn1",
            "t2-s5-sn4"
        ]);
    });

    it("preserves duplicates and returns every parsed entry", async () => {
        const { book, redisDriver } = makeBook();
        const page = makePageInfo("dup", 1000, 1, 1);
        redisDriver.usingRedisDriver.resolves(serializePages([page, page, page]));

        const result = await book.listPages();

        assert.equal(result.length, 3);
        assert.deepEqual(result, [page, page, page]);
    });

    it("supports boundary-sized numeric values in page metadata", async () => {
        const { book, redisDriver } = makeBook();
        const maxU48 = 281_474_976_710_655;
        const p1 = makePageInfo("max", maxU48, maxU48, maxU48);
        const p2 = makePageInfo("zero", 0, 0, 0);
        redisDriver.usingRedisDriver.resolves(serializePages([p1, p2]));

        const result = await book.listPages();

        assert.deepEqual(result.map((p) => p.pageKey), ["zero", "max"]);
    });

    it("propagates Redis driver failures", async () => {
        const { book, redisDriver } = makeBook();
        redisDriver.usingRedisDriver.rejects(new Error("redis timeout"));

        await assert.rejects(
            book.listPages(),
            /redis timeout/i
        );
    });

    it("propagates JSON parse failures for malformed payloads", async () => {
        const { book, redisDriver } = makeBook();
        redisDriver.usingRedisDriver.resolves([
            JSON.stringify(makePageInfo("ok", 1000, 1, 1)),
            "{not-json}"
        ]);

        await assert.rejects(
            book.listPages(),
            /SyntaxError|JSON|Expected property name/i
        );
    });

    it("does not call pageFactory or reconcile callback during listing", async () => {
        const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(makePage());
        const pagesReconcileCallback = sinon.stub<[IPageInfo | undefined, IPageInfo[]], Promise<void>>().resolves();
        const { book, redisDriver } = makeBook({ pageFactory, pagesReconcileCallback });
        redisDriver.usingRedisDriver.resolves([]);

        await book.listPages();

        assert.equal(pageFactory.called, false);
        assert.equal(pagesReconcileCallback.called, false);
    });
});
