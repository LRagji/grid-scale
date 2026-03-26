import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import sinon from "sinon";

import { RedisCascadingBook } from "../src/index.js";
import type { IPage, IPageInfo, IRDriver, IKeyBuilder } from "../src/index.js";

// ── constants ─────────────────────────────────────────────────────────────────
const VALID_CAPACITY = 10;
const VALID_PAGE_SIZE_BYTES = 100;
const VALID_ACTIVE_TIME_MS = 5_000;
const VALID_PAGE_TYPE = "test-page";

// ── type aliases ──────────────────────────────────────────────────────────────
type DriverStub = IRDriver & {
    usingRedisDriver: sinon.SinonStub;
    harmonizedTimeInMs: sinon.SinonStub;
    initialize: sinon.SinonStub;
};

type PageStub = IPage & {
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
        groupKey: sinon.stub().callsFake((pk: string, tn: string) => `${pk}:${tn}`),
        groupListKey: sinon.stub().callsFake((pk: string) => `${pk}:groups`)
    };
}

function makePageInfo(pageKey: string = "pk1", startTime: number = 1000): IPageInfo {
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

    const book = new RedisCascadingBook<PageStub>(
        VALID_CAPACITY, VALID_PAGE_SIZE_BYTES, VALID_ACTIVE_TIME_MS, VALID_PAGE_TYPE,
        pageFactory, pagesReconcileCallback, redisDriver, () => 1, keyBuilder);

    return { book, redisDriver, pagesReconcileCallback: pagesReconcileCallback as sinon.SinonStub };
}

// ── tests ─────────────────────────────────────────────────────────────────────
afterEach(() => {
    sinon.restore();
});

describe("RedisCascadingBook.removePage", () => {

    // ── SUCCESSFUL REMOVAL WITH RECONCILE ─────────────────────────────────────

    it("removes a page from the book and invokes reconcile when page exists", async () => {
        const { book, redisDriver, pagesReconcileCallback } = makeBook();
        const pageToRemove = makePageInfo("pk-old", 5000);

        redisDriver.usingRedisDriver.resolves(1); // ZREM returns 1 → page was removed

        await book.removePage(pageToRemove);

        assert.equal(
            redisDriver.usingRedisDriver.calledOnceWithExactly(
                [["zrem", "book-key", JSON.stringify(pageToRemove)]],
                "RemovePageFromBook",
                "run"
            ),
            true
        );
        assert.equal(
            (pagesReconcileCallback as sinon.SinonStub).calledOnceWithExactly(undefined, [pageToRemove]),
            true
        );
    });

    it("serializes the pageKey correctly in the ZREM command", async () => {
        const { book, redisDriver } = makeBook();
        const pageToRemove: IPageInfo = {
            pageKey: "complex:page:key",
            startTime: 123_456,
            startSize: 789,
            startSerialNumber: 42
        };

        redisDriver.usingRedisDriver.resolves(1);

        await book.removePage(pageToRemove);

        const expectedSerialized = JSON.stringify(pageToRemove);
        const actualArgs = redisDriver.usingRedisDriver.firstCall.args[0];
        assert.equal(actualArgs[0][2], expectedSerialized);
    });

    it("invokes reconcile with undefined newPageInfo and array containing the removed page", async () => {
        const { book, redisDriver, pagesReconcileCallback } = makeBook();
        const pageToRemove = makePageInfo("pk-remove-me", 8000);

        redisDriver.usingRedisDriver.resolves(1);

        await book.removePage(pageToRemove);

        const reconcileCall = (pagesReconcileCallback as sinon.SinonStub).firstCall;
        assert.equal(reconcileCall.args[0], undefined); // newPageInfo is undefined
        assert.deepEqual(reconcileCall.args[1], [pageToRemove]); // evictedPageInfo contains the removed page
    });

    // ── PAGE NOT FOUND (RESULT = 0) ───────────────────────────────────────────

    it("does not invoke reconcile when the page does not exist (ZREM returns 0)", async () => {
        const { book, redisDriver, pagesReconcileCallback } = makeBook();
        const pageToRemove = makePageInfo("nonexistent-page", 6000);

        redisDriver.usingRedisDriver.resolves(0); // ZREM returns 0 → page not found

        await book.removePage(pageToRemove);

        assert.equal((pagesReconcileCallback as sinon.SinonStub).called, false);
    });

    it("still issues the ZREM command even when the page does not exist", async () => {
        const { book, redisDriver } = makeBook();
        const pageToRemove = makePageInfo("missing", 7000);

        redisDriver.usingRedisDriver.resolves(0);

        await book.removePage(pageToRemove);

        assert.equal(redisDriver.usingRedisDriver.calledOnce, true);
        assert.equal(redisDriver.usingRedisDriver.firstCall.args[1], "RemovePageFromBook");
    });

    // ── INVOKE RECONCILE CALLBACK FLAG ────────────────────────────────────────

    it("invokes reconcile when invokeReconcileCallback is explicitly true and page exists", async () => {
        const { book, redisDriver, pagesReconcileCallback } = makeBook();
        const pageToRemove = makePageInfo("pk1", 5000);

        redisDriver.usingRedisDriver.resolves(1);

        await book.removePage(pageToRemove, true); // explicitly true

        assert.equal((pagesReconcileCallback as sinon.SinonStub).called, true);
    });

    it("does not invoke reconcile when invokeReconcileCallback is false, even if page exists", async () => {
        const { book, redisDriver, pagesReconcileCallback } = makeBook();
        const pageToRemove = makePageInfo("pk1", 5000);

        redisDriver.usingRedisDriver.resolves(1); // page would exist

        await book.removePage(pageToRemove, false); // invokeReconcileCallback = false

        assert.equal((pagesReconcileCallback as sinon.SinonStub).called, false);
    });

    it("uses the default invokeReconcileCallback of true when not provided", async () => {
        const { book, redisDriver, pagesReconcileCallback } = makeBook();
        const pageToRemove = makePageInfo("pk1", 5000);

        redisDriver.usingRedisDriver.resolves(1);

        await book.removePage(pageToRemove); // no second argument

        assert.equal((pagesReconcileCallback as sinon.SinonStub).called, true);
    });

    it("does not invoke reconcile when invokeReconcileCallback is false and page does not exist", async () => {
        const { book, redisDriver, pagesReconcileCallback } = makeBook();
        const pageToRemove = makePageInfo("missing", 7000);

        redisDriver.usingRedisDriver.resolves(0); // page not found

        await book.removePage(pageToRemove, false);

        assert.equal((pagesReconcileCallback as sinon.SinonStub).called, false);
    });

    // ── EDGE CASES: BOTH CONDITIONS MUST BE TRUE ──────────────────────────────

    it("requires both conditions (invokeReconcileCallback=true AND result=1) for reconcile to fire", async () => {
        const pageToRemove = makePageInfo("pk-test", 5000);

        // Test 1: invokeReconcileCallback=true, result=1 → reconcile FIRES
        const { book: book1, redisDriver: driver1, pagesReconcileCallback: cb1 } = makeBook();
        driver1.usingRedisDriver.resolves(1);
        await book1.removePage(pageToRemove, true);
        assert.equal((cb1 as sinon.SinonStub).called, true);

        // Test 2: invokeReconcileCallback=true, result=0 → reconcile DOES NOT FIRE
        const { book: book2, redisDriver: driver2, pagesReconcileCallback: cb2 } = makeBook();
        driver2.usingRedisDriver.resolves(0);
        await book2.removePage(pageToRemove, true);
        assert.equal((cb2 as sinon.SinonStub).called, false);

        // Test 3: invokeReconcileCallback=false, result=1 → reconcile DOES NOT FIRE
        const { book: book3, redisDriver: driver3, pagesReconcileCallback: cb3 } = makeBook();
        driver3.usingRedisDriver.resolves(1);
        await book3.removePage(pageToRemove, false);
        assert.equal((cb3 as sinon.SinonStub).called, false);

        // Test 4: invokeReconcileCallback=false, result=0 → reconcile DOES NOT FIRE
        const { book: book4, redisDriver: driver4, pagesReconcileCallback: cb4 } = makeBook();
        driver4.usingRedisDriver.resolves(0);
        await book4.removePage(pageToRemove, false);
        assert.equal((cb4 as sinon.SinonStub).called, false);
    });

    // ── FAILURE PROPAGATION ───────────────────────────────────────────────────

    it("propagates ZREM failures when the Redis command fails", async () => {
        const { book, redisDriver } = makeBook();
        const pageToRemove = makePageInfo("pk1", 5000);

        redisDriver.usingRedisDriver.rejects(new Error("redis connection lost"));

        await assert.rejects(
            book.removePage(pageToRemove),
            /redis connection lost/i
        );
    });

    it("propagates reconcile callback failures when reconcile is called but fails", async () => {
        const { book, redisDriver, pagesReconcileCallback } = makeBook();
        const pageToRemove = makePageInfo("pk1", 5000);

        redisDriver.usingRedisDriver.resolves(1); // page removal succeeds
        (pagesReconcileCallback as sinon.SinonStub).rejects(new Error("reconcile failed"));

        await assert.rejects(
            book.removePage(pageToRemove, true),
            /reconcile failed/i
        );
    });

    it("does not call reconcile if ZREM fails, so reconcile errors do not occur", async () => {
        const { book, redisDriver, pagesReconcileCallback } = makeBook();
        const pageToRemove = makePageInfo("pk1", 5000);

        redisDriver.usingRedisDriver.rejects(new Error("zrem failed"));

        await assert.rejects(
            book.removePage(pageToRemove),
            /zrem failed/i
        );

        assert.equal((pagesReconcileCallback as sinon.SinonStub).called, false);
    });

    // ── MULTIPLE REMOVALS ─────────────────────────────────────────────────────

    it("allows multiple pages to be removed sequentially", async () => {
        const { book, redisDriver, pagesReconcileCallback } = makeBook();
        const page1 = makePageInfo("pk1", 1000);
        const page2 = makePageInfo("pk2", 2000);
        const page3 = makePageInfo("pk3", 3000);

        redisDriver.usingRedisDriver.resolves(1);

        await book.removePage(page1);
        await book.removePage(page2);
        await book.removePage(page3);

        assert.equal(redisDriver.usingRedisDriver.callCount, 3);
        assert.equal((pagesReconcileCallback as sinon.SinonStub).callCount, 3);
        assert.deepEqual((pagesReconcileCallback as sinon.SinonStub).secondCall.args[1], [page2]);
        assert.deepEqual((pagesReconcileCallback as sinon.SinonStub).thirdCall.args[1], [page3]);
    });

    it("handles mixed success and not-found removals in sequence", async () => {
        const { book, redisDriver, pagesReconcileCallback } = makeBook();
        const page1 = makePageInfo("exists", 1000);
        const page2 = makePageInfo("missing", 2000);
        const page3 = makePageInfo("also-exists", 3000);

        redisDriver.usingRedisDriver.onCall(0).resolves(1); // page1 found
        redisDriver.usingRedisDriver.onCall(1).resolves(0); // page2 not found
        redisDriver.usingRedisDriver.onCall(2).resolves(1); // page3 found

        await book.removePage(page1);
        await book.removePage(page2);
        await book.removePage(page3);

        assert.equal(redisDriver.usingRedisDriver.callCount, 3);
        assert.equal((pagesReconcileCallback as sinon.SinonStub).callCount, 2); // only 2 because page2 not found
        assert.deepEqual((pagesReconcileCallback as sinon.SinonStub).firstCall.args[1], [page1]);
        assert.deepEqual((pagesReconcileCallback as sinon.SinonStub).secondCall.args[1], [page3]);
    });

    // ── KEYBUILDER INTEGRATION ────────────────────────────────────────────────

    it("uses the keyBuilder.bookKey() to obtain the Redis key for ZREM", async () => {
        const keyBuilder = makeKeyBuilder();
        const { book, redisDriver } = makeBook({ keyBuilder });

        redisDriver.usingRedisDriver.resolves(1);

        await book.removePage(makePageInfo("pk1", 5000));

        assert.equal((keyBuilder.bookKey as sinon.SinonStub).calledOnce, true);
        const zremCommand = redisDriver.usingRedisDriver.firstCall.args[0][0];
        assert.equal(zremCommand[1], "book-key"); // second element should be the book key
    });

    // ── EMPTY AND SPECIAL PAGEINFO ────────────────────────────────────────────

    it("handles removal of a page with special characters in the pageKey string", async () => {
        const { book, redisDriver } = makeBook();
        const specialPage: IPageInfo = {
            pageKey: 'page:"with:special:chars"',
            startTime: 9999,
            startSize: 123,
            startSerialNumber: 456
        };

        redisDriver.usingRedisDriver.resolves(1);

        await book.removePage(specialPage);

        const serialized = redisDriver.usingRedisDriver.firstCall.args[0][0][2];
        assert.equal(serialized, JSON.stringify(specialPage));
        assert.deepEqual(JSON.parse(serialized), specialPage);
    });

    it("handles removal of a page with zero values for startTime, startSize, startSerialNumber", async () => {
        const { book, redisDriver, pagesReconcileCallback } = makeBook();
        const zeroPage: IPageInfo = {
            pageKey: "page:0:0:0",
            startTime: 0,
            startSize: 0,
            startSerialNumber: 0
        };

        redisDriver.usingRedisDriver.resolves(1);

        await book.removePage(zeroPage);

        assert.equal(redisDriver.usingRedisDriver.calledOnce, true);
        assert.equal((pagesReconcileCallback as sinon.SinonStub).calledOnceWithExactly(undefined, [zeroPage]), true);
    });

    it("handles removal of a page with maximum u48 values", async () => {
        const { book, redisDriver, pagesReconcileCallback } = makeBook();
        const maxPage: IPageInfo = {
            pageKey: "page:max",
            startTime: 281_474_976_710_655, // 2^48 - 1
            startSize: 281_474_976_710_655,
            startSerialNumber: 281_474_976_710_655
        };

        redisDriver.usingRedisDriver.resolves(1);

        await book.removePage(maxPage);

        assert.equal((pagesReconcileCallback as sinon.SinonStub).calledOnceWithExactly(undefined, [maxPage]), true);
    });

    // ── CONCURRENT BEHAVIOR ───────────────────────────────────────────────────

    it("awaits the ZREM result before checking reconcile condition", async () => {
        const { book, redisDriver, pagesReconcileCallback } = makeBook();
        const page = makePageInfo("pk1", 5000);
        let zremResolvedBeforeReconcile = false;

        redisDriver.usingRedisDriver.returns(new Promise(resolve => {
            setTimeout(() => {
                zremResolvedBeforeReconcile = true;
                resolve(1);
            }, 10);
        }));

        (pagesReconcileCallback as sinon.SinonStub).callsFake(async () => {
            // When reconcile is called, ZREM should have already resolved
            assert.equal(zremResolvedBeforeReconcile, true);
        });

        await book.removePage(page);
    });

    it("awaits the reconcile callback to complete before returning", async () => {
        const { book, redisDriver, pagesReconcileCallback } = makeBook();
        const page = makePageInfo("pk1", 5000);
        let reconcileResolved = false;

        redisDriver.usingRedisDriver.resolves(1);
        (pagesReconcileCallback as sinon.SinonStub).callsFake(
            () => new Promise<void>(resolve => {
                setTimeout(() => {
                    reconcileResolved = true;
                    resolve();
                }, 10);
            })
        );

        await book.removePage(page);

        assert.equal(reconcileResolved, true);
    });
});
