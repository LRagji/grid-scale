import assert from "node:assert/strict";
import { describe, it } from "node:test";
import sinon from "sinon";

import { RedisCascadingBook } from "../src/index.js";
import type { IRDriver, IPage, IPageInfo } from "../src/index.js";
import { Utilities } from "../src/utilities.js";

const VALID_CAPACITY = 10;
const VALID_PAGE_SIZE_BYTES = 1024;
const VALID_ACTIVE_TIME_MS = 5000;
const VALID_PAGE_TYPE = "test-page";

function makeDriver(timeToleranceInMs = 100): IRDriver {
    return {
        timeToleranceInMs,
        initialize: sinon.stub<[], Promise<void>>().resolves(),
        usingRedisDriver: sinon.stub().resolves([]),
        harmonizedTimeInMs: sinon.stub().returnsArg(0),
    } as unknown as IRDriver;
}

function makePageFactory(): (pageInfo: IPageInfo, pageType: string) => Promise<IPage> {
    return sinon.stub<[IPageInfo, string], Promise<IPage>>().resolves({} as IPage);
}

function makeReconcileCallback(): (newPageInfo: IPageInfo | undefined, evictedPageInfo: IPageInfo[]) => Promise<void> {
    return sinon.stub<[IPageInfo | undefined, IPageInfo[]], Promise<void>>().resolves();
}

function validBook(overrides: Partial<{
    totalPageCapacity: number;
    pageSizeLimitInBytes: number;
    pageActiveTimeLimitInMs: number;
    pageType: string;
    pageFactory: (pageInfo: IPageInfo, pageType: string) => Promise<IPage>;
    pagesReconcileCallback: (newPageInfo: IPageInfo | undefined, evictedPageInfo: IPageInfo[]) => Promise<void>;
    redisDriver: IRDriver;
    sizeEstimator: (elements: any[]) => number;
}> = {}): RedisCascadingBook {
    return new RedisCascadingBook(
        overrides.totalPageCapacity ?? VALID_CAPACITY,
        overrides.pageSizeLimitInBytes ?? VALID_PAGE_SIZE_BYTES,
        overrides.pageActiveTimeLimitInMs ?? VALID_ACTIVE_TIME_MS,
        overrides.pageType ?? VALID_PAGE_TYPE,
        overrides.pageFactory ?? makePageFactory(),
        overrides.pagesReconcileCallback ?? makeReconcileCallback(),
        overrides.redisDriver ?? makeDriver(),
        overrides.sizeEstimator
    );
}

describe("RedisCascadingBook constructor", () => {

    describe("totalPageCapacity validation", () => {

        it("throws when totalPageCapacity is 0", () => {
            assert.throws(
                () => validBook({ totalPageCapacity: 0 }),
                /Total page capacity must be between 1 and/i
            );
        });

        it("throws when totalPageCapacity is negative", () => {
            assert.throws(
                () => validBook({ totalPageCapacity: -1 }),
                /Total page capacity must be between 1 and/i
            );
        });

        it("throws when totalPageCapacity exceeds u48In3", () => {
            assert.throws(
                () => validBook({ totalPageCapacity: Utilities.u48In3 + 1 }),
                /Total page capacity must be between 1 and/i
            );
        });

        it("accepts totalPageCapacity of 1", () => {
            assert.doesNotThrow(() => validBook({ totalPageCapacity: 1 }));
        });

        it("accepts totalPageCapacity equal to u48In3", () => {
            assert.doesNotThrow(() => validBook({ totalPageCapacity: Utilities.u48In3 }));
        });

    });

    describe("pageSizeLimitInBytes validation", () => {

        it("throws when pageSizeLimitInBytes is 0", () => {
            assert.throws(
                () => validBook({ pageSizeLimitInBytes: 0 }),
                /Page size limit in bytes must be between 1 and/i
            );
        });

        it("throws when pageSizeLimitInBytes is negative", () => {
            assert.throws(
                () => validBook({ pageSizeLimitInBytes: -1 }),
                /Page size limit in bytes must be between 1 and/i
            );
        });

        it("throws when pageSizeLimitInBytes exceeds u48In3", () => {
            assert.throws(
                () => validBook({ pageSizeLimitInBytes: Utilities.u48In3 + 1 }),
                /Page size limit in bytes must be between 1 and/i
            );
        });

        it("accepts pageSizeLimitInBytes of 1", () => {
            assert.doesNotThrow(() => validBook({ pageSizeLimitInBytes: 1 }));
        });

        it("accepts pageSizeLimitInBytes equal to u48In3", () => {
            assert.doesNotThrow(() => validBook({ pageSizeLimitInBytes: Utilities.u48In3 }));
        });

    });

    describe("pageActiveTimeLimitInMs validation", () => {

        it("throws when pageActiveTimeLimitInMs is exactly 1000", () => {
            assert.throws(
                () => validBook({ pageActiveTimeLimitInMs: 1000 }),
                /Page active time limit in ms must be between 1 second and/i
            );
        });

        it("throws when pageActiveTimeLimitInMs is less than 1000", () => {
            assert.throws(
                () => validBook({ pageActiveTimeLimitInMs: 999 }),
                /Page active time limit in ms must be between 1 second and/i
            );
        });

        it("throws when pageActiveTimeLimitInMs exceeds u48In3", () => {
            assert.throws(
                () => validBook({ pageActiveTimeLimitInMs: Utilities.u48In3 + 1 }),
                /Page active time limit in ms must be between 1 second and/i
            );
        });

        it("accepts pageActiveTimeLimitInMs of 1001", () => {
            assert.doesNotThrow(() => validBook({ pageActiveTimeLimitInMs: 1001 }));
        });

        it("accepts pageActiveTimeLimitInMs equal to u48In3", () => {
            assert.doesNotThrow(() => validBook({ pageActiveTimeLimitInMs: Utilities.u48In3 }));
        });

    });

    describe("pageFactory validation", () => {

        it("throws when pageFactory is null", () => {
            assert.throws(
                () => new RedisCascadingBook(VALID_CAPACITY, VALID_PAGE_SIZE_BYTES, VALID_ACTIVE_TIME_MS, VALID_PAGE_TYPE, null as any, makeReconcileCallback(), makeDriver()),
                /Page factory function must be provided/i
            );
        });

        it("throws when pageFactory is undefined", () => {
            assert.throws(
                () => new RedisCascadingBook(VALID_CAPACITY, VALID_PAGE_SIZE_BYTES, VALID_ACTIVE_TIME_MS, VALID_PAGE_TYPE, undefined as any, makeReconcileCallback(), makeDriver()),
                /Page factory function must be provided/i
            );
        });

        it("accepts a valid pageFactory function", () => {
            assert.doesNotThrow(() => validBook({ pageFactory: makePageFactory() }));
        });

    });

    describe("pagesReconcileCallback validation", () => {

        it("throws when pagesReconcileCallback is null", () => {
            assert.throws(
                () => new RedisCascadingBook(VALID_CAPACITY, VALID_PAGE_SIZE_BYTES, VALID_ACTIVE_TIME_MS, VALID_PAGE_TYPE, makePageFactory(), null as any, makeDriver()),
                /Pages reconcile callback function must be provided/i
            );
        });

        it("throws when pagesReconcileCallback is undefined", () => {
            assert.throws(
                () => new RedisCascadingBook(VALID_CAPACITY, VALID_PAGE_SIZE_BYTES, VALID_ACTIVE_TIME_MS, VALID_PAGE_TYPE, makePageFactory(), undefined as any, makeDriver()),
                /Pages reconcile callback function must be provided/i
            );
        });

        it("accepts a valid pagesReconcileCallback function", () => {
            assert.doesNotThrow(() => validBook({ pagesReconcileCallback: makeReconcileCallback() }));
        });

    });

    describe("redisDriver.timeToleranceInMs vs pageActiveTimeLimitInMs validation", () => {

        it("throws when timeToleranceInMs equals pageActiveTimeLimitInMs", () => {
            assert.throws(
                () => validBook({ redisDriver: makeDriver(VALID_ACTIVE_TIME_MS), pageActiveTimeLimitInMs: VALID_ACTIVE_TIME_MS }),
                /Time tolerance must be less than page active time limit/i
            );
        });

        it("throws when timeToleranceInMs exceeds pageActiveTimeLimitInMs", () => {
            assert.throws(
                () => validBook({ redisDriver: makeDriver(VALID_ACTIVE_TIME_MS + 1), pageActiveTimeLimitInMs: VALID_ACTIVE_TIME_MS }),
                /Time tolerance must be less than page active time limit/i
            );
        });

        it("accepts when timeToleranceInMs is one less than pageActiveTimeLimitInMs", () => {
            assert.doesNotThrow(() =>
                validBook({ redisDriver: makeDriver(VALID_ACTIVE_TIME_MS - 1), pageActiveTimeLimitInMs: VALID_ACTIVE_TIME_MS })
            );
        });

        it("accepts when timeToleranceInMs is zero", () => {
            assert.doesNotThrow(() =>
                validBook({ redisDriver: makeDriver(0), pageActiveTimeLimitInMs: VALID_ACTIVE_TIME_MS })
            );
        });

    });

    describe("sizeEstimator validation", () => {

        it("throws when sizeEstimator is explicitly null", () => {
            assert.throws(
                () => validBook({ sizeEstimator: null as any }),
                /Size estimator function must be provided/i
            );
        });

        it("uses default sizeEstimator when not provided", () => {
            assert.doesNotThrow(() => validBook({ sizeEstimator: undefined }));
        });

        it("accepts a custom sizeEstimator function", () => {
            assert.doesNotThrow(() => validBook({ sizeEstimator: () => 42 }));
        });

    });

    describe("successful construction", () => {

        it("constructs with all required parameters and valid values", () => {
            assert.doesNotThrow(() => validBook());
        });

        it("exposes the totalPageCapacity as provided", () => {
            const book = validBook({ totalPageCapacity: 5 });
            assert.equal(book.totalPageCapacity, 5);
        });

        it("exposes the pageSizeLimitInBytes as provided", () => {
            const book = validBook({ pageSizeLimitInBytes: 2048 });
            assert.equal(book.pageSizeLimitInBytes, 2048);
        });

        it("exposes the pageActiveTimeLimitInMs as provided", () => {
            const book = validBook({ pageActiveTimeLimitInMs: 3000 });
            assert.equal(book.pageActiveTimeLimitInMs, 3000);
        });

        it("exposes the pageType as provided", () => {
            const book = validBook({ pageType: "my-type" });
            assert.equal(book.pageType, "my-type");
        });

        it("exposes the pageFactory as provided", () => {
            const factory = makePageFactory();
            const book = validBook({ pageFactory: factory });
            assert.equal(book.pageFactory, factory);
        });

        it("exposes the pagesReconcileCallback as provided", () => {
            const callback = makeReconcileCallback();
            const book = validBook({ pagesReconcileCallback: callback });
            assert.equal(book.pagesReconcileCallback, callback);
        });

    });

});
