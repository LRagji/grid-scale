// import assert from "node:assert/strict";
// import { afterEach, describe, it } from "node:test";
// import sinon from "sinon";

// import { RedisCascadingBook } from "../src/index.js";
// import type { IDimensionalElement, IDimensionalQuery, IKeyBuilder, IPage, IPageInfo, IRDriver } from "../src/index.js";

// const VALID_CAPACITY = 10;
// const VALID_PAGE_SIZE_BYTES = 100;
// const VALID_ACTIVE_TIME_MS = 5_000;
// const VALID_PAGE_TYPE = "test-page";

// type DriverStub = IRDriver & {
//     usingRedisDriver: sinon.SinonStub;
//     harmonizedTimeInMs: sinon.SinonStub;
//     initialize: sinon.SinonStub;
// };

// type PageStub = IPage & {
//     upsertElements: sinon.SinonStub;
//     queryElementsByDimensions: sinon.SinonStub;
//     dumpPage: sinon.SinonStub;
//     fetchElementsByRange: sinon.SinonStub;
// };

// function makeDriver(): DriverStub {
//     return {
//         timeToleranceInMs: 100,
//         initialize: sinon.stub().resolves(),
//         usingRedisDriver: sinon.stub(),
//         harmonizedTimeInMs: sinon.stub().returns(10_000)
//     } as unknown as DriverStub;
// }

// function makeKeyBuilder(): IKeyBuilder {
//     return {
//         counterKey: sinon.stub().returns("counter-key"),
//         pageKey: sinon.stub().callsFake((t: string, s: string, w: string) => `page:${t}:${s}:${w}`),
//         bookKey: sinon.stub().returns("book-key"),
//         groupKey: sinon.stub().callsFake((pk: string, tn: string) => `${pk}:${tn}`),
//         groupListKey: sinon.stub().callsFake((pk: string) => `${pk}:groups`)
//     };
// }

// function makePage(elements: IDimensionalElement[]): PageStub {
//     return {
//         info: { pageKey: "unused", startTime: 0, startSize: 0, startSerialNumber: 0 },
//         pageType: VALID_PAGE_TYPE,
//         upsertElements: sinon.stub().resolves(),
//         queryElementsByDimensions: sinon.stub().resolves([]),
//         dumpPage: sinon.stub().resolves(elements),
//         fetchElementsByRange: sinon.stub().resolves([])
//     } as unknown as PageStub;
// }

// function makePageInfo(pageKey: string, startTime: number): IPageInfo {
//     return { pageKey, startTime, startSize: 0, startSerialNumber: 0 };
// }

// function makeBook(pageFactory: (pageInfo: IPageInfo, pageType: string) => Promise<PageStub>, hashFunction?: (element: IDimensionalElement) => string) {
//     const redisDriver = makeDriver();
//     const keyBuilder = makeKeyBuilder();

//     const book = hashFunction
//         ? new RedisCascadingBook<PageStub>(
//             VALID_CAPACITY,
//             VALID_PAGE_SIZE_BYTES,
//             VALID_ACTIVE_TIME_MS,
//             VALID_PAGE_TYPE,
//             pageFactory,
//             sinon.stub<[IPageInfo | undefined, IPageInfo[]], Promise<void>>().resolves(),
//             redisDriver,
//             () => 1,
//             keyBuilder,
//             hashFunction
//         )
//         : new RedisCascadingBook<PageStub>(
//             VALID_CAPACITY,
//             VALID_PAGE_SIZE_BYTES,
//             VALID_ACTIVE_TIME_MS,
//             VALID_PAGE_TYPE,
//             pageFactory,
//             sinon.stub<[IPageInfo | undefined, IPageInfo[]], Promise<void>>().resolves(),
//             redisDriver,
//             () => 1,
//             keyBuilder
//         );

//     return { book, redisDriver };
// }

// function stubListPages(redisDriver: DriverStub, pageInfos: IPageInfo[]): void {
//     redisDriver.usingRedisDriver.resolves(pageInfos.map(pi => JSON.stringify(pi)));
// }

// afterEach(() => {
//     sinon.restore();
// });

// describe("RedisCascadingBook.queryElementsByDimensions", () => {
//     it("filters deduplicated in-memory data by dimensional query", async () => {
//         const oldVersion = { dim: { device: "A", temp: 10 }, pld: { value: "old" } };
//         const newVersion = { dim: { device: "A", temp: 20 }, pld: { value: "new" } };
//         const other = { dim: { device: "B", temp: 15 }, pld: { value: "other" } };

//         const page1 = makePage([oldVersion, other]);
//         const page2 = makePage([newVersion]);

//         const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
//         pageFactory.onFirstCall().resolves(page1);
//         pageFactory.onSecondCall().resolves(page2);

//         const { book, redisDriver } = makeBook(pageFactory);
//         stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

//         const query: IDimensionalQuery = {
//             query: {
//                 operator: "AND",
//                 conditions: [
//                     { dimension: "device", operator: "eq", value: "A" },
//                     { dimension: "temp", operator: "gt", value: 15 }
//                 ]
//             }
//         };

//         const result = await book.queryElementsByDimensions(query, 10);

//         assert.deepEqual(result, [newVersion]);
//         assert.equal(page1.dumpPage.calledOnce, true);
//         assert.equal(page2.dumpPage.calledOnce, true);
//     });

//     it("respects maxElementsCount after filtering", async () => {
//         const elements = [
//             { dim: { country: "India", score: 90 }, pld: { id: "e1" } },
//             { dim: { country: "USA", score: 95 }, pld: { id: "e2" } },
//             { dim: { country: "India", score: 99 }, pld: { id: "e3" } }
//         ];

//         const page = makePage(elements);
//         const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(page);
//         const { book, redisDriver } = makeBook(pageFactory);
//         stubListPages(redisDriver, [makePageInfo("pk1", 1000)]);

//         const query: IDimensionalQuery = {
//             query: {
//                 operator: "OR",
//                 conditions: [
//                     { dimension: "country", operator: "eq", value: "India" },
//                     { dimension: "score", operator: "gt", value: 90 }
//                 ]
//             }
//         };

//         const result = await book.queryElementsByDimensions(query, 2);

//         assert.equal(result.length, 2);
//     });

//     it("throws when maxElementsCount is invalid", async () => {
//         const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>().resolves(makePage([]));
//         const { book, redisDriver } = makeBook(pageFactory);
//         stubListPages(redisDriver, []);

//         const query: IDimensionalQuery = {
//             query: {
//                 operator: "AND",
//                 conditions: [{ dimension: "x", operator: "eq", value: "y" }]
//             }
//         };

//         await assert.rejects(
//             book.queryElementsByDimensions(query, 0),
//             /Max elements count must be between 1 and 10000/i
//         );
//     });

//     it("uses latest page value when multiple elements share the same hash", async () => {
//         const first = { dim: { sensor: "A", value: 1 }, pld: { id: "old" } };
//         const second = { dim: { sensor: "A", value: 2 }, pld: { id: "new" } };

//         const page1 = makePage([first]);
//         const page2 = makePage([second]);
//         const pageFactory = sinon.stub<[IPageInfo, string], Promise<PageStub>>();
//         pageFactory.onFirstCall().resolves(page1);
//         pageFactory.onSecondCall().resolves(page2);

//         const { book, redisDriver } = makeBook(pageFactory, () => "same-hash");
//         stubListPages(redisDriver, [makePageInfo("pk1", 1000), makePageInfo("pk2", 2000)]);

//         const query: IDimensionalQuery = {
//             query: {
//                 operator: "AND",
//                 conditions: [{ dimension: "sensor", operator: "eq", value: "A" }]
//             }
//         };

//         const result = await book.queryElementsByDimensions(query, 10);
//         assert.deepEqual(result, [second]);
//     });
// });
