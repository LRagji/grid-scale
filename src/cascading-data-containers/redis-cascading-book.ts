import { IRDriver, RedisKeywords } from "../interfaces/i-r-driver.js";
import { IKeyBuilder, RKeyBuilder } from "../redis-wal/r-key-builder.js";
import { Utilities } from "../utilities.js";
import { IBook } from "./interfaces/i-book.js";
import { IDimensionalElement } from "./interfaces/i-dimensional-element.js";
import { IDimensionalQuery } from "./interfaces/i-dimensional-query.js";
import { IPage } from "./interfaces/i-page.js";
import { IPageInfo } from "./interfaces/i-page-info.js";

export class RedisCascadingBook<PT extends IPage> implements IBook<PT> {

    private readonly pageSerialNumberLimit = Utilities.u48In3; // This is the maximum number of elements we can have on a page before we risk overflow of the serial number counter, which can lead to empty gaps on page or worse overflows.
    private readonly counterBytes = 6;//Only u48 so its 6 bytes
    private timeCounterBuffer = Buffer.alloc(this.counterBytes);

    constructor(
        public readonly totalPageCapacity: number,
        public readonly pageSizeLimitInBytes: number,
        public readonly pageActiveTimeLimitInMs: number,
        public readonly pageType: string,
        public readonly pageFactory: (pageInfo: IPageInfo, pageType: string) => Promise<PT>,
        public readonly pagesReconcileCallback: (newPageInfo: IPageInfo | undefined, evictedPageInfo: IPageInfo[]) => Promise<void>,
        //Private members
        private readonly redisDriver: IRDriver,
        private readonly sizeEstimator: (element: IDimensionalElement[]) => number = (element: IDimensionalElement[]) => 1,
        private readonly keyBuilder: IKeyBuilder = new RKeyBuilder(),
        private readonly hashFunction: (element: IDimensionalElement) => string = Utilities.hashElement
    ) {
        if (this.totalPageCapacity <= 0 || this.totalPageCapacity > Utilities.u48In3) {
            throw new Error("Total page capacity must be between 1 and " + Utilities.u48In3 + ". Currently, it is set to " + this.totalPageCapacity + " pages.");
        }
        if (this.pageSizeLimitInBytes <= 0 || this.pageSizeLimitInBytes > Utilities.u48In3) {
            throw new Error("Page size limit in bytes must be between 1 and " + Utilities.u48In3 + ". Currently, it is set to " + this.pageSizeLimitInBytes.toString() + " bytes.");
        }
        if (this.pageActiveTimeLimitInMs <= 1000 || this.pageActiveTimeLimitInMs > Utilities.u48In3) {
            throw new Error("Page active time limit in ms must be between 1 second and " + Utilities.u48In3 + " ms. Currently, it is set to " + this.pageActiveTimeLimitInMs.toString() + " ms.");
        }
        if (!this.pageFactory) {
            throw new Error("Page factory function must be provided.");
        }
        if (!this.pagesReconcileCallback) {
            throw new Error("Pages reconcile callback function must be provided.");
        }
        if (this.redisDriver.timeToleranceInMs >= this.pageActiveTimeLimitInMs) {
            throw new Error("Time tolerance must be less than page active time limit to ensure proper functioning of the system. Currently, time tolerance is " + this.redisDriver.timeToleranceInMs.toString() + " ms and page active time limit is " + this.pageActiveTimeLimitInMs.toString() + " ms.");
        }
        if (!this.sizeEstimator) {
            throw new Error("Size estimator function must be provided.");
        }
    }

    public async listPages(): Promise<IPageInfo[]> {
        const bookKey = this.keyBuilder.bookKey();
        const pageKeys = await this.redisDriver.usingRedisDriver<string[]>([[RedisKeywords.ZRANGE, bookKey, "0", "-1"]], 'FetchAllPagesWithRanks', "run");
        const sortedPageInfo = pageKeys
            .map(serializedPageInfo => JSON.parse(serializedPageInfo) as IPageInfo)
            .sort((aObj: IPageInfo, bObj: IPageInfo) => {
                if (aObj.startTime !== bObj.startTime) {
                    return aObj.startTime - bObj.startTime;
                }
                if (aObj.startSize !== bObj.startSize) {
                    return aObj.startSize - bObj.startSize;
                }
                return aObj.startSerialNumber - bObj.startSerialNumber;
            });
        return sortedPageInfo;
    }

    public async fetchPageByKey(pageKey: IPageInfo): Promise<PT> {
        const page = await this.pageFactory(pageKey, this.pageType);
        return page;
    }

    public async removePage(pageKey: IPageInfo, invokeReconcileCallback = true): Promise<void> {
        const redisKey = this.keyBuilder.bookKey();
        const result = await this.redisDriver.usingRedisDriver<number>([[RedisKeywords.ZREM, redisKey, JSON.stringify(pageKey)]], 'RemovePageFromBook', 'run');
        if (invokeReconcileCallback === true && result === 1) {
            await this.pagesReconcileCallback(undefined, [pageKey]);
        }
    }

    public async upsertElements(elements: IDimensionalElement[]): Promise<void> {
        const numberOfElements = elements.length;
        if (numberOfElements <= 0 || numberOfElements > Utilities.u48In3) {
            throw new Error("Number of elements must be between 1 and " + Utilities.u48In3 + ". Currently, it is set to " + numberOfElements.toString() + ".");
        }

        const totalElementSizeInBytes = this.sizeEstimator(elements);
        if (totalElementSizeInBytes <= 0 || totalElementSizeInBytes > Utilities.u48In3) {
            throw new Error("Estimated size must be between 1 and " + Utilities.u48In3 + ". Currently, it is set to " + totalElementSizeInBytes.toString() + " bytes.");
        }

        const harmonizedInsertTimestamp = this.redisDriver.harmonizedTimeInMs(Date.now());
        const pageResults = await this.navigateWriteablePage(harmonizedInsertTimestamp, totalElementSizeInBytes, numberOfElements);
        await pageResults.page.upsertElements(elements, pageResults.sequenceStartNumber);
    }

    public async queryElementsByDimensions(query: IDimensionalQuery, maxElementsCount: number = 1000): Promise<IDimensionalElement[]> {
        throw new Error("Not implemented yet. This method will be implemented once the query parser is implemented to allow querying by dimensions. For now, you can use queryByRank as a shortcut for testing purposes, which allows querying by rank within groups defined by group keys.");
    }

    public async queryByRank(groupKeys: string[], startInclusiveRank: number, endExclusiveRank: number, maxElementsPerGroup: number = 1000): Promise<IDimensionalElement[]> {

        const deDuplicatedGroupKeys = this.validateQueryRangeParams(groupKeys, startInclusiveRank, endExclusiveRank, maxElementsPerGroup);
        const rankedPages = await this.listPages();
        const rankedPageResults = await this.parallelQueryPages(rankedPages, deDuplicatedGroupKeys, startInclusiveRank, endExclusiveRank, maxElementsPerGroup);
        const result: IDimensionalElement[] = this.aggregateRankedElements(rankedPageResults);

        return result;
    }

    //Private methods
    private async navigateWriteablePage(harmonizedInsertTimestampInMs: number, sizeInBytes: number, count: number): Promise<{ page: PT, sequenceStartNumber: number }> {

        const {
            pageKey,
            pageStartTime,
            pageStartSize,
            pageStartSerialNumber,
            sequenceStartNumber } = await this.incrementAndGenerateKey(harmonizedInsertTimestampInMs, sizeInBytes, count);
        const pageInfo: IPageInfo = { pageKey, startTime: pageStartTime, startSize: pageStartSize, startSerialNumber: pageStartSerialNumber };

        const bookkeepingResults = await this.upsertPageInfo(pageKey, harmonizedInsertTimestampInMs, pageStartTime, pageStartSize, pageStartSerialNumber);

        if (bookkeepingResults.newPage === true || bookkeepingResults.trimmedPages.length > 0) {
            await this.pagesReconcileCallback(bookkeepingResults.newPage ? pageInfo : undefined, bookkeepingResults.trimmedPages);
        }

        const page = await this.pageFactory(pageInfo, this.pageType);

        return { page, sequenceStartNumber };
    }

    private async incrementAndGenerateKey(insertTimeWithTolerance: number, sizeInBytes: number, count: number): Promise<{
        pageKey: string,
        pageStartTime: number,
        pageStartSize: number,
        pageStartSerialNumber: number,
        sequenceStartNumber: number
    }> {
        const counterKey = this.keyBuilder.counterKey();
        // Current js engine v8 only guarantees 53 bit precision for integers, so we use 48 bits for the time header.
        // We use the same 48 bits for counter sizes etc.
        // Redis is the ultimate decider of when page turns cause of time by using redis server time via key expiry.
        // Rest 2 counters are expected no to overflow within this time window and will be use to determine the page name.
        this.timeCounterBuffer.writeUintBE(insertTimeWithTolerance, 0, this.counterBytes);

        const commands = [
            [RedisKeywords.SET, counterKey, this.timeCounterBuffer as any, RedisKeywords.SET_ONLY_IF_NO_EXPIRY],
            [
                RedisKeywords.BITFIELD, counterKey, RedisKeywords.OVERFLOW, RedisKeywords.FAIL,
                RedisKeywords.GET, "u48", "#0",
                RedisKeywords.INCRBY, "u48", "#1", `${sizeInBytes}`,
                RedisKeywords.INCRBY, "u48", "#2", `${count}`
            ],
            [RedisKeywords.PEXPIRE, counterKey, `${this.pageActiveTimeLimitInMs}`, RedisKeywords.SET_ONLY_IF_NO_EXPIRY]//This is how redis mod-minus time for us, using its own clock
        ];

        const response = await this.redisDriver.usingRedisDriver<string[][]>(commands, 'IncrementCounter', 'pipeline');

        const receivedTime = parseInt(response[1][0].toString(), 10);
        const sizeCounter = parseInt(response[1][1].toString(), 10); // This counter is always increasing so needs to be modded to determine page key.
        const writeCounter = parseInt(response[1][2].toString(), 10); // This counter is always increasing so needs to be modded to determine page key.

        const pageStartTime = Utilities.modMinus(receivedTime, this.pageActiveTimeLimitInMs);
        const previousSize = sizeCounter - sizeInBytes;
        const pageStartSize = Utilities.modMinus(previousSize, this.pageSizeLimitInBytes);
        const previousSerialNumber = writeCounter - count;
        const pageStartSerialNumber = Utilities.modMinus(previousSerialNumber, this.pageSerialNumberLimit);
        const pageKey = this.keyBuilder.pageKey(pageStartTime.toString(), pageStartSize.toString(), pageStartSerialNumber.toString());

        return { pageKey, pageStartTime, pageStartSize, pageStartSerialNumber, sequenceStartNumber: previousSerialNumber };
    }

    private async upsertPageInfo(pageKey: string, insertTime: number, startTime: number, startSize: number, startSerialNumber: number): Promise<{ newPage: boolean, trimmedPages: IPageInfo[] }> {
        const redisKey = this.keyBuilder.bookKey();
        const upsertTrimCommands = [
            [RedisKeywords.ZADD, redisKey, insertTime.toString(), JSON.stringify({ pageKey, startSize, startSerialNumber, startTime } as IPageInfo)],
            [RedisKeywords.ZRANGE, redisKey, "0", `-${this.totalPageCapacity + 1}`],
            [RedisKeywords.ZREMRANGEBYRANK, redisKey, "0", `-${this.totalPageCapacity + 1}`]
        ];
        const response = await this.redisDriver.usingRedisDriver<void>(upsertTrimCommands, 'UpdateBookForNewPage', 'pipeline');
        const newPage = Array.isArray(response) && parseInt(response[0] ?? "0", 10) === 1; // This means a new page was added.
        const trimmedPages = (response[1] ?? []).map(serializedPageInfo => JSON.parse(serializedPageInfo) as IPageInfo)
        return { newPage, trimmedPages };
    }

    private async parallelQueryPages(rankedPages: IPageInfo[], deDuplicatedGroupKeys: string[], startInclusiveRank: number, endExclusiveRank: number, maxElementsPerGroup: number): Promise<Map<number, Map<string, IDimensionalElement[]>>> {
        const pageQueriesHandles = new Array<Promise<IDimensionalElement[]>>();
        for (const [pageRank, pageInfo] of rankedPages.entries()) {
            const page = await this.fetchPageByKey(pageInfo);
            if (page === null) {
                continue;
            }
            const promiseHandleForParallelQuery = page.fetchElementsByRange(deDuplicatedGroupKeys, startInclusiveRank, endExclusiveRank, maxElementsPerGroup);
            pageQueriesHandles.push(promiseHandleForParallelQuery);
        }
        const pageResults = await Promise.all(pageQueriesHandles);
        const returnObject = new Map<number, Map<string, IDimensionalElement[]>>(
            pageResults.map((result, index) => {
                const hashedElements = new Map<string, IDimensionalElement[]>();
                for (const element of result) {
                    const hash = this.hashFunction(element);
                    const clashingElement = hashedElements.get(hash) ?? [];
                    clashingElement.push(element);
                    hashedElements.set(hash, clashingElement);
                }
                return [index, hashedElements];
            })
        );
        return returnObject;
    }

    private aggregateRankedElements(pageResults: Map<number, Map<string, IDimensionalElement[]>>): IDimensionalElement[] {
        const result: IDimensionalElement[] = [];
        const valuesMapArray = Array.from(pageResults.values())
            .map(hashedElementsMap => Array.from(hashedElementsMap.keys()))
            .flat();
        const deDuplicatedHashKeys = new Set<string>(valuesMapArray);
        const sortedRanks = Array.from(pageResults.keys()).sort((a, b) => a - b);

        for (const hashKey of deDuplicatedHashKeys) {
            let tempHashElements = new Array<IDimensionalElement>();
            for (const pageRank of sortedRanks) {
                const elements = pageResults.get(pageRank)?.get(hashKey) ?? [];
                if (elements.length > 0) {
                    tempHashElements = elements;
                }
            }
            if (tempHashElements.length > 0) {
                result.push(...tempHashElements);
            }
        }

        return result;
    }

    private validateQueryRangeParams(groupKeys: string[], startInclusiveRank: number, endExclusiveRank: number, maxElementsPerGroup: number): string[] {

        if (groupKeys.length === 0) {
            throw new Error("At least one group key must be specified for querying.");
        }
        if (startInclusiveRank < 0 || endExclusiveRank < 0) {
            throw new Error("Start rank and end rank must be non-negative.");
        }
        if (endExclusiveRank < startInclusiveRank) {
            throw new Error("End rank must be greater than or equal to start rank.");
        }
        if ((endExclusiveRank - startInclusiveRank) === 0) {
            throw new Error(`The difference between end rank and start rank must be greater than 0. Currently, it is ${endExclusiveRank - startInclusiveRank}.`);
        }
        if (groupKeys.length > 10) {
            throw new Error("A maximum of 10 group keys can be specified for querying to prevent excessive load. Currently, " + groupKeys.length + " group keys were provided.");
        }
        if (maxElementsPerGroup <= 0 || maxElementsPerGroup > 10000) {
            throw new Error("Max elements must be between 1 and 10000. Currently, it is set to " + maxElementsPerGroup.toString() + ".");
        }

        return [...(new Set(groupKeys)).values()]
    }

}
