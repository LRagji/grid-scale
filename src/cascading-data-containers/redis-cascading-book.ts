import { IRDriver, RedisKeywords } from "../interfaces/i-r-driver.js";
import { RKeyBuilder } from "../utilities/r-key-builder.js";
import { ConvenienceMethods } from "../utilities/convenience-methods.js";
import { IBook } from "../interfaces/i-book.js";
import { IDimensionalElement } from "../interfaces/i-dimensional-element.js";
import { IDimensionalQuery } from "../interfaces/i-dimensional-query.js";
import { IPage } from "../interfaces/i-page.js";
import { IPageInfo } from "../interfaces/i-page-info.js";
import { IKeyBuilder } from "../interfaces/i-key-builder.js";

export class RedisCascadingBook implements IBook {

    private readonly pageSerialNumberLimit = ConvenienceMethods.u48In3; // This is the maximum number of elements we can have on a page before we risk overflow of the serial number counter, which can lead to empty gaps on page or worse overflows.
    private readonly counterBytes = 6;//Only u48 so its 6 bytes
    private timeCounterBuffer = Buffer.alloc(this.counterBytes);

    private static parseRedisInteger(value: unknown, label: string): number {
        const parsed = typeof value === "number" ? value : parseInt(String(value), 10);
        if (!Number.isFinite(parsed) || Number.isNaN(parsed)) {
            throw new Error(`Invalid redis integer response for ${label}: ${String(value)}`);
        }
        return parsed;
    }

    constructor(
        public readonly totalPageCapacity: number,
        public readonly pageSizeLimitInBytes: number,
        public readonly pageActiveTimeLimitInMs: number,
        public readonly pageType: string,
        public readonly pageFactory: (pageInfo: IPageInfo, pageType: string) => Promise<IPage>,
        public readonly pagesReconcileCallback: (newPageInfo: IPageInfo | undefined, evictedPageInfo: IPageInfo[]) => Promise<void>,
        //Private members
        private readonly redisDriver: IRDriver,
        private readonly sizeEstimator: (element: IDimensionalElement[]) => number = (element: IDimensionalElement[]) => 1,
        private readonly keyBuilder: IKeyBuilder = new RKeyBuilder()
    ) {
        if (this.totalPageCapacity <= 0 || this.totalPageCapacity > ConvenienceMethods.u48In3) {
            throw new Error("Total page capacity must be between 1 and " + ConvenienceMethods.u48In3 + ". Currently, it is set to " + this.totalPageCapacity + " pages.");
        }
        if (this.pageSizeLimitInBytes <= 0 || this.pageSizeLimitInBytes > ConvenienceMethods.u48In3) {
            throw new Error("Page size limit in bytes must be between 1 and " + ConvenienceMethods.u48In3 + ". Currently, it is set to " + this.pageSizeLimitInBytes.toString() + " bytes.");
        }
        if (this.pageActiveTimeLimitInMs <= 1000 || this.pageActiveTimeLimitInMs > ConvenienceMethods.u48In3) {
            throw new Error("Page active time limit in ms must be between 1 second and " + ConvenienceMethods.u48In3 + " ms. Currently, it is set to " + this.pageActiveTimeLimitInMs.toString() + " ms.");
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

    public async listPagesSorted(): Promise<IPageInfo[]> {
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

    public async fetchPageByKey(pageKey: IPageInfo): Promise<IPage> {
        const page = await this.pageFactory(pageKey, this.pageType);
        return page;
    }

    public async removePage(pageKey: IPageInfo, invokeReconcileCallback = true): Promise<void> {
        const redisKey = this.keyBuilder.bookKey();
        const result = await this.redisDriver.usingRedisDriver<number>([[RedisKeywords.ZREM, redisKey, JSON.stringify(pageKey)]], 'RemovePageFromBook', 'run');
        if (invokeReconcileCallback === true && RedisCascadingBook.parseRedisInteger(result, "RemovePageFromBook") === 1) {
            await this.pagesReconcileCallback(undefined, [pageKey]);
        }
    }

    public async upsertElements(elements: IDimensionalElement[]): Promise<void> {
        //Takes 6 redis commands to write a page
        const numberOfElements = elements.length;
        if (numberOfElements <= 0 || numberOfElements > ConvenienceMethods.u48In3) {
            throw new Error("Number of elements must be between 1 and " + ConvenienceMethods.u48In3 + ". Currently, it is set to " + numberOfElements.toString() + ".");
        }

        const totalElementSizeInBytes = this.sizeEstimator(elements);
        if (totalElementSizeInBytes <= 0 || totalElementSizeInBytes > ConvenienceMethods.u48In3) {
            throw new Error("Estimated size must be between 1 and " + ConvenienceMethods.u48In3 + ". Currently, it is set to " + totalElementSizeInBytes.toString() + " bytes.");
        }

        const harmonizedInsertTimestamp = this.redisDriver.harmonizedTimeInMs(Date.now());
        const pageResults = await this.navigateWriteablePage(harmonizedInsertTimestamp, totalElementSizeInBytes, numberOfElements);
        await pageResults.page.upsertElements(elements, pageResults.sequenceStartNumber);
    }

    public async queryElementsByDimensions(query: IDimensionalQuery, maxElementsCount: number = 1000): Promise<IDimensionalElement[]> {

        if (maxElementsCount <= 0 || maxElementsCount > 10000) {
            throw new Error("Max elements count must be between 1 and 10000. Currently, it is set to " + maxElementsCount.toString() + ".");
        }

        const rankedPages = await this.listPagesSorted();
        const rankedPageResults = await this.parallelQueryPages(rankedPages, async (page) => await page.queryElementsByDimensions(query, maxElementsCount) as IDimensionalElement[]);
        return rankedPageResults;
    }



    //Private methods
    private async navigateWriteablePage(harmonizedInsertTimestampInMs: number, sizeInBytes: number, count: number): Promise<{ page: IPage, sequenceStartNumber: number }> {

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

        if (!Array.isArray(response) || !Array.isArray(response[1]) || response[1].length < 3) {
            throw new Error("Invalid IncrementCounter response shape from redis pipeline.");
        }

        const receivedTime = RedisCascadingBook.parseRedisInteger(response[1][0], "IncrementCounter.time");
        const sizeCounter = RedisCascadingBook.parseRedisInteger(response[1][1], "IncrementCounter.size"); // This counter is always increasing so needs to be modded to determine page key.
        const writeCounter = RedisCascadingBook.parseRedisInteger(response[1][2], "IncrementCounter.write"); // This counter is always increasing so needs to be modded to determine page key.

        const pageStartTime = ConvenienceMethods.modMinus(receivedTime, this.pageActiveTimeLimitInMs);
        const previousSize = sizeCounter - sizeInBytes;
        const pageStartSize = ConvenienceMethods.modMinus(previousSize, this.pageSizeLimitInBytes);
        const previousSerialNumber = writeCounter - count;
        const pageStartSerialNumber = ConvenienceMethods.modMinus(previousSerialNumber, this.pageSerialNumberLimit);
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
        const response = await this.redisDriver.usingRedisDriver<(number | string | string[])[]>(upsertTrimCommands, 'UpdateBookForNewPage', 'pipeline');
        if (!Array.isArray(response)) {
            throw new Error("Invalid UpdateBookForNewPage response shape from redis pipeline.");
        }
        const newPage = RedisCascadingBook.parseRedisInteger(response[0] ?? 0, "UpdateBookForNewPage.newPage") === 1; // This means a new page was added.
        const trimmedPagePayload = Array.isArray(response[1]) ? response[1] : [];
        const trimmedPages = trimmedPagePayload.map(serializedPageInfo => JSON.parse(serializedPageInfo) as IPageInfo)
        return { newPage, trimmedPages };
    }

    private async parallelQueryPages(rankedPages: IPageInfo[], queryFunction: (page: IPage) => Promise<IDimensionalElement[]>): Promise<IDimensionalElement[]> {
        const pageQueriesHandles: Array<Promise<IDimensionalElement[]>> = rankedPages.map(async (pageInfo) => {
            const page = await this.fetchPageByKey(pageInfo);
            if (page === null) {
                return [];
            }
            return await queryFunction(page);
            //return await page.fetchElementsByRange(deDuplicatedGroupKeys, startInclusiveRank, endExclusiveRank, maxElementsPerGroup);
        });

        const pageResults = await Promise.all(pageQueriesHandles);
        const hashedElements = new Map<string | null, IDimensionalElement[]>();;

        for (const pageResult of pageResults) {//We are moving in ascending order so MVCC is automatically applied as we overwrite with newer versions of the same element as we move along the pages.
            if (pageResult.length === 0) {
                continue;
            }
            for (const element of pageResult) {
                //Null hash has a special meaning here, it means that the element does not have a globalIdentityHash and thus cannot be reliably deduplicated, so we will group all elements without globalIdentityHash under the same null hash key and rely on the query filters to filter them down.
                let clashingElement = hashedElements.get(element.globalIdentityHash) ?? [];
                if (element.globalIdentityHash == null) { // covers both null and undefined: cannot deduplicate, accumulate all
                    clashingElement.push(element);
                }
                else {
                    clashingElement = [element];
                }
                hashedElements.set(element.globalIdentityHash, clashingElement);
            }
        }

        const deDuplicatedElements = Array.from(hashedElements.values()).flat();;

        return deDuplicatedElements;
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
