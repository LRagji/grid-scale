import { ChunkCache } from "./chunk-cache.js";
import { distributedQueryPlan, tagName, upsertPlan } from "./grid-scale.js";
import { kWayMerge } from "./merge/k-way-merge.js";
import { TConfig } from "./t-config.js";
import { ISyncPlugin } from "./threads/i-sync-plugin.js";
import { IThreadCommunication } from "./threads/i-thread-communication.js";

export type BackgroundPluginInitializeParam = { config: TConfig, cacheLimit: number, bulkDropLimit: number };

export class BackgroundPlugin implements ISyncPlugin<BackgroundPluginInitializeParam> {

    public static readonly invokeHeader: "BackgroundPlugin" = "BackgroundPlugin";
    public static readonly invokeSubHeaders = { "Store": "Store", "Iterate": "Iterate", "CloseQuery": "CloseQuery" };

    public readonly name = BackgroundPlugin.invokeHeader;

    private chunkCache: ChunkCache;
    private identity: string;
    private activeQueries = new Map<string, IterableIterator<unknown>>();//TODO: Need to understand how can query be cancelled.

    initialize(identity: string, param: BackgroundPluginInitializeParam): void {
        this.chunkCache = new ChunkCache(param.config, param.cacheLimit, param.bulkDropLimit);
        this.identity = identity;
    }

    work(input: IThreadCommunication<string | upsertPlan<Array<unknown>> | distributedQueryPlan>): IThreadCommunication<string | IteratorResult<Array<unknown>>> {
        switch (input.subHeader) {
            case BackgroundPlugin.invokeSubHeaders.Store:
                return this.store(input.payload as upsertPlan<Array<unknown>>);
                break;
            case BackgroundPlugin.invokeSubHeaders.CloseQuery:
                const queryId = input.payload as string;
                if (this.activeQueries.has(queryId)) {
                    const queryIterator = this.activeQueries.get(queryId);
                    queryIterator.return();
                    this.activeQueries.delete(queryId);
                }
                break;
            case BackgroundPlugin.invokeSubHeaders.Iterate:
                return this.read(input.payload as distributedQueryPlan);
                break;
            default:
                throw new Error(`Unknown subHeader: ${input.subHeader}`);
        }
    }

    private static frameMerge<T>(elements: T[]): { yieldIndex: number, purgeIndexes: number[] } {
        let purgeIndexes = [];
        let yieldIndex = -1;
        elements.forEach((element, index) => {
            if (element == null || Array.isArray(element) === false || (Array.isArray(element) && element.length === 0)) {
                purgeIndexes.push(index);
            }
            if (index === 0) {
                yieldIndex = index;
            }
            else {
                //TagName need to be compared
                if (element[4] === elements[yieldIndex][4] && element[0] < elements[yieldIndex][0]) {
                    yieldIndex = index;
                }
                else if (element[4] === elements[yieldIndex][4] && element[0] === elements[yieldIndex][0]) {
                    //Compare Insert time in descending order MVCC
                    if (elements[1] > elements[yieldIndex][1]) {
                        purgeIndexes.push(yieldIndex);
                        yieldIndex = index;
                    }
                    else {
                        purgeIndexes.push(index);
                    }
                }
            }

        });
        return { yieldIndex, purgeIndexes };
    };

    private read(distributedQueryPlan: distributedQueryPlan): IThreadCommunication<IteratorResult<Array<unknown>>> {
        let existingQuery = this.activeQueries.get(distributedQueryPlan.planId);
        if (existingQuery == null) {
            existingQuery = this.resultIterate(distributedQueryPlan);
            this.activeQueries.set(distributedQueryPlan.planId, existingQuery);
        }
        let iteratorResult = existingQuery.next();
        const resultPage: IteratorResult<Array<unknown>> = {
            value: new Array<unknown>(), done: false as boolean
        };
        while (iteratorResult.done === false && resultPage.value.length < (distributedQueryPlan.pageSize - 1)) {
            resultPage.value.push(iteratorResult.value);
            iteratorResult = existingQuery.next();
        }
        //Capture the last element if iterator is not done.
        if (iteratorResult.done === false && resultPage.value.length === (distributedQueryPlan.pageSize - 1)) {
            resultPage.value.push(iteratorResult.value);
        }
        if (iteratorResult.done && resultPage.value.length === 0) {
            this.activeQueries.delete(distributedQueryPlan.planId);
            resultPage.done = true;
        }
        return {
            header: "Response",
            subHeader: "Response",
            payload: resultPage
        } as IThreadCommunication<IteratorResult<Array<unknown>>>;
    }

    private *resultIterate<rowType>(distributedQueryPlan: distributedQueryPlan): IterableIterator<rowType> {
        for (const [logicalChunkId, whiteListedTags] of distributedQueryPlan.plan.groupedTags) {
            //Actual Disk Search & Open Operations
            const tagWiseDiskIterators = new Map<tagName, IterableIterator<rowType>[]>();
            for (const [chunkId, tagDiskInfo] of distributedQueryPlan.plan.plan) {
                for (const [tagName, diskInfo] of tagDiskInfo) {
                    if (whiteListedTags.has(tagName) === false) {
                        continue;
                    }
                    const chunk = this.chunkCache.getChunk(chunkId);//It is already cached.
                    const iterators = chunk.get<rowType>(diskInfo, [tagName], distributedQueryPlan.startInclusiveTime, distributedQueryPlan.endExclusiveTime);
                    if (iterators.length > 0) {
                        //Gather all the iterators for a tag from every chunk(For MVCC and H-Scale Merge)
                        const existingIterators = tagWiseDiskIterators.get(tagName) || [];
                        existingIterators.push(...iterators);
                        tagWiseDiskIterators.set(tagName, existingIterators);

                    }
                }
            }
            // Merge & Iterate Operation
            const results = new Array<unknown>();
            for (const [tagName, iterators] of tagWiseDiskIterators) {
                for (const dataRow of kWayMerge<rowType>(iterators, BackgroundPlugin.frameMerge<rowType>)) {
                    yield dataRow;
                }
            }
        }
    }

    private store(upsertPlan: upsertPlan<Array<unknown>>): IThreadCommunication<string> {
        for (const [logicalChunkId, diskIndexSamplesMap] of upsertPlan.chunkAllocations) {
            const chunk = this.chunkCache.getChunk(logicalChunkId);
            for (const [diskIndex, diskSampleSets] of diskIndexSamplesMap) {
                chunk.set(diskIndex, diskSampleSets, this.identity);
            }
        }
        return {
            header: "Response",
            subHeader: "Response",
            payload: "Success"
        };
    }

    public [Symbol.dispose]() {
        this.chunkCache[Symbol.dispose]();
    }
}