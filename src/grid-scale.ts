import { ChunkPlanner } from "./chunk-planner.js";
import { StatefulProxyManager } from "node-apparatus";
import { INonVolatileHashMap } from "./non-volatile-hash-map/i-non-volatile-hash-map.js";
import { IChunk } from "./chunk/i-chunk.js";
import { ChunkFactoryBase } from "./chunk/chunk-factory-base.js";
import { IChunkMetadata } from "./chunk-metadata/i-chunk-metadata.js";
import crypto from "node:crypto";
import { metaKeyBirth, metaKeyLastWrite } from "./types/meta-keys.js";
import { cachedPageCountKey, cachedRedirectKey, cachedTimeKey, cachePageKey } from "./types/cache-keys.js";

export class GridScale {

    private chunkPluginFactoryType: typeof ChunkFactoryBase<IChunk>;

    constructor(
        private readonly chunkRegistry: INonVolatileHashMap,
        private readonly chunkPlanner: ChunkPlanner,
        private readonly remoteProxies: StatefulProxyManager,
        private readonly chunkPluginFactoryPath: URL,
        private readonly chunkMetaRegistry: IChunkMetadata,
        private readonly chunkCache: INonVolatileHashMap
    ) { }

    public async initialize(): Promise<void> {
        this.chunkPluginFactoryType = (await import(this.chunkPluginFactoryPath.toString())).default.constructor;
    }

    public async store(records: Map<bigint, any[]>, insertTime = Date.now(), diagnostics = new Map<string, any>()): Promise<void> {
        let timings = Date.now();
        const recordLength: number = this.chunkPluginFactoryType.columnCount - 1;//-1 cause records are without the tag column
        const recordTimestampIndex: number = this.chunkPluginFactoryType.timeColumnIndex;
        const recordInsertTimeIndex: number = this.chunkPluginFactoryType.insertTimeColumnIndex;
        const upsertPlan = this.chunkPlanner.planUpserts(records, recordLength, recordTimestampIndex, recordInsertTimeIndex, insertTime, this.remoteProxies.WorkerCount);
        //Diagnostics
        const diagnosticsWorkerPlan = new Array<string>();
        for (const [workerIdx, plans] of upsertPlan.chunkAllocations.entries()) {
            if (plans === undefined) { continue; }
            for (const [planIdx, plan] of plans.entries()) {
                const recs = plan[1];
                let samples = 0;
                for (const [tagName, rows] of recs) {
                    samples += rows.length;
                }
                diagnosticsWorkerPlan.push(`worker:${workerIdx} plan:${planIdx} shards:1 tags:${recs.size} records:${samples}`);
            }
        }
        diagnostics.set("workersPlan", diagnosticsWorkerPlan);
        diagnostics.set("planTime", Date.now() - timings);

        //Write data
        timings = Date.now();
        let promiseHandles = new Array<Promise<void>>();
        for (let workerIdx = 0; workerIdx < upsertPlan.chunkAllocations.length; workerIdx++) {
            const workerPlan = upsertPlan.chunkAllocations[workerIdx];
            if (workerPlan == undefined) {
                continue;
            }
            promiseHandles.push(this.remoteProxies.invokeMethod("bulkWrite", [workerPlan], workerIdx));
        }
        await Promise.all(promiseHandles);
        diagnostics.set("writeTime", Date.now() - timings);

        //Link 
        timings = Date.now();
        for (const [logicalChunkId, [timeBucket, displacedChunks]] of upsertPlan.chunkDisplacements) {
            const fieldValues = new Array<string>();
            const displacedChunkIds = Array.from(displacedChunks.values());
            for (const displacedChunkId of displacedChunkIds) {
                fieldValues.push(displacedChunkId);
                fieldValues.push(Date.now().toString());
            }
            await this.chunkRegistry.set(logicalChunkId, fieldValues);
        }
        diagnostics.set("linkTime", Date.now() - timings);

        //Update Metadata
        timings = Date.now();
        promiseHandles = new Array<Promise<void>>();
        for (let workerIdx = 0; workerIdx < upsertPlan.chunkAllocations.length; workerIdx++) {
            const workerPlan = upsertPlan.chunkAllocations[workerIdx];
            if (workerPlan == undefined) {
                continue;
            }
            const connectionPaths = new Array<string>();
            for (const plan of workerPlan) {
                connectionPaths.push(plan[0]);
            }
            promiseHandles.push(this.chunkMetaRegistry.metadataSet(connectionPaths, metaKeyBirth, insertTime.toString()));
            promiseHandles.push(this.chunkMetaRegistry.metadataSet(connectionPaths, metaKeyLastWrite, insertTime.toString()));
        }
        await Promise.all(promiseHandles);
        diagnostics.set("metadata", Date.now() - timings);
    }

    public async *iterator(tagIds: bigint[], startInclusive: number, endExclusive: number,
        queryId = "queryId_" + Date.now().toString() + Math.random().toString(),
        nextStepCallback: (timeSteps: [number, number][], tagsSteps: bigint[][], previousTimeStep: [number, number] | undefined, previousTagStep: bigint[] | undefined) => { nextTimeStep?: [number, number], nextTagStep?: bigint[] } = this.chunkPlanner.singleTimeWiseStepDirector,
        timeStepSize = 2 ** 16,
        tagStepSize = 2 ** 6,
        mapLambdaPath: URL | undefined = undefined,
        aggregateFunction: (first: boolean, last: boolean, inputPage: any[][] | null, accumulator: any) => { yield: boolean, yieldValue: any, accumulator: any } = undefined,
        accumulator: any = undefined,
        affinityBasedPlanning = true,
        diagnostics = new Array<Map<string, any>>()): AsyncIterableIterator<any[][]> {

        const timePages = this.chunkPlanner.decomposeByTimePages(startInclusive, endExclusive, timeStepSize);
        const tagPages = this.chunkPlanner.decomposeByTagPages(tagIds, tagStepSize);
        let aggregateResult;
        let nextStep = nextStepCallback(timePages, tagPages, undefined, undefined);

        if (aggregateFunction !== undefined) {
            aggregateResult = aggregateFunction(true, false, null, accumulator);
            if (aggregateResult.yield === true) {
                yield aggregateResult.yieldValue;
            }
        }

        while (nextStep.nextTagStep !== undefined && nextStep.nextTimeStep !== undefined) {
            const stepDiagnostics = new Map<string, any>();
            const pageCursor = this.stepIterator(nextStep.nextTagStep, nextStep.nextTimeStep[0], nextStep.nextTimeStep[1], timeStepSize, tagStepSize, queryId, affinityBasedPlanning, mapLambdaPath, stepDiagnostics);
            if (aggregateFunction !== undefined) {
                for await (const page of pageCursor) {
                    aggregateResult = aggregateFunction(false, false, page, aggregateResult.accumulator);
                    if (aggregateResult.yield === true) {
                        yield aggregateResult.yieldValue;
                    }
                }
            }
            else {
                yield* pageCursor;
            }
            diagnostics.push(stepDiagnostics);
            nextStep = nextStepCallback(timePages, tagPages, nextStep.nextTimeStep, nextStep.nextTagStep);
        }

        if (aggregateFunction !== undefined) {
            aggregateResult = aggregateFunction(false, true, null, aggregateResult.accumulator);
            if (aggregateResult.yield === true) {
                yield aggregateResult.yieldValue;
            }
        }

    }

    private async *stepIterator(tagIds: bigint[], startInclusive: number, endExclusive: number, timeStepSize: number, tagStepSize: number, queryId: string, affinityBasedPlanning: boolean, mapLambdaPath: URL | undefined, diagnostics: Map<string, any>): AsyncIterableIterator<any[][]> {
        let timings = Date.now();
        const iterationPlan = await this.chunkPlanner.planRange(tagIds, startInclusive, endExclusive, timeStepSize, tagStepSize, this.remoteProxies.WorkerCount, affinityBasedPlanning);
        diagnostics.set("planTime", Date.now() - timings);

        timings = Date.now();
        const diagnosticsWorkerPlan = new Array<string>();
        for await (const resultData of this.threadsIterator<any[]>(queryId, iterationPlan.affinityDistributedChunkReads, mapLambdaPath, diagnosticsWorkerPlan)) {
            yield resultData;
        }
        diagnostics.set("workersPlan", diagnosticsWorkerPlan);
        diagnostics.set("yieldTime", Date.now() - timings);
    }

    private async *threadsIterator<T>(queryId: string, affinityDistributedChunkReads: [Set<string>, Set<string>, [number, number], number][][], mapLambdaPath: URL, diagnosticsWorkerPlan: string[]): AsyncIterableIterator<T[]> {
        const threadCursors = new Map<number, AsyncIterableIterator<T[]>>();
        const cursorResults = new Map<number, Promise<{ result: IteratorResult<T[]>, key: number }>>();
        const triggerNextIterationWithKey = (cursor: AsyncIterableIterator<T[]>, key: number) => cursor.next().then((result) => ({ result, key }));
        try {
            //Open Cursors
            for (const [workerIdx, plans] of affinityDistributedChunkReads.entries()) {
                if (plans === undefined || plans.length === 0) { continue; }
                const cursor = this.planIterator<T>(queryId, plans, mapLambdaPath, workerIdx, diagnosticsWorkerPlan);
                threadCursors.set(workerIdx, cursor);
                //Trigger First Iteration
                cursorResults.set(workerIdx, triggerNextIterationWithKey(cursor, workerIdx));
            }

            //Consume Cursors
            while (threadCursors.size > 0) {
                const winnerResult = await Promise.race(cursorResults.values());
                if (winnerResult.result.done === true) {
                    cursorResults.delete(winnerResult.key);
                    threadCursors.delete(winnerResult.key);
                }
                else {
                    yield winnerResult.result.value;
                    cursorResults.set(winnerResult.key, triggerNextIterationWithKey(threadCursors.get(winnerResult.key), winnerResult.key));
                }
            }
        }
        finally {
            for (const [workerIdx, cursor] of threadCursors) {
                cursor.return();
                await this.remoteProxies.invokeMethod<any[]>("clearIteration", [queryId], workerIdx);
            }
            threadCursors.clear();
            cursorResults.clear();
        }
    }

    private async *planIterator<T>(queryId: string, plans: [Set<string>, Set<string>, [number, number], number][], mapLambdaPath: URL, workerIndex: number, diagnosticsWorkerPlan: string[]): AsyncIterableIterator<T[]> {

        let planCounter = 0;
        for (const [connectionPaths, tagNames, [startInclusive, endExclusive], lastWritten] of plans) {

            const cacheIndexKeyHash = crypto.createHash("md5")
                .update(Array.from(connectionPaths).sort().join(""))
                .update(Array.from(tagNames).sort().join(""))
                .update(startInclusive.toString())
                .update(endExclusive.toString())
                .update(mapLambdaPath.toString());

            let totalPages = 0;
            //Cache can expire on 2 accounts 1. last write to the same path 2. Some one linked a new chunk to the path.
            const cacheIndexKey = `plan-${cacheIndexKeyHash.digest("hex")}`;
            let cacheResponse = await this.chunkCache.getFieldValues(cacheIndexKey, [cachedRedirectKey]);
            let actualCacheKey = cacheResponse.get(cachedRedirectKey) ?? "";
            let returnFromCache = actualCacheKey != "";
            cacheResponse = returnFromCache && await this.chunkCache.getFieldValues(actualCacheKey, [cachedTimeKey, cachedPageCountKey]);
            returnFromCache = returnFromCache && cacheResponse.size > 0;
            returnFromCache = returnFromCache && parseInt(cacheResponse.get(cachedTimeKey) ?? "0", 10) >= lastWritten;
            returnFromCache = returnFromCache && parseInt(cacheResponse.get(cachedPageCountKey) ?? "0", 10) > 0;

            const writeCoolDownPeriod = 1000 * 60 * 5;//5 minutes
            const updateCache = Date.now() - lastWritten > writeCoolDownPeriod;
            if (returnFromCache === false) {
                const tempCacheKey = crypto.randomBytes(16).toString("hex");
                const pageCursor = this.pageIterator<T>(queryId, tagNames, connectionPaths, startInclusive, endExclusive, mapLambdaPath, workerIndex);
                for await (const page of pageCursor) {
                    if (page.length > 0) {
                        if (updateCache === true) {
                            await this.chunkCache.set(tempCacheKey, [cachePageKey(totalPages), JSON.stringify(page)]);//We have to save as we cannot hold so many pages in memory
                        }
                        yield page;
                        totalPages++;
                    }
                }
                if (updateCache === true) {
                    await this.chunkCache.set(tempCacheKey, [cachedTimeKey, lastWritten.toString(), cachedPageCountKey, totalPages.toString()]);
                    await this.chunkCache.set(cacheIndexKey, [cachedRedirectKey, tempCacheKey]);//Commit for others to read.
                }
            }
            else {
                totalPages = parseInt(cacheResponse.get(cachedPageCountKey) ?? "0", 10);
                for (let pageCounter = 0; pageCounter < totalPages; pageCounter++) {
                    const pageKey = cachePageKey(pageCounter);
                    const cachedPaged = await this.chunkCache.getFieldValues(actualCacheKey, [pageKey]);
                    if (cachedPaged.size != 0 && cachedPaged.get(pageKey) != undefined) {
                        const page = cachedPaged.size > 0 ? JSON.parse(cachedPaged.get(pageKey) ?? "[]") as T[] : new Array<T>();
                        yield page;
                    }
                    else {
                        //To eliminate this scenario we need to change the query mechanism to paginated results instead of iteration based, 
                        //which can be done by taking shorter steps, but this will increase time and reduce memory.
                        console.warn(`Cache(${actualCacheKey}) was removed or deleted while read was ongoing for ${pageKey} of total pages ${totalPages}`);
                    }
                }
            }
            diagnosticsWorkerPlan.push(`W:${workerIndex} P:${planCounter} CKey:${cacheIndexKey} Cached:${returnFromCache} WritesCooled:${updateCache} Pages:${totalPages}`);
            planCounter++;
        }
    }

    private async *pageIterator<T>(queryId: string, tagNames: Set<string>, connectionPaths: Set<string>, startInclusive: number, endExclusive: number, mapLambdaPath: URL, workerIndex: number): AsyncIterableIterator<T[]> {
        let page = new Array<T>();
        do {
            page = await this.remoteProxies.invokeMethod<T[]>("bulkIterate", [queryId, tagNames, connectionPaths, startInclusive, endExclusive, 10000, mapLambdaPath?.toString()], workerIndex);
            if (page.length > 0) {
                yield page;
            }
        }
        while (page.length > 0)
    }
}