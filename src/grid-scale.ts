import crypto from "node:crypto";
import { ChunkPlanner } from "./chunk-planner.js";
import { StatefulProxyManager } from "node-apparatus";
import { INonVolatileHashMap } from "./non-volatile-hash-map/i-non-volatile-hash-map.js";
import { IChunk } from "./types/i-chunk.js";
import { ChunkFactoryBase } from "./types/chunk-factory-base.js";
import { IChunkMetadata } from "./types/i-chunk-metadata.js";
import { metaKeyBirth, metaKeyLastWrite } from "./types/meta-keys.js";
import { cachedPageCountKey, cachedRedirectKey, cachedTimeKey, cachePageKey } from "./types/cache-keys.js";
import { IteratorPaginationConfig } from "./types/iterator-pagination-config.js";
import { IteratorLambdaConfig } from "./types/iterator-lambda-config.js";
import { IteratorPlanConfig } from "./types/iterator-plan-config.js";
import { IteratorCacheConfig } from "./types/iterator-cache-config.js";
import { unzipSync } from "node:zlib";

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
        planConfig: IteratorPlanConfig = new IteratorPlanConfig(),
        paginationConfig: IteratorPaginationConfig = new IteratorPaginationConfig(),
        cacheConfig: IteratorCacheConfig = new IteratorCacheConfig(),
        lambdaConfig: IteratorLambdaConfig<any> = new IteratorLambdaConfig<undefined>()): AsyncIterableIterator<any[][]> {

        const timePages = this.chunkPlanner.decomposeByTimePages(startInclusive, endExclusive, paginationConfig.TimeStepSize);
        const tagPages = this.chunkPlanner.decomposeByTagPages(tagIds, paginationConfig.TagStepSize);
        let aggregateResult;
        let nextStep = paginationConfig.nextStepCallback(timePages, tagPages, undefined, undefined);

        if (lambdaConfig.aggregateLambda !== undefined) {
            aggregateResult = lambdaConfig.aggregateLambda(true, false, null, lambdaConfig.accumulator);
            if (aggregateResult.yield === true) {
                yield aggregateResult.yieldValue;
            }
        }

        while (nextStep.nextTagStep !== undefined && nextStep.nextTimeStep !== undefined) {
            const stepDiagnostics = new Map<string, any>();
            const pageCursor = this.stepIterator(nextStep.nextTagStep, nextStep.nextTimeStep[0], nextStep.nextTimeStep[1], paginationConfig.TimeStepSize, paginationConfig.TagStepSize, planConfig.perThreadPageSize, planConfig.queryId, planConfig.affinityBasedPlan, lambdaConfig.windowLambdaPath?.toString() ?? "", stepDiagnostics, cacheConfig.readCache, cacheConfig.updateCache, cacheConfig.resultCoolDownTime, planConfig.zippedResults);
            if (lambdaConfig.aggregateLambda !== undefined) {
                for await (const page of pageCursor) {
                    aggregateResult = lambdaConfig.aggregateLambda(false, false, page, aggregateResult.accumulator);
                    if (aggregateResult.yield === true) {
                        yield aggregateResult.yieldValue;
                    }
                }
            }
            else {
                yield* pageCursor;
            }
            planConfig.diagnostics.push(stepDiagnostics);
            nextStep = paginationConfig.nextStepCallback(timePages, tagPages, nextStep.nextTimeStep, nextStep.nextTagStep);
        }

        if (lambdaConfig.aggregateLambda !== undefined) {
            aggregateResult = lambdaConfig.aggregateLambda(false, true, null, aggregateResult.accumulator);
            if (aggregateResult.yield === true) {
                yield aggregateResult.yieldValue;
            }
        }

    }

    private async *stepIterator(tagIds: bigint[], startInclusive: number, endExclusive: number, timeStepSize: number, tagStepSize: number, resultPageSize: number, queryId: string, affinityBasedPlanning: boolean, mapLambdaPath: string, diagnostics: Map<string, any>, readCache: boolean, updateCache: boolean, resultCoolDownTime: number, zippedResults: boolean): AsyncIterableIterator<any[][]> {
        let timings = Date.now();
        const iterationPlan = await this.chunkPlanner.planRange(tagIds, startInclusive, endExclusive, timeStepSize, tagStepSize, this.remoteProxies.WorkerCount, affinityBasedPlanning);
        diagnostics.set("planTime", Date.now() - timings);

        timings = Date.now();
        const diagnosticsWorkerPlan = new Array<string>();
        for await (const resultData of this.threadsIterator<any[]>(queryId, iterationPlan.affinityDistributedChunkReads, resultPageSize, mapLambdaPath, diagnosticsWorkerPlan, readCache, updateCache, resultCoolDownTime, zippedResults)) {
            yield resultData;
        }
        diagnostics.set("workersPlan", diagnosticsWorkerPlan);
        diagnostics.set("yieldTime", Date.now() - timings);
    }

    private async *threadsIterator<T>(queryId: string, affinityDistributedChunkReads: [Set<string>, Set<string>, [number, number], number][][], resultPageSize: number, mapLambdaPath: string, diagnosticsWorkerPlan: string[], readCache: boolean, updateCache: boolean, resultCoolDownTime: number, zippedResults: boolean): AsyncIterableIterator<T[]> {
        const threadCursors = new Map<number, AsyncIterableIterator<T[]>>();
        const cursorResults = new Map<number, Promise<{ result: IteratorResult<T[]>, key: number }>>();
        const triggerNextIterationWithKey = (cursor: AsyncIterableIterator<T[]>, key: number) => cursor.next().then((result) => ({ result, key }));
        try {
            //Open Cursors
            for (const [workerIdx, plans] of affinityDistributedChunkReads.entries()) {
                if (plans === undefined || plans.length === 0) { continue; }
                const cursor = this.planIterator<T>(queryId, plans, resultPageSize, mapLambdaPath, workerIdx, diagnosticsWorkerPlan, readCache, updateCache, resultCoolDownTime, zippedResults);
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

    private async *planIterator<T>(queryId: string, plans: [Set<string>, Set<string>, [number, number], number][], resultPageSize: number, mapLambdaPath: string, workerIndex: number, diagnosticsWorkerPlan: string[], readCache: boolean, updateCache: boolean, resultCoolDownTime: number, zippedResults: boolean): AsyncIterableIterator<T[]> {

        let planCounter = 0;
        for (const [connectionPaths, tagNames, [startInclusive, endExclusive], lastWritten] of plans) {
            const taskStartTime = Date.now();
            let returnFromCache = readCache;
            const cacheIndexKeyHash = returnFromCache && crypto.createHash("md5")
                .update(Array.from(connectionPaths).sort().join(""))
                .update(Array.from(tagNames).sort().join(""))
                .update(startInclusive.toString())
                .update(endExclusive.toString())
                .update(resultPageSize.toString())
                .update(zippedResults.toString())
                .update(mapLambdaPath);

            let totalPages = 0;
            //Cache can expire on 2 accounts 1. last write to the same path 2. Some one linked a new chunk to the path.
            const cacheIndexKey = returnFromCache && `plan-${cacheIndexKeyHash.digest("hex")}`;
            let cacheResponse = returnFromCache && await this.chunkCache.getFieldValues(cacheIndexKey, [cachedRedirectKey]);
            let actualCacheKey = returnFromCache && (cacheResponse.get(cachedRedirectKey) ?? "");
            returnFromCache = returnFromCache && actualCacheKey != "";
            cacheResponse = returnFromCache && await this.chunkCache.getFieldValues(actualCacheKey, [cachedTimeKey, cachedPageCountKey]);
            returnFromCache = returnFromCache && cacheResponse.size > 0;
            returnFromCache = returnFromCache && parseInt(cacheResponse.get(cachedTimeKey) ?? "0", 10) >= lastWritten;
            returnFromCache = returnFromCache && parseInt(cacheResponse.get(cachedPageCountKey) ?? "0", 10) > 0;


            const updateCacheInternal = updateCache && Date.now() - lastWritten > resultCoolDownTime;
            if (returnFromCache === false) {
                const tempCacheKey = crypto.randomBytes(16).toString("hex");
                const pageCursor = this.pageIterator<T>(queryId, tagNames, connectionPaths, startInclusive, endExclusive, resultPageSize, mapLambdaPath, workerIndex, zippedResults);
                for await (const page of pageCursor) {
                    if (page.length > 0) {
                        if (updateCacheInternal === true) {
                            await this.chunkCache.set(tempCacheKey, [cachePageKey(totalPages), JSON.stringify(page)]);//We have to save each page at a time as we cannot hold so many pages in memory
                        }
                        if (zippedResults === true) {
                            const unZippedPage = JSON.parse(unzipSync(Buffer.from(page as unknown as Uint8Array)).toString());
                            yield unZippedPage;
                        }
                        else {
                            yield page;
                        }
                        totalPages++;
                    }
                }
                if (updateCacheInternal === true) {
                    await this.chunkCache.set(tempCacheKey, [cachedTimeKey, lastWritten.toString(), cachedPageCountKey, totalPages.toString()]);
                    await this.chunkCache.set(cacheIndexKey, [cachedRedirectKey, tempCacheKey]);//Commit for others to read.
                }
            }
            else {
                totalPages = parseInt(cacheResponse.get(cachedPageCountKey) ?? "0", 10);
                for (let pageCounter = 0; pageCounter < totalPages; pageCounter++) {
                    const pageKey = cachePageKey(pageCounter);
                    const cachedPaged = await this.chunkCache.getFieldValues(actualCacheKey, [pageKey]);
                    const cachedPage = cachedPaged.get(pageKey);
                    if (cachedPaged.size != 0 && cachedPage != undefined) {
                        if (zippedResults === true) {
                            const unZippedPage = JSON.parse(unzipSync(Buffer.from(JSON.parse(cachedPage) as unknown as Uint8Array)).toString());
                            yield unZippedPage;
                        }
                        else {
                            const page = cachedPaged.size > 0 ? JSON.parse(cachedPage) as T[] : new Array<T>();
                            yield page;
                        }
                    }
                    else {
                        //To eliminate this scenario we need to change the query mechanism to paginated results instead of iteration based, 
                        //which can be done by taking shorter steps, but this will increase time and reduce memory.
                        console.warn(`Cache(${actualCacheKey}) was removed or deleted while read was ongoing for ${pageKey} of total pages ${totalPages} for Query:${queryId}`);
                    }
                }
            }
            diagnosticsWorkerPlan.push(`W:${workerIndex} P:${planCounter} CKey:${cacheIndexKey} Cached:${returnFromCache} WritesCooled:${updateCache} Pages:${totalPages} T:${Date.now() - taskStartTime}`);
            planCounter++;
        }
    }

    private async *pageIterator<T>(queryId: string, tagNames: Set<string>, connectionPaths: Set<string>, startInclusive: number, endExclusive: number, resultPageSize: number, mapLambdaPath: string, workerIndex: number, zippedResults: boolean): AsyncIterableIterator<T[]> {
        let page = new Array<T>();
        do {
            page = await this.remoteProxies.invokeMethod<T[]>("bulkIterate", [queryId, tagNames, connectionPaths, startInclusive, endExclusive, resultPageSize, mapLambdaPath, zippedResults], workerIndex);
            if (page.length > 0) {
                yield page;
            }
        }
        while (page.length > 0)
    }
}