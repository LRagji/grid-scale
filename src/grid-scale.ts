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
        private readonly lambdaCache: INonVolatileHashMap
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
        resultPageSize = 10000,
        mapLambdaPath: URL | undefined = undefined,
        aggregateFunction: (first: boolean, last: boolean, inputPage: any[][] | null, accumulator: any) => { yield: boolean, yieldValue: any, accumulator: any } = undefined,
        accumulator: any = undefined,
        affinityBasedPlanning = true,
        diagnostics = new Array<Map<string, any>>()): AsyncIterableIterator<any[][]> {

        const timePages = this.chunkPlanner.decomposeByTimePages(startInclusive, endExclusive);
        const tagPages = this.chunkPlanner.decomposeByTagPages(tagIds);
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
            const pageCursor = this.stepIterator(nextStep.nextTagStep, nextStep.nextTimeStep[0], nextStep.nextTimeStep[1], queryId, resultPageSize, affinityBasedPlanning, mapLambdaPath, stepDiagnostics);
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

    private async *stepIterator(tagIds: bigint[], startInclusive: number, endExclusive: number, queryId: string, resultPageSize: number, affinityBasedPlanning: boolean, mapLambdaPath: URL | undefined, diagnostics: Map<string, any>): AsyncIterableIterator<any[][]> {
        let timings = Date.now();
        const iterationPlan = await this.chunkPlanner.planRange(tagIds, startInclusive, endExclusive, this.remoteProxies.WorkerCount, affinityBasedPlanning);
        const diagnosticsWorkerPlan = new Array<string>();
        for (const [workerIdx, plans] of iterationPlan.affinityDistributedChunkReads.entries()) {
            if (plans === undefined) { continue; }
            for (const [planIdx, plan] of plans.entries()) {
                diagnosticsWorkerPlan.push(`worker:${workerIdx} plan:${planIdx} shards:${plan[0].size} tags:${plan[1].size}`);
            }
        }
        diagnostics.set("workersPlan", diagnosticsWorkerPlan);
        diagnostics.set("planTime", Date.now() - timings);

        timings = Date.now();
        for await (const resultData of this.threadsIterator<any[]>(queryId, iterationPlan.affinityDistributedChunkReads, resultPageSize, mapLambdaPath)) {
            yield resultData;
        }
        diagnostics.set("yieldTime", Date.now() - timings);
    }

    private async *threadsIterator<T>(queryId: string, affinityDistributedChunkReads: [Set<string>, Set<string>, [number, number], number][][], resultPageSize: number, mapLambdaPath: URL): AsyncIterableIterator<T[]> {
        const threadCursors = new Map<number, AsyncIterableIterator<T[]>>();
        const cursorResults = new Map<number, Promise<{ result: IteratorResult<T[]>, key: number }>>();
        const triggerNextIterationWithKey = (cursor: AsyncIterableIterator<T[]>, key: number) => cursor.next().then((result) => ({ result, key }));
        try {
            //Open Cursors
            for (const [workerIdx, plans] of affinityDistributedChunkReads.entries()) {
                if (plans === undefined || plans.length === 0) { continue; }
                const cursor = this.planIterator<T>(queryId, plans, resultPageSize, mapLambdaPath, workerIdx);
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

    private async *planIterator<T>(queryId: string, plans: [Set<string>, Set<string>, [number, number], number][], resultPageSize: number, mapLambdaPath: URL, workerIndex: number): AsyncIterableIterator<T[]> {

        for (const [connectionPaths, tagNames, [startInclusive, endExclusive], lastWritten] of plans) {

            const cacheIndexKeyHash = crypto.createHash("md5")
                .update(Array.from(connectionPaths).sort().join(""))
                .update(Array.from(tagNames).sort().join(""))
                .update(startInclusive.toString())
                .update(endExclusive.toString())
                .update(resultPageSize.toString())
                .update(mapLambdaPath.toString());

            const cacheIndexKey = `plan-${cacheIndexKeyHash.digest("hex")}`;
            let cacheResponse = await this.lambdaCache.getFieldValues(cacheIndexKey, [cachedRedirectKey]);
            let actualCacheKey = cacheResponse.get(cachedRedirectKey) ?? "";
            let returnFromCache = actualCacheKey != "";
            cacheResponse = returnFromCache && await this.lambdaCache.getFieldValues(actualCacheKey, [cachedTimeKey, cachedPageCountKey]);
            returnFromCache = returnFromCache && cacheResponse.size > 0;
            returnFromCache = returnFromCache && parseInt(cacheResponse.get(cachedTimeKey) ?? "0", 10) >= lastWritten;
            returnFromCache = returnFromCache && parseInt(cacheResponse.get(cachedPageCountKey) ?? "0", 10) > 0;

            if (returnFromCache === false) {
                let pageCounter = 0;
                const tempCacheKey = crypto.randomBytes(16).toString("hex");
                const pageCursor = this.pageIterator<T>(queryId, tagNames, connectionPaths, startInclusive, endExclusive, resultPageSize, mapLambdaPath, workerIndex);
                for await (const page of pageCursor) {
                    if (page.length > 0) {
                        await this.lambdaCache.set(tempCacheKey, [cachePageKey(pageCounter), JSON.stringify(page)]);//We have to save as we cannot hold so many pages in memory
                        yield page;
                        pageCounter++;
                    }
                }
                await this.lambdaCache.set(tempCacheKey, [cachedTimeKey, lastWritten.toString(), cachedPageCountKey, pageCounter.toString()]);
                await this.lambdaCache.set(cacheIndexKey, [cachedRedirectKey, tempCacheKey]);//Commit for others to read.
            }
            else {
                const pageCount = parseInt(cacheResponse.get(cachedPageCountKey) ?? "0", 10);
                for (let pageCounter = 0; pageCounter < pageCount; pageCounter++) {
                    const pageKey = cachePageKey(pageCounter);
                    const cachedPaged = await this.lambdaCache.getFieldValues(actualCacheKey, [pageKey]);
                    if (cachedPaged.size != 0 && cachedPaged.get(pageKey) != undefined) {
                        const page = cachedPaged.size > 0 ? JSON.parse(cachedPaged.get(pageKey) ?? "[]") as T[] : new Array<T>();
                        yield page;
                    }
                    else {
                        //To eliminate this scenario we need to change the query mechanism to paginated results instead of iteration based.
                        console.warn(`Cache(${actualCacheKey}) was removed or deleted while read was ongoing for ${pageKey} of total pages ${pageCount}`);
                    }
                }
            }
        }
    }

    private async *pageIterator<T>(queryId: string, tagNames: Set<string>, connectionPaths: Set<string>, startInclusive: number, endExclusive: number, resultPageSize: number, mapLambdaPath: URL, workerIndex: number): AsyncIterableIterator<T[]> {
        let page = new Array<T>();
        do {
            page = await this.remoteProxies.invokeMethod<T[]>("bulkIterate", [queryId, tagNames, connectionPaths, startInclusive, endExclusive, resultPageSize, mapLambdaPath?.toString()], workerIndex);
            if (page.length > 0) {
                yield page;
            }
        }
        while (page.length > 0)
    }
}