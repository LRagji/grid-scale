import { ChunkPlanner } from "./chunk-planner.js";
import { StatefulProxyManager } from "node-apparatus";
import { INonVolatileHashMap } from "./non-volatile-hash-map/i-non-volatile-hash-map.js";
import { IChunk } from "./chunk/i-chunk.js";
import { ChunkFactoryBase } from "./chunk/chunk-factory-base.js";

export class GridScale {

    private chunkPluginFactoryType: typeof ChunkFactoryBase<IChunk>;
    constructor(
        private readonly chunkRegistry: INonVolatileHashMap,
        private readonly chunkPlanner: ChunkPlanner,
        private readonly remoteProxies: StatefulProxyManager,
        private readonly chunkPluginFactoryPath: URL
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

        timings = Date.now();
        const promiseHandles = new Array<Promise<void>>();
        for (let workerIdx = 0; workerIdx < upsertPlan.chunkAllocations.length; workerIdx++) {
            const workerPlan = upsertPlan.chunkAllocations[workerIdx];
            if (workerPlan == undefined) {
                continue;
            }
            promiseHandles.push(this.remoteProxies.invokeMethod("bulkWrite", [workerPlan], workerIdx));
        }
        await Promise.all(promiseHandles);
        diagnostics.set("writeTime", Date.now() - timings);

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
    }

    public async *iteratorByTimePage(tagIds: bigint[], startInclusive: number, endExclusive: number, queryId = "queryId_" + Math.random().toString(), resultPageSize = 10000, diagnostics = new Map<string, any>()): AsyncIterableIterator<any[][]> {
        let timings = Date.now();
        const iterationPlan = await this.chunkPlanner.planRangeIterationByTime(tagIds, startInclusive, endExclusive, this.remoteProxies.WorkerCount);
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
        const workerPromises = new Map<number, Promise<unknown>>();
        let startInclusiveWorkerIndex = 0;
        let endExclusiveWorkerIndex = iterationPlan.affinityDistributedChunkReads.length;
        let pageCounter = 0;
        let rowsCounter = 0;
        try {
            do {
                for (let workerIdx = startInclusiveWorkerIndex; workerIdx < endExclusiveWorkerIndex; workerIdx++) {
                    const plans = iterationPlan.affinityDistributedChunkReads[workerIdx];
                    if (plans !== undefined) {
                        const resultPromise = async () => {
                            const results = await this.remoteProxies.invokeMethod<any[]>("bulkIterate", [queryId, plans, startInclusive, endExclusive, resultPageSize], workerIdx);
                            return [workerIdx, results];
                        }
                        workerPromises.set(workerIdx, resultPromise());
                    }
                }
                const completedThreadResult = await Promise.race(workerPromises.values());
                const workerIdx = completedThreadResult[0];
                const pageData = completedThreadResult[1];
                pageCounter++;
                rowsCounter += pageData.length;
                if (pageData.length === 0) {
                    workerPromises.delete(workerIdx);
                    startInclusiveWorkerIndex = 0;
                    endExclusiveWorkerIndex = 0;
                }
                else {
                    startInclusiveWorkerIndex = workerIdx;
                    endExclusiveWorkerIndex = startInclusiveWorkerIndex + 1;
                }

                yield pageData;
            }
            while (workerPromises.size > 0)
        }
        finally {
            workerPromises.clear();
            for (let workerIdx = 0; workerIdx < iterationPlan.affinityDistributedChunkReads.length; workerIdx++) {
                await this.remoteProxies.invokeMethod<any[]>("clearIteration", [queryId], workerIdx);
            }
        }
        diagnostics.set("dataPageCounter", pageCounter);
        diagnostics.set("rowCounter", rowsCounter);
        diagnostics.set("yieldTime", Date.now() - timings);
    }

    public fetchTimePages(startInclusive: number, endExclusive: number): [number, number][] {
        return this.chunkPlanner.decomposeTimeRangeByPages(startInclusive, endExclusive);
    }
}