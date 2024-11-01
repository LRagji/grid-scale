import { ChunkPlanner } from "./chunk-planner.js";
import { StatefulProxyManager } from "node-apparatus";
import { INonVolatileHashMap } from "./non-volatile-hash-map/i-non-volatile-hash-map.js";

export class GridScale {

    constructor(
        private readonly chunkRegistry: INonVolatileHashMap,
        private readonly chunkPlanner: ChunkPlanner,
        private readonly remoteProxies: StatefulProxyManager
    ) { }

    public async store(records: Map<string, any[]>, recordLength: number, recordTimestampIndex: number, recordInsertTimeIndex: number, insertTime = Date.now(), diagnostics = new Map<string, any>()): Promise<void> {
        let timings = Date.now();
        const upsertPlan = this.chunkPlanner.planUpserts(records, recordLength, recordTimestampIndex, recordInsertTimeIndex, insertTime, this.remoteProxies.WorkerCount);
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

    public async *iteratorByTimePage(tags: string[], startInclusive: number, endExclusive: number, queryId = "queryId_" + Math.random().toString(), pageSize = 10000, diagnostics = new Map<string, any>()): AsyncIterableIterator<any[]> {
        let timings = Date.now();
        const iterationPlan = await this.chunkPlanner.planRangeIterationByTime(tags, startInclusive, endExclusive, this.remoteProxies.WorkerCount);
        for (const [workerIdx, plans] of iterationPlan.affinityDistributedChunkReads.entries()) {
            if (plans === undefined) { continue; }
            for (const [planIdx, plan] of plans.entries()) {
                diagnostics.set(`Worker:${workerIdx} Plan:${planIdx}`, `Sections:${plan[0].size} Tags:${plan[1].size}`);
            }
        }
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
                            const results = await this.remoteProxies.invokeMethod<any[]>("bulkIterate", [queryId, plans, startInclusive, endExclusive, pageSize], workerIdx);
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

                yield* pageData;
            }
            while (workerPromises.size > 0)
        }
        finally {
            workerPromises.clear();
            for (let workerIdx = 0; workerIdx < iterationPlan.affinityDistributedChunkReads.length; workerIdx++) {
                await this.remoteProxies.invokeMethod<any[]>("clearIteration", [queryId], workerIdx);
            }
        }
        diagnostics.set("pageCounter", pageCounter);
        diagnostics.set("rowCounter", rowsCounter);
        diagnostics.set("yieldTime", Date.now() - timings);
    }


}