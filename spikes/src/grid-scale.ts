import { ChunkPlanner } from "./chunk-planner.js";
import { LongRunnerProxies } from "./multi-threads/long-runner-proxies.js";
import { INonVolatileHashMap } from "./non-volatile-hash-map/i-non-volatile-hash-map.js";

export class GridScale {

    constructor(
        private readonly chunkRegistry: INonVolatileHashMap,
        private readonly chunkPlanner: ChunkPlanner,
        private readonly remoteProxies: LongRunnerProxies
    ) { }

    public async store(records: Map<string, any[]>, recordLength: number, recordTimestampIndex: number, insertTime = Date.now(), diagnostics = new Map<string, any>()): Promise<void> {
        let timings = Date.now();
        const upsertPlan = this.chunkPlanner.planUpserts(records, recordLength, recordTimestampIndex, insertTime, this.remoteProxies.WorkerCount);
        diagnostics.set("planTime", Date.now() - timings);

        timings = Date.now();
        const promiseHandles = new Array<Promise<void>>();
        for (let workerIdx = 0; workerIdx < upsertPlan.chunkAllocations.length; workerIdx++) {
            promiseHandles.push(this.remoteProxies.invokeRemoteMethod("bulkWrite", [upsertPlan.chunkAllocations[workerIdx]], workerIdx));
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

    public async *iteratorByTimePage(tags: string[], startInclusive: number, endExclusive: number, queryId = "queryId_" + Math.random().toString(), diagnostics = new Map<string, any>()): AsyncIterableIterator<any[]> {
        let timings = Date.now();
        const iterationPlan = await this.chunkPlanner.planRangeIterationByTime(tags, startInclusive, endExclusive, this.remoteProxies.WorkerCount);
        diagnostics.set("planTime", Date.now() - timings);

        timings = Date.now();
        const workerPromises = new Map<number, Promise<unknown>>();
        let startInclusiveWorkerIndex = 0;
        let endExclusiveWorkerIndex = iterationPlan.affinityDistributedChunkReads.length;
        const pageSize = 1000;
        try {
            do {
                for (let workerIdx = startInclusiveWorkerIndex; workerIdx < endExclusiveWorkerIndex; workerIdx++) {
                    const plans = iterationPlan.affinityDistributedChunkReads[workerIdx];
                    const resultPromise = async () => {
                        const results = await this.remoteProxies.invokeRemoteMethod<any[]>("bulkIterate", [queryId, plans, startInclusive, endExclusive, pageSize], workerIdx);
                        return [workerIdx, results];
                    }
                    workerPromises.set(workerIdx, resultPromise());
                }
                const completedThreadResult = await Promise.race(workerPromises.values());
                const workerIdx = completedThreadResult[0];
                const pageData = completedThreadResult[1];
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
                await this.remoteProxies.invokeRemoteMethod<any[]>("clearIteration", [queryId]);
            }
        }
        diagnostics.set("yieldTime", Date.now() - timings);
    }


}