import { ChunkPlanner } from "./chunk-planner.js";
import { GridThreadPlugin } from "./multi-threads/grid-thread-plugin.js";
import { IProxyMethod, serialize } from "./multi-threads/i-proxy-method.js";
import { LongRunnerProxy } from "./multi-threads/long-runner-proxy.js";
import { INonVolatileHashMap } from "./non-volatile-hash-map/i-non-volatile-hash-map.js";

export class GridScale {

    constructor(
        private readonly chunkRegistry: INonVolatileHashMap,
        private readonly chunkPlanner: ChunkPlanner,
        private readonly gridThreadPlugin: GridThreadPlugin,
        private readonly workers: LongRunnerProxy[]
    ) { }

    public async store(records: Map<string, any[]>, recordLength: number, recordTimestampIndex: number, insertTime = Date.now(), diagnostics = new Map<string, any>()): Promise<void> {
        let timings = Date.now();
        const upsertPlan = this.chunkPlanner.planUpserts(records, recordLength, recordTimestampIndex, insertTime);
        diagnostics.set("planTime", Date.now() - timings);

        timings = Date.now();
        const writePayload = Array.from(upsertPlan.chunkAllocations.entries());
        const chunkSize = Math.ceil(writePayload.length / this.workers.length);
        const promiseHandles = new Array<Promise<void>>();
        for (let workerIdx = 0; workerIdx < this.workers.length; workerIdx++) {
            const bulkWrites = writePayload.splice(0, chunkSize);
            promiseHandles.push(this.workers[workerIdx].invokeRemoteMethod("bulkWrite", [bulkWrites]));
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

    public async *iteratorByTimePage(tags: string[], startInclusive: number, endExclusive: number, diagnostics = new Map<string, any>()): AsyncIterableIterator<any[]> {
        let timings = Date.now();
        const iterationPlan = await this.chunkPlanner.planRangeIterationByTime(tags, startInclusive, endExclusive);
        diagnostics.set("planTime", Date.now() - timings);

        timings = Date.now();
        const pageSize = 1000;
        const workerPromises = new Array<[unknown, Promise<any[]>]>();
        const chunkSize = Math.ceil(iterationPlan.chunkReads.length / this.workers.length);
        for (let workerIdx = 0; workerIdx < this.workers.length; workerIdx++) {
            const plan = {
                queryId: "queryId_" + Math.random().toString() + "_" + workerIdx,
                plan: iterationPlan.chunkReads.splice(0, chunkSize),
                startInclusive, endExclusive
            };

            const promise = this.workers[workerIdx].invokeRemoteMethod<any[]>("bulkIterate", [plan]);
            workerPromises.push([plan, promise]);
        }

        do {
            const results = await Promise.race(workerPromises.map(async ([plan, readPromise], idx) => await readPromise));
        }
        while (workerPromises.length > 0)

        for (const [connectionPaths, tagSet] of iterationPlan.chunkReads) {
            const queryId = "queryId" + Math.random().toString();
            try {
                let pagedData = new Array<any>();
                do {
                    pagedData = this.gridThreadPlugin.iterate(queryId, connectionPaths, tagSet, startInclusive, endExclusive, pageSize);
                    yield* pagedData;
                }
                while (pagedData.length >= pageSize);
            }
            finally {
                this.gridThreadPlugin.clearIteration(queryId);
            }
        }
        diagnostics.set("yieldTime", Date.now() - timings);
    }
}