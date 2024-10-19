import { ChunkCache } from "./chunk-cache.js";
import { ChunkPlanner } from "./chunk-planner.js";
import { IChunk } from "./chunk/i-chunk.js";
import { INonVolatileHashMap } from "./non-volatile-hash-map/i-non-volatile-hash-map.js";

export class GridScale {

    constructor(
        private readonly chunkRegistry: INonVolatileHashMap,
        private readonly chunkPlanner: ChunkPlanner,
        private readonly chunkCache: ChunkCache<IChunk>,
        private readonly writeFileName: (callerSignature: string) => string,
        private readonly mergeFunction: <T>(cursors: IterableIterator<T>[]) => IterableIterator<T>,
    ) { }

    public async store(records: Map<string, any[]>, recordLength: number, recordTimestampIndex: number, insertTime = Date.now(), diagnostics = new Map<string, any>()): Promise<void> {
        let timings = Date.now();
        const upsertPlan = this.chunkPlanner.planUpserts(records, recordLength, recordTimestampIndex, insertTime);
        diagnostics.set("planTime", Date.now() - timings);

        timings = Date.now();
        for (const [connectionPath, tagRecords] of upsertPlan.chunkAllocations) {
            const chunk = this.chunkCache.getChunk(connectionPath, "write", this.writeFileName(process.pid.toString()));
            chunk.bulkSet(tagRecords);
        }
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
        for (const [connectionPaths, tagSet] of iterationPlan.chunkReads) {
            const chunkIterators = new Array<IterableIterator<any>>();
            for (const connectionPath of connectionPaths) {
                const chunk = this.chunkCache.getChunk(connectionPath, "read", this.writeFileName(process.pid.toString()));
                chunkIterators.push(chunk.bulkIterator(Array.from(tagSet.values()), startInclusive, endExclusive));
            }
            yield* this.mergeFunction(chunkIterators);
        }
        diagnostics.set("yieldTime", Date.now() - timings);
    }
}