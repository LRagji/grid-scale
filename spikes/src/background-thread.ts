import { ChunkCache } from "./chunk-cache.js";
import { upsertPlan } from "./grid-scale.js";
import { ISyncRunner } from "./threads/i-sync-runner.js";
import { IThreadCommunication } from "./threads/i-thread-communication.js";

export class BackgroundThread implements ISyncRunner<any> {
    public readonly name: "Execute";
    private chunkCache: ChunkCache;
    private identity: string;

    initialize(identity: string, args: any): void {
        this.chunkCache = new ChunkCache(args.config, args.cacheLimit, args.bulkDropLimit);
        this.identity = identity;
    }

    work(input: IThreadCommunication<any>): IThreadCommunication<any> {
        if (input.subHeader.toLowerCase() == "store") {
            const upsertPlan = input.payload as upsertPlan<Array<unknown>>;
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
        return {
            header: "Response",
            subHeader: "Error",
            payload: `Unknown subHeader: ${input.subHeader}`
        };
    }

    public [Symbol.dispose]() {
        this.chunkCache[Symbol.dispose]();
    }
}