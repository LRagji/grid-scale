import { ChunkCache } from "./chunk-cache.js";
import { upsertPlan } from "./grid-scale.js";
import { TConfig } from "./t-config.js";
import { ISyncPlugin } from "./threads/i-sync-plugin.js";
import { IThreadCommunication } from "./threads/i-thread-communication.js";

export type BackgroundPluginInitializeParam = { config: TConfig, cacheLimit: number, bulkDropLimit: number };

export class BackgroundPlugin implements ISyncPlugin<BackgroundPluginInitializeParam> {

    public static readonly invokeHeader: "BackgroundPlugin" = "BackgroundPlugin";
    public static readonly invokeSubHeaders = { "Store": "Store" };

    public readonly name = BackgroundPlugin.invokeHeader;

    private chunkCache: ChunkCache;
    private identity: string;

    initialize(identity: string, param: BackgroundPluginInitializeParam): void {
        this.chunkCache = new ChunkCache(param.config, param.cacheLimit, param.bulkDropLimit);
        this.identity = identity;
    }

    work(input: IThreadCommunication<upsertPlan<Array<unknown>>>): IThreadCommunication<string> {
        if (input.subHeader == BackgroundPlugin.invokeSubHeaders.Store) {
            const upsertPlan = input.payload;
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

        throw new Error(`Unknown subHeader: ${input.subHeader}`);
    }

    public [Symbol.dispose]() {
        this.chunkCache[Symbol.dispose]();
    }
}