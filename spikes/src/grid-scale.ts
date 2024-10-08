import { Chunk } from "./chunk.js";
import { ChunkId } from "./chunk-id.js";
import { TConfig } from "./t-config.js";

export type logicalChunkId = string;
export type diskIndex = number;
export type tagName = string;
export type samples = number[];

export class GridScale<T> {

    private readonly chunkCache = new Map<logicalChunkId, Chunk>();

    constructor(private readonly config: TConfig) { }

    public allocateChunks(sampleSets: Map<tagName, samples>): Map<logicalChunkId, Map<diskIndex, Map<tagName, samples>>> {
        const chunkGroups = new Map<logicalChunkId, Map<diskIndex, Map<tagName, samples>>>(); //ChunkID->DiskIndex->TagName->Samples[]
        for (const [tagName, samples] of sampleSets) {
            for (let timeIndex = 0; timeIndex < samples.length; timeIndex += 2) {
                const chunkId = ChunkId.from(tagName, samples[timeIndex], this.config);
                const diskIndexSamplesMap = chunkGroups.get(chunkId.logicalChunkId) || new Map<diskIndex, Map<tagName, samples>>();
                const diskIndex = chunkId.tagNameMod(this.config.setPaths.get(this.config.activePath).length);
                const sampleSets = diskIndexSamplesMap.get(diskIndex) || new Map<tagName, samples>();
                sampleSets.set(tagName, samples);
                diskIndexSamplesMap.set(diskIndex, sampleSets);
                chunkGroups.set(chunkId.logicalChunkId, diskIndexSamplesMap);
            }
        }
        return chunkGroups;
    }

    public allocateLinking(sampleSets: Map<tagName, samples>): Map<logicalChunkId, Map<diskIndex, Map<tagName, samples>>> {
        const chunkGroups = new Map<logicalChunkId, Map<diskIndex, Map<tagName, samples>>>(); //ChunkID->DiskIndex->TagName->Samples[]
        for (const [tagName, samples] of sampleSets) {
            for (let timeIndex = 0; timeIndex < samples.length; timeIndex += 2) {
                const chunkId = ChunkId.from(tagName, samples[timeIndex], this.config);
                const diskIndexSamplesMap = chunkGroups.get(chunkId.logicalChunkId) || new Map<diskIndex, Map<tagName, samples>>();
                const diskIndex = chunkId.tagNameMod(this.config.setPaths.get(this.config.activePath).length);
                const sampleSets = diskIndexSamplesMap.get(diskIndex) || new Map<tagName, samples>();
                sampleSets.set(tagName, samples);
                diskIndexSamplesMap.set(diskIndex, sampleSets);
                chunkGroups.set(chunkId.logicalChunkId, diskIndexSamplesMap);
            }
        }
        return chunkGroups;
    }

    public getChunk(logicalChunkId): Chunk {
        if (this.chunkCache.has(logicalChunkId)) {
            return this.chunkCache.get(logicalChunkId);
        }
        else {
            if (this.chunkCache.size > this.config.maxDBOpen) {
                const bulkEviction = Math.min(10, this.chunkCache.size, this.config.maxDBOpen);
                let evicted = 0;
                for (const [id, chunk] of this.chunkCache.entries()) {
                    if (evicted >= bulkEviction) {
                        break;
                    }
                    chunk[Symbol.dispose]();
                    this.chunkCache.delete(id);
                    evicted++;
                }
            }
            const chunk = new Chunk(this.config, logicalChunkId);
            this.chunkCache.set(logicalChunkId, chunk);
            return chunk;
        }
    }

    [Symbol.dispose]() {
        this.chunkCache.forEach(chunk => chunk[Symbol.dispose]());
    }
}