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

    public allocateChunksWithFormattedSamples<rowType>(sampleSets: Map<tagName, samples>, insertTime: number, formatCallback: (dataToFormat: samples, insertTime: number) => rowType[]): { chunkAllocations: Map<logicalChunkId, Map<diskIndex, Map<tagName, rowType[]>>>, chunkDisplacements: Map<logicalChunkId, Set<logicalChunkId>> } {
        const chunkGroups = new Map<logicalChunkId, Map<diskIndex, Map<tagName, rowType[]>>>(); //ChunkID->DiskIndex->TagName->Samples[]
        const chunkDisplacements = new Map<logicalChunkId, Set<logicalChunkId>>(); //ChunkID->DisplacedChunkIDs
        for (const [tagName, samples] of sampleSets) {
            for (let timeIndex = 0; timeIndex < samples.length; timeIndex += 2) {
                const chunkIdByInsertTime = ChunkId.from(tagName, insertTime, this.config);
                //Chunk Allocations
                const diskIndexSamplesMap = chunkGroups.get(chunkIdByInsertTime.logicalChunkId) || new Map<diskIndex, Map<tagName, rowType[]>>();
                const diskIndex = chunkIdByInsertTime.tagNameMod(this.config.setPaths.get(this.config.activePath).length);
                const sampleSets = diskIndexSamplesMap.get(diskIndex) || new Map<tagName, rowType[]>();
                const formattedSamples = formatCallback(samples, insertTime);
                sampleSets.set(tagName, formattedSamples);
                diskIndexSamplesMap.set(diskIndex, sampleSets);
                chunkGroups.set(chunkIdByInsertTime.logicalChunkId, diskIndexSamplesMap);
                //Chunk Misplacement's
                const chunkIdBySampleTime = ChunkId.from(tagName, samples[timeIndex], this.config);
                const toleranceWidth = this.config.timeBucketWidth * this.config.timeBucketTolerance;
                const minimumTolerance = chunkIdByInsertTime.timeBucketed[0] - toleranceWidth;
                const maximumTolerance = chunkIdByInsertTime.timeBucketed[0] + toleranceWidth;
                if (chunkIdBySampleTime.timeBucketed[0] < minimumTolerance || chunkIdBySampleTime.timeBucketed[0] > maximumTolerance) {
                    const displacements = chunkDisplacements.get(chunkIdBySampleTime.logicalChunkId) || new Set<logicalChunkId>();
                    displacements.add(chunkIdByInsertTime.logicalChunkId);
                    chunkDisplacements.set(chunkIdBySampleTime.logicalChunkId, displacements);
                }
            }
        }
        return { chunkAllocations: chunkGroups, chunkDisplacements };
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