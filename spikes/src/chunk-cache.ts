import { Chunk } from "./chunk.js";
import { logicalChunkId } from "./grid-scale";
import { TConfig } from "./t-config.js";

export class ChunkCache {
    private readonly chunkCache = new Map<logicalChunkId, Chunk>();

    constructor(private readonly config: TConfig, private readonly cacheLimit: number, private readonly bulkDropLimit: number) { }

    public getChunk(logicalChunkId: logicalChunkId): Chunk {
        if (this.chunkCache.has(logicalChunkId)) {
            return this.chunkCache.get(logicalChunkId);
        }
        else {
            if (this.chunkCache.size > this.cacheLimit) {
                const bulkEviction = Math.min(this.bulkDropLimit, this.chunkCache.size, this.cacheLimit);
                let evicted = 0;
                for (const [id, chunk] of this.chunkCache.entries()) {
                    if (evicted >= bulkEviction) {
                        break;
                    }
                    if (chunk.canBeDisposed() === false) {
                        continue;
                    }
                    chunk[Symbol.dispose]();
                    this.chunkCache.delete(id);
                    evicted++;
                }
                if (evicted === 0) {
                    throw new Error(`Chunk cache is full & no chunks can be evicted this time, please retry later.`);
                }
            }
            const chunk = new Chunk(this.config, logicalChunkId);
            this.chunkCache.set(logicalChunkId, chunk);
            return chunk;
        }
    }

    public [Symbol.dispose]() {
        this.chunkCache.forEach(chunk => chunk[Symbol.dispose]());
        this.chunkCache.clear();
    }
}