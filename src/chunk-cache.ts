import { InjectableConstructor } from "node-apparatus";
import { ChunkBase } from "./chunk/chunk-base.js";
import { ShardAccessMode } from "./types/shard-access-mode.js";

export class ChunkCache<T extends ChunkBase> {
    private readonly chunkCache = new Map<string, T>();

    constructor(
        private readonly chunkType: new (...args: any[]) => T,
        private readonly cacheLimit: number,
        private readonly bulkDropLimit: number,
        private readonly mergeFunction: <T>(cursors: IterableIterator<T>[]) => IterableIterator<T>,
        private readonly searchRegex: RegExp,
        private readonly injectableConstructor: InjectableConstructor = new InjectableConstructor()) { }

    public getChunk(connectionPath: string, mode: ShardAccessMode, callerSignature: string): T {
        const cacheKey = connectionPath + mode;
        if (this.chunkCache.has(cacheKey)) {
            return this.chunkCache.get(cacheKey);
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
            const chunk = this.injectableConstructor.createInstance<T>(this.chunkType, [connectionPath, mode, this.mergeFunction, callerSignature, this.searchRegex, this.injectableConstructor]);
            this.chunkCache.set(cacheKey, chunk);
            return chunk;
        }
    }

    public async [Symbol.asyncDispose]() {
        const handles = Array.from(this.chunkCache.values())
            .map(chunk => chunk[Symbol.asyncDispose]());
        await Promise.allSettled(handles);
        this.chunkCache.clear();
    }
}