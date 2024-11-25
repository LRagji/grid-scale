import { InjectableConstructor } from "node-apparatus";
import { IChunk } from "../../types/i-chunk.js";
import { SqliteChunkFactory } from "./sqlite-chunk-factory.js";
import { gridKWayMerge } from "../../merge/grid-row-merge.js";
import { ChunkFactoryBase } from "../../types/chunk-factory-base.js";
import { ShardAccessMode } from "../../types/shard-access-mode.js";
import ChunkSqlite from "./chunk-implementation/chunk-sqlite.js";
import { ChunkGenerator } from "./chunk-implementation/chunk-mock.js";

export class CachedChunkFactory<T extends IChunk> extends SqliteChunkFactory<T> {

    private readonly chunkCache = new Map<string, T>();

    constructor(chunkType: new (...args: any[]) => T,
        mergeFunction: <T>(cursors: IterableIterator<T>[]) => IterableIterator<T> = gridKWayMerge(ChunkFactoryBase.tagColumnIndex, ChunkFactoryBase.timeColumnIndex, ChunkFactoryBase.insertTimeColumnIndex),
        injectableConstructor: InjectableConstructor = new InjectableConstructor(),
        private readonly cacheLimit: number,
        private readonly bulkDropLimit: number = Math.ceil(cacheLimit / 4)) {
        super(chunkType, mergeFunction, injectableConstructor);
    }

    public override getChunk(connectionPath: string, mode: ShardAccessMode, callerSignature: string): T | null {
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
            const chunk = super.getChunk(connectionPath, mode, callerSignature);
            this.chunkCache.set(cacheKey, chunk);
            return chunk;
        }
    }

    public pruneChunk(connectionPath: string, mode: ShardAccessMode, callerSignature: string): void {
        const cacheKey = connectionPath + mode;
        if (this.chunkCache.has(cacheKey)) {
            if (this.chunkCache.get(cacheKey).canBeDisposed() === true) {
                this.chunkCache.get(cacheKey)[Symbol.asyncDispose]();
                this.chunkCache.delete(cacheKey);
            }
        }
    }

    public override async [Symbol.asyncDispose]() {
        const handles = Array.from(this.chunkCache.values())
            .map(chunk => chunk[Symbol.asyncDispose]());
        await Promise.allSettled(handles);
        this.chunkCache.clear();
        super[Symbol.asyncDispose]();
    }
}

export default new CachedChunkFactory<ChunkSqlite>(ChunkSqlite, undefined, undefined, 1000);
//export default new CachedChunkFactory<ChunkGenerator>(ChunkGenerator, undefined, undefined, 1000);