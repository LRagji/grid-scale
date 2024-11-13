import { InjectableConstructor } from "node-apparatus";
import { ChunkFactoryBase } from "../../chunk/chunk-factory-base.js";
import { IChunk } from "../../chunk/i-chunk.js";
import { ShardAccessMode } from "../../types/shard-access-mode.js";
import { gridKWayMerge } from "../../merge/grid-row-merge.js";
import ChunkSqlite from "../chunk-implementation/chunk-sqlite.js";
import { ChunkGenerator } from "../chunk-implementation/chunk-mock.js";


export class SqliteChunkFactory<T extends IChunk> extends ChunkFactoryBase<T> {

    public static readonly tagColumnIndex = 4;
    public static readonly timeColumnIndex = 0;
    public static readonly insertTimeColumnIndex = 1;
    public static readonly columnCount = 5;

    private readonly chunkCache = new Map<string, T>();

    constructor(
        private readonly chunkType: new (...args: any[]) => T,
        private readonly cacheLimit: number,
        private readonly bulkDropLimit: number = Math.ceil(cacheLimit / 4),
        private readonly mergeFunction: <T>(cursors: IterableIterator<T>[]) => IterableIterator<T> = gridKWayMerge(ChunkFactoryBase.tagColumnIndex, ChunkFactoryBase.timeColumnIndex, ChunkFactoryBase.insertTimeColumnIndex),
        private readonly injectableConstructor: InjectableConstructor = new InjectableConstructor()) {
        super();
    }

    public override getChunk(connectionPath: string, mode: ShardAccessMode, callerSignature: string): T {
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
            const chunk = this.injectableConstructor.createInstance<T>(this.chunkType, [connectionPath, mode, this.mergeFunction, callerSignature, this.injectableConstructor]);
            this.chunkCache.set(cacheKey, chunk);
            return chunk;
        }
    }

    public override async [Symbol.asyncDispose]() {
        const handles = Array.from(this.chunkCache.values())
            .map(chunk => chunk[Symbol.asyncDispose]());
        await Promise.allSettled(handles);
        this.chunkCache.clear();
    }
}

//export default new SqliteChunkFactory<ChunkGenerator>(ChunkGenerator, 1000);
export default new SqliteChunkFactory<ChunkSqlite>(ChunkSqlite, 1000);