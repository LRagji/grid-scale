
import { InjectableConstructor } from "node-apparatus";
import { IChunk } from "../../chunk/i-chunk.js";
import { gridKWayMerge } from "../../merge/grid-row-merge.js";
import { ChunkFactoryBase } from "../../chunk/chunk-factory-base.js";
import { ShardAccessMode } from "../../types/shard-access-mode.js";
import ChunkSqlite from "./chunk-implementation/chunk-sqlite.js";
import { ChunkGenerator } from "./chunk-implementation/chunk-mock.js";
import { CachedChunkFactory } from "./cached-chunk-factory.js";

export class TTLChunkFactory<T extends IChunk> extends CachedChunkFactory<T> {


    constructor(chunkType: new (...args: any[]) => T,
        mergeFunction: <T>(cursors: IterableIterator<T>[]) => IterableIterator<T> = gridKWayMerge(ChunkFactoryBase.tagColumnIndex, ChunkFactoryBase.timeColumnIndex, ChunkFactoryBase.insertTimeColumnIndex),
        injectableConstructor: InjectableConstructor = new InjectableConstructor(),
        cacheLimit: number,
        bulkDropLimit: number | undefined = undefined,
        private readonly timeToLive: number) {
        super(chunkType, mergeFunction, injectableConstructor, cacheLimit, bulkDropLimit);
    }

    public override getChunk(connectionPath: string, mode: ShardAccessMode, callerSignature: string): T | null {
        const currentTime = Date.now();
        const chunk = super.getChunk(connectionPath, mode, callerSignature);
        if (mode === "write") {
            //Registered a new birth certificate for the chunk
            chunk.metadataSet("birth", currentTime.toString());
        }
        else {
            //Check if the chunk is still alive
            //If not, dispose it
            const anyAlive = chunk.metadataGet("birth")
                .filter(birth => (currentTime - parseInt(birth)) < this.timeToLive);
            if (anyAlive.length === 0) {
                super.pruneChunk(connectionPath, mode, callerSignature);
                return null;
            }
        }
        return chunk;
    }
}

export default new TTLChunkFactory<ChunkSqlite>(ChunkSqlite, undefined, undefined, 1000, undefined, Number.MAX_SAFE_INTEGER);
//export default new TTLChunkFactory<ChunkGenerator>(ChunkGenerator, undefined, undefined, 1000, undefined, 86400000);