import { InjectableConstructor } from "node-apparatus";
import { ChunkFactoryBase } from "../../types/chunk-factory-base.js";
import { IChunk } from "../../types/i-chunk.js";
import { ShardAccessMode } from "../../types/shard-access-mode.js";
import { gridKWayMerge } from "./grid-row-merge.js";

export class SqliteChunkFactory<T extends IChunk> extends ChunkFactoryBase<T> {

    public static readonly tagColumnIndex = 4;
    public static readonly timeColumnIndex = 0;
    public static readonly insertTimeColumnIndex = 1;
    public static readonly columnCount = 5;

    constructor(
        private readonly chunkType: new (...args: any[]) => T,
        private readonly mergeFunction: <T>(cursors: IterableIterator<T>[]) => IterableIterator<T> = gridKWayMerge(ChunkFactoryBase.tagColumnIndex, ChunkFactoryBase.timeColumnIndex, ChunkFactoryBase.insertTimeColumnIndex),
        private readonly injectableConstructor: InjectableConstructor = new InjectableConstructor()) {
        super();
    }

    public override getChunk(connectionPath: string, mode: ShardAccessMode, callerSignature: string): T | null {
        return this.injectableConstructor.createInstance<T>(this.chunkType, [connectionPath, mode, this.mergeFunction, callerSignature, this.injectableConstructor]);
    }

    public override async [Symbol.asyncDispose]() {
    }
}