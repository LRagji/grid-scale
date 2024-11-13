import { IChunk } from "./i-chunk.js";
import { ShardAccessMode } from "../types/shard-access-mode.js";

export abstract class ChunkFactoryBase<T extends IChunk> {

    public static readonly tagColumnIndex: number = -1;
    public static readonly timeColumnIndex: number = -1;
    public static readonly insertTimeColumnIndex: number = -1;
    public static readonly columnCount: number = -1;

    public abstract getChunk(connectionPath: string, mode: ShardAccessMode, callerSignature: string): T

    public abstract [Symbol.asyncDispose](): Promise<void>
}