export abstract class ChunkBase {
    public static readonly columnCount: number = -1;
    public static readonly timeColumnIndex: number = -1;
    public static readonly insertTimeColumnIndex: number = -1;
    public static readonly tagColumnIndex: number = -1;


    abstract bulkSet(records: Map<string, any[][]>): void
    abstract bulkIterator(tags: string[], startTimeInclusive: number, endTimeExclusive: number): IterableIterator<any[]>
    abstract canBeDisposed(): boolean
    abstract [Symbol.asyncDispose](): Promise<void>

}