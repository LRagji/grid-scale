export interface IChunk {

    bulkSet(records: Map<string, any[][]>): Promise<void>
    bulkIterator(tags: string[], startTimeInclusive: number, endTimeExclusive: number): IterableIterator<any[]>
    canBeDisposed(): boolean
    [Symbol.asyncDispose](): Promise<void>

}