export interface IChunk {

    bulkSet(records: Map<string, any[][]>): Promise<void>
    bulkIterator(tags: string[], startTimeInclusive: number, endTimeExclusive: number): AsyncIterableIterator<any[]>
    canBeDisposed(): boolean
    [Symbol.asyncDispose](): Promise<void>

}