export interface IChunk {
    bulkSet(records: Map<string, any[][]>): void
    bulkIterator(tags: string[], startTimeInclusive: number, endTimeExclusive: number): IterableIterator<any[]>
    canBeDisposed(): boolean
    [Symbol.asyncDispose](): Promise<void>
}