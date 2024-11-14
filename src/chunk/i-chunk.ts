export interface IChunk {

    metadataSet(key: string, value: string): void
    metadataGet(key: string): string[]

    bulkSet(records: Map<string, any[][]>): void
    bulkIterator(tags: string[], startTimeInclusive: number, endTimeExclusive: number): IterableIterator<any[]>
    canBeDisposed(): boolean
    [Symbol.asyncDispose](): Promise<void>

}