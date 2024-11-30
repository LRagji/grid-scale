export interface ISmartCache {
    //Parse the results save it correctly in cache
    //In Zipped format
    iterate<T>(keys: string, tags: string[], timeRange: [number, number]): AsyncIterableIterator<T>;
    append<T>(key: string, tags: string[], time: number, value: T): Promise<void>;
}