import { BootstrapConstructor } from "express-service-bootstrap";
import { ChunkCache } from "../chunk-cache.js";
import { IChunk } from "../chunk/i-chunk.js";
import { gridKWayMerge } from "../merge/grid-row-merge.js";
import { ChunkSqlite } from "../chunk/chunk-sqlite.js";

export class GridThreadPlugin {

    private readonly mergeFunction: <T>(cursors: IterableIterator<T>[]) => IterableIterator<T>;
    private readonly iteratorCache = new Map<string, IterableIterator<any>>();
    private readonly chunkCache: ChunkCache<IChunk>;
    private readonly writeFileName: string;

    constructor(
        selfIdentity: string,
        preName: string, postName: string, cacheSize: number,
        mergeRowTagIndex: number, mergeRowTimeIndex: number,
        private readonly injectableConstructor: BootstrapConstructor = new BootstrapConstructor()) {
        this.writeFileName = preName + selfIdentity + postName;
        this.mergeFunction = gridKWayMerge(mergeRowTagIndex, mergeRowTimeIndex);
        const searchRegExp = new RegExp("^" + preName + "[a-z0-9-]+\\" + postName + "$");//`^ts[a-z0-9]+\\.db$`;
        this.chunkCache = new ChunkCache<ChunkSqlite>(ChunkSqlite, cacheSize, Math.ceil(cacheSize / 4), this.mergeFunction, searchRegExp, this.injectableConstructor);
    }

    public write(connectionPath: string, tagRecords: Map<string, any[]>): void {
        const chunk = this.chunkCache.getChunk(connectionPath, "write", this.writeFileName);
        chunk.bulkSet(tagRecords);
    }

    public iterate(queryId: string, connectionPaths: Set<string>, tagSet: Set<string>, startInclusive: number, endExclusive: number, pageSize: number): any[][] {

        if (this.iteratorCache.has(queryId) === false) {
            const chunkIterators = new Array<IterableIterator<any>>();
            for (const connectionPath of connectionPaths) {
                const chunk = this.chunkCache.getChunk(connectionPath, "read", this.writeFileName);
                chunkIterators.push(chunk.bulkIterator(Array.from(tagSet.values()), startInclusive, endExclusive));
            }
            this.iteratorCache.set(queryId, this.mergeFunction(chunkIterators));
        }

        const iterator = this.iteratorCache.get(queryId);
        const page = new Array<any[]>();
        let iteratorResult = iterator.next();
        while (page.length < (pageSize - 1) && iteratorResult.done === false) {
            page.push(iteratorResult.value);
            iteratorResult = iterator.next();
        }
        if (iteratorResult.done === true) {
            this.iteratorCache.delete(queryId);
        }
        else {
            page.push(iteratorResult.value);
        }

        return page;
    }

    public clearIteration(queryId: string): void {
        if (this.iteratorCache.has(queryId)) {
            this.iteratorCache.get(queryId).return();
            this.iteratorCache.delete(queryId);
        }
    }

    public async [Symbol.asyncDispose]() {
        for (const iterator of this.iteratorCache.values()) {
            iterator.return();
        }
        this.iteratorCache.clear();
        await this.chunkCache[Symbol.asyncDispose]();
    }

}