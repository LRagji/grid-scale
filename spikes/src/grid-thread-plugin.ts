import { BootstrapConstructor } from "express-service-bootstrap";
import { ChunkCache } from "./chunk-cache.js";
import { IChunk } from "./chunk/i-chunk.js";
import { gridKWayMerge } from "./merge/grid-row-merge.js";
import { ChunkSqlite } from "./chunk/chunk-sqlite.js";
import { LongRunner } from "./multi-threads/long-runner.js";
import { isMainThread, parentPort, MessagePort } from "node:worker_threads";

export class GridThreadPlugin extends LongRunner {

    private mergeFunction: <T>(cursors: IterableIterator<T>[]) => IterableIterator<T>;
    private readonly iteratorCache = new Map<string, [IterableIterator<any>, number]>();
    private chunkCache: ChunkCache<IChunk>;
    private writeFileName: string;

    public constructor(
        shouldActivateMessagePort: boolean = !isMainThread,
        messagePort: MessagePort = parentPort,
        private readonly injectableConstructor: BootstrapConstructor = new BootstrapConstructor()) {
        super(shouldActivateMessagePort, messagePort);
    }

    public initialize(selfIdentity: string, preName: string, postName: string, cacheSize: number, mergeRowTagIndex: number, mergeRowTimeIndex: number): void {
        this.writeFileName = preName + selfIdentity + postName;
        this.mergeFunction = gridKWayMerge(mergeRowTagIndex, mergeRowTimeIndex);
        const searchRegExp = new RegExp("^" + preName + "[a-z0-9-]+\\" + postName + "$");//`^ts[a-z0-9]+\\.db$`;
        this.chunkCache = new ChunkCache<ChunkSqlite>(ChunkSqlite, cacheSize, Math.ceil(cacheSize / 4), this.mergeFunction, searchRegExp, this.injectableConstructor);
    }

    public bulkWrite(plan: [string, Map<string, any[]>][]): void {
        for (const [connectionPath, tagRecords] of plan) {
            const chunk = this.chunkCache.getChunk(connectionPath, "write", this.writeFileName);
            chunk.bulkSet(tagRecords);
        }
    }

    public bulkIterate(queryId: string, plans: [Set<string>, Set<string>][], startInclusive: number, endExclusive: number, pageSize: number): any[][] {
        let currentPlanIndex = 0
        const page = new Array<any[]>();
        do {
            if (this.iteratorCache.has(queryId) === false) {
                const chunkIterators = new Array<IterableIterator<any>>();
                const connectionPaths = plans[currentPlanIndex][0];
                const tagSet = plans[currentPlanIndex][1];
                for (const connectionPath of connectionPaths) {
                    const chunk = this.chunkCache.getChunk(connectionPath, "read", this.writeFileName);
                    chunkIterators.push(chunk.bulkIterator(Array.from(tagSet.values()), startInclusive, endExclusive));
                }
                this.iteratorCache.set(queryId, [this.mergeFunction(chunkIterators), currentPlanIndex]);
            }

            const [iterator, planIndex] = this.iteratorCache.get(queryId);
            currentPlanIndex = planIndex
            let iteratorResult = iterator.next();
            while (page.length < (pageSize - 1) && iteratorResult.done === false) {
                page.push(iteratorResult.value);
                iteratorResult = iterator.next();
            }
            if (iteratorResult.done === false) {
                page.push(iteratorResult.value);
            }

            if (page.length !== 0) {
                break;
            }

            this.iteratorCache.delete(queryId);
            currentPlanIndex++;
        }
        while (currentPlanIndex < plans.length);

        if (currentPlanIndex === plans.length) {
            this.iteratorCache.delete(queryId);
        }
        return page;
    }

    public clearIteration(queryId: string): void {
        if (this.iteratorCache.has(queryId)) {
            this.iteratorCache.get(queryId)[0].return();
            this.iteratorCache.delete(queryId);
        }
    }

    public override async [Symbol.asyncDispose]() {
        await super[Symbol.asyncDispose]();
        for (const [iterator, planIndex] of this.iteratorCache.values()) {
            iterator.return();
        }
        this.iteratorCache.clear();
        await this.chunkCache[Symbol.asyncDispose]();
    }

}