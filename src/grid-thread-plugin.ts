import { InjectableConstructor, StatefulRecipient } from "node-apparatus";
import { ChunkFactoryBase } from "./chunk/chunk-factory-base.js";
import { IChunk } from "./chunk/i-chunk.js";
import { gridKWayMerge } from "./merge/grid-row-merge.js";
import { isMainThread, parentPort, MessagePort } from "node:worker_threads";
import crypto from "node:crypto";
import { INonVolatileHashMap } from "./non-volatile-hash-map/i-non-volatile-hash-map.js";
import { RedisHashMap } from "./non-volatile-hash-map/redis-hash-map.js";

export class GridThreadPlugin extends StatefulRecipient {

    private mergeFunction: <T>(cursors: IterableIterator<T>[]) => IterableIterator<T>;
    private readonly iteratorCache = new Map<string, [IterableIterator<any>, number, number, string, boolean]>();
    private chunkFactory: ChunkFactoryBase<IChunk>;
    private callerSignature: string;
    private readonly lambdaCache: INonVolatileHashMap = new RedisHashMap("redis://localhost:6379", "lambda-cache-");
    private readonly cacheInProgressSuffix = "-in-progress";
    private readonly metaKeyLastWrite = "lastWrite";

    public constructor(
        shouldActivateMessagePort: boolean = !isMainThread,
        messagePort: MessagePort = parentPort,
        private readonly injectableConstructor: InjectableConstructor = new InjectableConstructor()) {
        super(shouldActivateMessagePort, messagePort);
    }

    public async initialize(callerSignature: string, chunkPluginFactoryPath: string): Promise<void> {
        this.callerSignature = callerSignature;
        const chunkFactoryInstance = (await import(chunkPluginFactoryPath)).default;
        const chunkFactoryType: typeof ChunkFactoryBase<IChunk> = chunkFactoryInstance.constructor;
        this.chunkFactory = chunkFactoryInstance;
        this.mergeFunction = gridKWayMerge(chunkFactoryType.tagColumnIndex, chunkFactoryType.timeColumnIndex, chunkFactoryType.insertTimeColumnIndex);
        await (this.lambdaCache as RedisHashMap).initialize();
    }

    public bulkWrite(plan: [string, Map<string, any[]>][]): void {
        for (const [connectionPath, tagRecords] of plan) {
            const chunk = this.chunkFactory.getChunk(connectionPath, "write", this.callerSignature);
            if (chunk !== null) {
                chunk.bulkSet(tagRecords);
                chunk.metadataSet(this.metaKeyLastWrite, Date.now().toString());
            }
        }
    }

    public async bulkIterate(queryId: string, plans: [Set<string>, Set<string>, [number, number]][], pageSize: number, lambdaFunctionPath: string | null): Promise<any[][]> {
        let lambdaFunction: (page: any[][]) => any[][] = (page: any[][]) => page;
        if (lambdaFunctionPath != undefined) {
            lambdaFunction = (await import(lambdaFunctionPath)).default as (page: any[][]) => any[][];
        }
        let currentPlanIndex = 0
        let page = new Array<any[]>();
        do {
            if (this.iteratorCache.has(queryId) === false) {
                const connectionPaths = Array.from(plans[currentPlanIndex][0]).sort();
                const tagSet = plans[currentPlanIndex][1];
                const timeRange = plans[currentPlanIndex][2];
                let uniqueKey = `-${lambdaFunctionPath ?? "passthrough"}-${Array.from(tagSet).sort().join(",")}-${timeRange[0]}-${timeRange[1]}-${pageSize}`;
                const chunkLastWrite = new Map<string, number>();
                const openChunks = new Array<IChunk>();

                for (const connectionPath of connectionPaths) {
                    const chunk = this.chunkFactory.getChunk(connectionPath, "read", this.callerSignature);
                    if (chunk !== null) {
                        uniqueKey = connectionPath + "," + uniqueKey
                        const lastMaxWrite = (chunk.metadataGet(this.metaKeyLastWrite, "0")).reduce((acc: number, val: string | null) => Math.max(acc, parseInt(val, 10)), 0);
                        if (!Number.isNaN(lastMaxWrite)) {
                            chunkLastWrite.set(connectionPath, lastMaxWrite);
                            openChunks.push(chunk);
                        }
                    }
                }
                const hashKey = `plan-${crypto.createHash("md5").update(uniqueKey).digest("hex")}`;
                let returnFromCache = chunkLastWrite.size > 0;
                if (returnFromCache === true) {
                    const cachedKVP = await this.lambdaCache.getFieldValues(hashKey, Array.from(chunkLastWrite.keys()));
                    returnFromCache = returnFromCache && cachedKVP.size > 0;
                    for (const [path, lastWriteTime] of chunkLastWrite) {
                        returnFromCache = returnFromCache && cachedKVP.has(path) && parseInt(cachedKVP.get(path) ?? "0", 10) >= lastWriteTime;
                        if (returnFromCache === false) {
                            break;
                        }
                    }
                }
                else {
                    this.iteratorCache.delete(queryId);
                    currentPlanIndex++;
                    continue;
                }
                if (returnFromCache === true) {
                    this.iteratorCache.set(queryId, [null, currentPlanIndex, 0, hashKey, true]);
                }
                else {
                    await this.lambdaCache.del([hashKey]);
                    const chunkLastWriteArray = Array.from(chunkLastWrite.entries()).reduce((acc, [path, lastWriteTime]) => acc.concat([path, lastWriteTime.toString()]), new Array<string>());
                    await this.lambdaCache.set(hashKey + this.cacheInProgressSuffix, chunkLastWriteArray);
                    const iterators = openChunks.map(chunk => chunk.bulkIterator(Array.from(tagSet.values()), timeRange[0], timeRange[1]));
                    this.iteratorCache.set(queryId, [this.mergeFunction(iterators), currentPlanIndex, 0, hashKey, false]);
                }
            }

            let [iterator, planIndex, pageNumber, hashKey, returnFromCache] = this.iteratorCache.get(queryId);
            currentPlanIndex = planIndex
            if (returnFromCache === true) {
                const pageKey = `p-${pageNumber}`;
                const cachedPaged = await this.lambdaCache.getFieldValues(hashKey, [pageKey]);
                page = cachedPaged.size > 0 ? JSON.parse(cachedPaged.get(pageKey) ?? "[]") : new Array<any[]>();
                if (page.length !== 0) {
                    pageNumber++;
                    this.iteratorCache.set(queryId, [iterator, currentPlanIndex, pageNumber, hashKey, returnFromCache]);
                    break;
                }
                this.iteratorCache.delete(queryId);
                currentPlanIndex++;
            }
            else {
                let iteratorResult = iterator.next();
                while (page.length < (pageSize - 1) && iteratorResult.done === false) {
                    page.push(iteratorResult.value);
                    iteratorResult = iterator.next();
                }

                if (iteratorResult.done === false) {
                    page.push(iteratorResult.value);
                }

                if (page.length !== 0) {
                    page = lambdaFunction(page) ?? new Array<any[]>();
                    if (page.length !== 0) {
                        await this.lambdaCache.set(hashKey + this.cacheInProgressSuffix, [`p-${pageNumber.toString()}`, JSON.stringify(page)]);
                        pageNumber++;
                        this.iteratorCache.set(queryId, [iterator, currentPlanIndex, pageNumber, hashKey, returnFromCache]);
                        break;
                    }
                }
                else {
                    //This is where the plan ends and we have to move to the next plan
                    await this.lambdaCache.rename(hashKey + this.cacheInProgressSuffix, hashKey);//Commit for other to read.
                    this.iteratorCache.delete(queryId);
                    currentPlanIndex++;
                }
            }
        }
        while (currentPlanIndex < plans.length);

        // if (currentPlanIndex === plans.length) {
        //     this.iteratorCache.delete(queryId);
        // }


        return page;
    }

    public clearIteration(queryId: string): void {
        if (this.iteratorCache.has(queryId)) {
            this.iteratorCache.get(queryId)[0].return();
            this.iteratorCache.delete(queryId);
        }
    }

    public async[Symbol.asyncDispose]() {
        await super[Symbol.asyncDispose]();
        for (const [iterator, planIndex] of this.iteratorCache.values()) {
            iterator.return();
        }
        this.iteratorCache.clear();
        await this.chunkFactory[Symbol.asyncDispose]();
        await this.lambdaCache[Symbol.asyncDispose]();
    }

}

export default new GridThreadPlugin(!isMainThread, parentPort, new InjectableConstructor());