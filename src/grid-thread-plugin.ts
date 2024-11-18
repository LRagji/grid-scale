import { InjectableConstructor, StatefulRecipient } from "node-apparatus";
import { ChunkFactoryBase } from "./chunk/chunk-factory-base.js";
import { IChunk } from "./chunk/i-chunk.js";
import { gridKWayMerge } from "./merge/grid-row-merge.js";
import { isMainThread, parentPort, MessagePort } from "node:worker_threads";

export class GridThreadPlugin extends StatefulRecipient {

    private mergeFunction: <T>(cursors: IterableIterator<T>[]) => IterableIterator<T>;
    private readonly iteratorCache = new Map<string, [IterableIterator<any>, number]>();
    private chunkFactory: ChunkFactoryBase<IChunk>;
    private callerSignature: string;

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
    }

    public bulkWrite(plan: [string, Map<string, any[]>][]): void {
        for (const [connectionPath, tagRecords] of plan) {
            const chunk = this.chunkFactory.getChunk(connectionPath, "write", this.callerSignature);
            if (chunk !== null) {
                chunk.bulkSet(tagRecords);
            }
        }
    }

    public async bulkIterate(queryId: string, plans: [Set<string>, Set<string>, [number, number]][], pageSize: number, lambdaFunctionPath: string | null): Promise<any[][]> {
        let lambdaFunction: (page: any[][]) => any[][] = (page: any[][]) => page;
        if (lambdaFunctionPath != undefined) {
            lambdaFunction = (await import(lambdaFunctionPath)).default as (page: any[][]) => any[][];
        }
        let currentPlanIndex = 0
        const page = new Array<any[]>();
        do {
            const timeRange = plans[currentPlanIndex][2];
            const startInclusive = timeRange[0];
            const endExclusive = timeRange[1];
            if (this.iteratorCache.has(queryId) === false) {
                const chunkIterators = new Array<IterableIterator<any>>();
                const connectionPaths = plans[currentPlanIndex][0];
                const tagSet = plans[currentPlanIndex][1];
                for (const connectionPath of connectionPaths) {
                    const chunk = this.chunkFactory.getChunk(connectionPath, "read", this.callerSignature);
                    if (chunk !== null) {
                        chunkIterators.push(chunk.bulkIterator(Array.from(tagSet.values()), startInclusive, endExclusive));
                    }
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
        const computedOutput = lambdaFunction(page);
        return computedOutput;
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
    }

}

export default new GridThreadPlugin(!isMainThread, parentPort, new InjectableConstructor());