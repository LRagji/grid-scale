import { InjectableConstructor, StatefulRecipient } from "node-apparatus";
import { ChunkFactoryBase } from "./types/chunk-factory-base.js";
import { IChunk } from "./types/i-chunk.js";
import { gridKWayMerge } from "./merge/grid-row-merge.js";
import { isMainThread, parentPort, MessagePort } from "node:worker_threads";
import { gzipSync } from "node:zlib";

export class GridThreadPlugin extends StatefulRecipient {

    private mergeFunction: <T>(cursors: IterableIterator<T>[]) => IterableIterator<T>;
    private readonly iteratorCache = new Map<string, IterableIterator<any>>();
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

    public async bulkIterate(queryId: string, tagSet: Set<string>, connectionPaths: Set<string>, startInclusive: number, endExclusive: number, pageSize: number, lambdaFunctionPath: string, zippedResults: boolean): Promise<any[][]> {
        let lambdaFunction: (page: any[][]) => any[][] = (page: any[][]) => page;
        if (lambdaFunctionPath != "") {
            lambdaFunction = (await import(lambdaFunctionPath)).default as (page: any[][]) => any[][];
        }

        let page = new Array<any[]>();

        if (this.iteratorCache.has(queryId) === false) {
            const iterators = new Array<IterableIterator<any>>();
            for (const connectionPath of connectionPaths) {
                const chunk = this.chunkFactory.getChunk(connectionPath, "read", this.callerSignature);
                if (chunk !== null) {
                    iterators.push(chunk.bulkIterator(Array.from(tagSet.values()), startInclusive, endExclusive));
                }
            }

            if (iterators.length === 0) {
                return this.readyReturnPage(page, zippedResults);
            }
            else if (iterators.length === 1) {
                this.iteratorCache.set(queryId, iterators[0]);
            }
            else {
                this.iteratorCache.set(queryId, this.mergeFunction(iterators));
            }
        }

        let iterator = this.iteratorCache.get(queryId);
        let iteratorResult;
        do {
            iteratorResult = iterator.next();
            while (page.length < (pageSize - 1) && iteratorResult.done === false) {
                page.push(iteratorResult.value);
                iteratorResult = iterator.next();
            }

            if (iteratorResult.done === false) {
                page.push(iteratorResult.value);
            }

            if (page.length !== 0) {
                page = lambdaFunction(page) ?? new Array<any[]>();
            }
        }
        while (page.length === 0 && iteratorResult.done === false)

        if (iteratorResult.done === true && page.length === 0) {
            this.clearIteration(queryId);
        }

        return this.readyReturnPage(page, zippedResults);
    }

    private readyReturnPage(page: any[][], zippedResults: boolean): any[][] {
        if (zippedResults === true && page.length !== 0) {
            const compressedPage = Array.from(gzipSync(JSON.stringify(page)));
            return compressedPage as unknown as any[][];
        }
        else {
            return page;
        }
    }

    public clearIteration(queryId: string): void {
        if (this.iteratorCache.has(queryId)) {
            this.iteratorCache.get(queryId)?.return();
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