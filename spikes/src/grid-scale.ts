import { ChunkId } from "./chunk-id.js";
import { TConfig } from "./t-config.js";
import { ChunkLinker } from "./chunk-linker.js";
import { cpus } from "node:os";
import { Worker } from "node:worker_threads";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { BackgroundPlugin, BackgroundPluginInitializeParam } from "./background-plugin.js";
import { deserialize, HeaderPayload, IThreadCommunication, serialize } from "./threads/i-thread-communication.js";
import { randomInt } from "node:crypto";

export type logicalChunkId = string;
export type diskIndex = number;
export type tagName = string;
export type samples = number[];
export type setDiskPath = string;
export type upsertPlan<rowType> = { chunkAllocations: Map<logicalChunkId, Map<diskIndex, Map<tagName, rowType[]>>>, chunkDisplacements: Map<logicalChunkId, { insertTimeBucketed: number, related: Set<logicalChunkId> }> };
export type queryPlan = { plan: Map<logicalChunkId, Map<tagName, Map<setDiskPath, diskIndex>>>, groupedTags: Map<logicalChunkId, Set<tagName>> };
export type distributedQueryPlan = { planId: string, plan: queryPlan, startInclusiveTime: number, endExclusiveTime: number, pageSize: number };

export class GridScaleBase<rowType extends Array<any>> {

    protected readonly chunkLinkRegistry: ChunkLinker
    private readonly selfWorker: BackgroundPlugin;
    private readonly externalWorkers = new Array<Worker>();
    private readonly writerPartIdentifier = `${process.pid}`;

    constructor(private readonly config: TConfig, workerCount: number = 0) {
        this.chunkLinkRegistry = new ChunkLinker(config);
        const totalWorkers = Math.min(cpus().length, Math.max(0, workerCount));
        const workerFilePath = join(dirname(fileURLToPath(import.meta.url)), './threads/long-running-thread.js');
        const pluginParameter = { config, cacheLimit: config.maxDBOpen, bulkDropLimit: 10 } as BackgroundPluginInitializeParam;
        for (let index = 0; index < totalWorkers; index++) {
            this.externalWorkers.push(new Worker(workerFilePath, { workerData: [pluginParameter] }));
        }
        if (this.externalWorkers.length === 0) {
            this.selfWorker = new BackgroundPlugin();
            this.selfWorker.initialize(this.writerPartIdentifier, pluginParameter);
        }
    }

    public async store<rawRowType extends Array<any>>(rawData: Map<tagName, rawRowType>, rowTransformer: (dataToFormat: rawRowType, insertTime: number) => rowType[], insertTime = Date.now(), diagnostics = new Map<string, any>()): Promise<void> {
        let diagnosticTime = Date.now();
        const upsertPlan = this.upsertPlan(rawData, insertTime, rowTransformer);
        diagnostics.set("Query Plan", Date.now() - diagnosticTime);

        diagnosticTime = Date.now();
        if (this.externalWorkers.length === 0) {
            const pluginPayload = {
                header: BackgroundPlugin.invokeHeader,
                subHeader: BackgroundPlugin.invokeSubHeaders.Store,
                payload: upsertPlan
            } as IThreadCommunication<upsertPlan<rowType>>;
            this.selfWorker.work(pluginPayload);
        }
        else {
            const chunkSize = Math.ceil(upsertPlan.chunkAllocations.size / this.externalWorkers.length);
            const workLoad = Array.from(upsertPlan.chunkAllocations.entries());
            const workerHandles = new Array<Promise<string>>();
            for (let workerIndex = 0; workerIndex < this.externalWorkers.length; workerIndex++) {
                workerHandles.push(new Promise<string>((resolve, reject) => {
                    const worker = this.externalWorkers[workerIndex];
                    const pluginPayload = {
                        header: BackgroundPlugin.invokeHeader,
                        subHeader: BackgroundPlugin.invokeSubHeaders.Store,
                        payload: { chunkAllocations: new Map(workLoad.splice(0, chunkSize)), chunkDisplacements: new Map() }
                    } as IThreadCommunication<upsertPlan<rowType>>;

                    const workerErrorHandler = (error: Error) => {
                        worker.off('message', workerMessageHandler);
                        reject(error);
                    };

                    const workerMessageHandler = (message: any) => {
                        worker.off('error', workerErrorHandler);
                        const comMessage = deserialize<string>(message);
                        if (comMessage.header === HeaderPayload.Error) {
                            reject(new Error(comMessage.subHeader + comMessage.payload));
                        }
                        if (comMessage.header === HeaderPayload.Response) {
                            resolve(comMessage.payload);
                        }
                    };

                    worker.once('error', workerErrorHandler);
                    worker.once('message', workerMessageHandler);
                    worker.postMessage(serialize(pluginPayload));
                }));
            }
            await Promise.all(workerHandles);
        }
        diagnostics.set("Disk Write", Date.now() - diagnosticTime);

        diagnosticTime = Date.now();
        let displacedChunks = 0;
        for (const [indexedLogicalChunkId, logicalChunkIds] of upsertPlan.chunkDisplacements) {
            await this.chunkLinkRegistry.link(indexedLogicalChunkId, Array.from(logicalChunkIds.related.values()), logicalChunkIds.insertTimeBucketed, `${this.writerPartIdentifier}-${insertTime}`);
            displacedChunks += logicalChunkIds.related.size;
        }
        diagnostics.set("Linking Chunks", Date.now() - diagnosticTime);
        diagnostics.set("Total Chunks", upsertPlan.chunkAllocations.size);
        diagnostics.set("Displaced Chunks", displacedChunks);
    }

    private upsertPlan<rawRowType extends Array<any>>(sampleSets: Map<tagName, rawRowType>, insertTime: number, formatCallback: (dataToFormat: samples, insertTime: number) => rowType[]): upsertPlan<rowType> {
        const chunkGroups = new Map<logicalChunkId, Map<diskIndex, Map<tagName, rowType[]>>>(); //ChunkID->DiskIndex->TagName->Samples[]
        const chunkDisplacements = new Map<logicalChunkId, { insertTimeBucketed: number, related: Set<logicalChunkId> }>(); //ChunkID->DisplacedChunkIDs
        for (const [tagName, samples] of sampleSets) {
            for (let timeIndex = 0; timeIndex < samples.length; timeIndex += 2) {
                const chunkIdByInsertTime = ChunkId.from(tagName, insertTime, this.config);
                //Chunk Allocations
                const diskIndexSamplesMap = chunkGroups.get(chunkIdByInsertTime.logicalChunkId) || new Map<diskIndex, Map<tagName, rowType[]>>();
                const diskIndex = chunkIdByInsertTime.tagNameMod(this.config.setPaths.get(this.config.activePath).length);
                const sampleSets = diskIndexSamplesMap.get(diskIndex) || new Map<tagName, rowType[]>();
                const formattedSamples = formatCallback(samples, insertTime);
                sampleSets.set(tagName, formattedSamples);
                diskIndexSamplesMap.set(diskIndex, sampleSets);
                chunkGroups.set(chunkIdByInsertTime.logicalChunkId, diskIndexSamplesMap);
                //Chunk Misplacement's
                const chunkIdBySampleTime = ChunkId.from(tagName, samples[timeIndex], this.config);
                const toleranceWidth = this.config.timeBucketWidth * this.config.timeBucketTolerance;
                const minimumTolerance = chunkIdByInsertTime.timeBucketed[0] - toleranceWidth;
                const maximumTolerance = chunkIdByInsertTime.timeBucketed[0] + toleranceWidth;
                if (chunkIdBySampleTime.timeBucketed[0] < minimumTolerance || chunkIdBySampleTime.timeBucketed[0] > maximumTolerance) {
                    const displacements = chunkDisplacements.get(chunkIdBySampleTime.logicalChunkId) || { insertTimeBucketed: chunkIdByInsertTime.timeBucketed[0], related: new Set<logicalChunkId>() };
                    displacements.related.add(chunkIdByInsertTime.logicalChunkId);
                    chunkDisplacements.set(chunkIdBySampleTime.logicalChunkId, displacements);
                }
            }
        }
        return { chunkAllocations: chunkGroups, chunkDisplacements };
    }

    public async *read(tagNames: tagName[], startInclusiveTime: number, endExclusiveTime: number, diagnostics = new Map<string, any>()): AsyncIterableIterator<AsyncIterableIterator<rowType>> {
        let startTime = startInclusiveTime;
        let queryId = `${Date.now()}-${randomInt(281474976710655)}`;
        let pageNumber = 0;
        while (startTime < endExclusiveTime) {
            let diagnosticTime = Date.now();
            const pageEndTime = Math.min(startTime + this.config.timeBucketWidth, endExclusiveTime);

            //Generate Query Plan Operation
            //[ChunkId]|[TagName]|[DiskSetPath]|[DiskIndex]
            const queryPlan = await this.readQueryPlanPageByTime(tagNames, startTime);
            diagnostics.set(`${pageNumber}-Query Plan`, Date.now() - diagnosticTime);

            diagnosticTime = Date.now();
            yield this.distributePlan<rowType>(queryPlan, startTime, pageEndTime, 10000, `${queryId}-${pageNumber}`);
            diagnostics.set(`${pageNumber}-Worker Disk Search, Open, Merge & Iterate`, Date.now() - diagnosticTime);

            pageNumber++;
            startTime = pageEndTime;
        }

    }

    private async *distributePlan<rowType>(queryPlan: queryPlan, startInclusiveTime: number, endExclusiveTime: number, pageSize: number, queryId: string): AsyncIterableIterator<rowType> {
        if (this.externalWorkers.length === 0) {
            const pluginIteratePayload = {
                header: BackgroundPlugin.invokeHeader,
                subHeader: BackgroundPlugin.invokeSubHeaders.Iterate,
                payload: { planId: queryId, plan: queryPlan, startInclusiveTime, endExclusiveTime, pageSize }
            } as IThreadCommunication<distributedQueryPlan>;
            try {
                let result = this.selfWorker.work(pluginIteratePayload) as IThreadCommunication<IteratorResult<Array<rowType>>>;
                while (result.payload.done === false) {
                    for (const row of result.payload.value) {
                        yield row;
                    }
                    result = this.selfWorker.work(pluginIteratePayload) as IThreadCommunication<IteratorResult<Array<rowType>>>;
                }
            }
            finally {
                const pluginCloseQueryPayload = {
                    header: BackgroundPlugin.invokeHeader,
                    subHeader: BackgroundPlugin.invokeSubHeaders.CloseQuery,
                    payload: queryId
                } as IThreadCommunication<string>;
                this.selfWorker.work(pluginCloseQueryPayload);
            }
        }
        else {
            const workerPayloads = new Array<distributedQueryPlan>(this.externalWorkers.length).fill(null);
            let incrementingIndex = 0;
            queryPlan.groupedTags.forEach((tags, logicalChunkId) => {
                const workerIndex = incrementingIndex % this.externalWorkers.length;
                let existingPayload = workerPayloads[workerIndex];
                if (existingPayload === null) {
                    const groupedTags = new Map<logicalChunkId, Set<tagName>>();
                    groupedTags.set(logicalChunkId, tags);
                    existingPayload = { planId: `${queryId}-${workerIndex}-${logicalChunkId}`, plan: { plan: queryPlan.plan, groupedTags }, startInclusiveTime, endExclusiveTime, pageSize };
                }
                else {
                    existingPayload.planId += `-${logicalChunkId}`;
                    existingPayload.plan.groupedTags.set(logicalChunkId, tags);
                }
                workerPayloads[workerIndex] = existingPayload;
                incrementingIndex++;
            });
            try {
                while (workerPayloads.some(payload => payload !== null)) {
                    const workerHandles = new Array<Promise<IteratorResult<Array<rowType>>>>();
                    for (let workerIndex = 0; workerIndex < this.externalWorkers.length; workerIndex++) {
                        if (workerPayloads[workerIndex] === null) {
                            workerHandles.push(Promise.resolve({ value: [], done: true }));
                            continue;
                        }
                        workerHandles.push(new Promise<IteratorResult<Array<rowType>>>((resolve, reject) => {
                            const worker = this.externalWorkers[workerIndex];
                            const pluginIteratePayload = {
                                header: BackgroundPlugin.invokeHeader,
                                subHeader: BackgroundPlugin.invokeSubHeaders.Iterate,
                                payload: workerPayloads[workerIndex]
                            } as IThreadCommunication<distributedQueryPlan>;

                            const workerErrorHandler = (error: Error) => {
                                worker.off('message', workerMessageHandler);
                                reject(error);
                            };
                            const workerMessageHandler = (message: any) => {
                                worker.off('error', workerErrorHandler);
                                const comMessage = deserialize(message);
                                if (comMessage.header === HeaderPayload.Error) {
                                    reject(new Error((comMessage as IThreadCommunication<string>).subHeader + (comMessage as IThreadCommunication<string>).payload));
                                }
                                if (comMessage.header === HeaderPayload.Response) {
                                    resolve((comMessage as IThreadCommunication<IteratorResult<Array<rowType>>>).payload);
                                }
                            };

                            worker.once('error', workerErrorHandler);
                            worker.once('message', workerMessageHandler);
                            worker.postMessage(serialize(pluginIteratePayload));
                        }));
                    }

                    for (let responseIndex = 0; responseIndex < workerHandles.length; responseIndex++) {
                        const response = await workerHandles[responseIndex];
                        for (const row of response.value) {
                            yield row;
                        }
                        if (response.done === true) {
                            workerPayloads[responseIndex] = null;
                        }
                    }
                }
            }
            finally {
                for (let workerIndex = 0; workerIndex < this.externalWorkers.length; workerIndex++) {
                    if (workerPayloads[workerIndex] !== null) {
                        const pluginCloseQueryPayload = {
                            header: BackgroundPlugin.invokeHeader,
                            subHeader: BackgroundPlugin.invokeSubHeaders.CloseQuery,
                            payload: workerPayloads[workerIndex].planId
                        } as IThreadCommunication<string>;
                        this.externalWorkers[workerIndex].postMessage(serialize(pluginCloseQueryPayload));
                    }
                }
            }
        }
    }

    private updateQueryPlan(tagName: tagName, logicalId: logicalChunkId, diskIndexFn: (size: number) => number, queryPlanByRef: Map<logicalChunkId, Map<tagName, Map<setDiskPath, diskIndex>>>) {
        const tagDiskAccess = queryPlanByRef.get(logicalId) || new Map<tagName, Map<setDiskPath, diskIndex>>();
        const diskAccess = tagDiskAccess.get(tagName) || new Map<setDiskPath, diskIndex>();
        this.config.setPaths.forEach((diskPaths, setPath) => {
            const diskIndex = diskIndexFn(diskPaths.length);
            diskAccess.set(setPath, diskIndex);
        });
        tagDiskAccess.set(tagName, diskAccess);
        queryPlanByRef.set(logicalId, tagDiskAccess);
    }

    private async readQueryPlanPageByTime(tagNames: tagName[], startInclusiveTime: number): Promise<queryPlan> {
        type chunkInfo = { id: logicalChunkId, insertTime: number };
        const startInclusiveBucketedTime = ChunkId.bucket(startInclusiveTime, this.config.timeBucketWidth);
        const tempResults = new Map<logicalChunkId, Map<tagName, Map<setDiskPath, diskIndex>>>();
        let orderedChunkIds = new Array<chunkInfo>();
        const visitedChunkIds = new Map<logicalChunkId, logicalChunkId[]>();
        const tagsGroupedByLogicalChunkId = new Map<logicalChunkId, Set<tagName>>();

        for (let tagIndex = 0; tagIndex < tagNames.length; tagIndex++) {
            const tagName = tagNames[tagIndex];
            const defaultChunkId = ChunkId.from(tagName, startInclusiveBucketedTime, this.config);
            const diskIndexFn = defaultChunkId.tagNameMod.bind(defaultChunkId);

            const existingTags = tagsGroupedByLogicalChunkId.get(defaultChunkId.logicalChunkId) || new Set<tagName>();
            existingTags.add(tagName);
            tagsGroupedByLogicalChunkId.set(defaultChunkId.logicalChunkId, existingTags);

            if (visitedChunkIds.has(defaultChunkId.logicalChunkId)) {
                for (const chunkId of visitedChunkIds.get(defaultChunkId.logicalChunkId)) {
                    this.updateQueryPlan(tagName, chunkId, diskIndexFn, tempResults);
                }
                continue;
            }
            const LHSToleranceChunkId = ChunkId.from(tagName, startInclusiveBucketedTime - (this.config.timeBucketWidth * this.config.timeBucketTolerance), this.config);
            const RHSToleranceChunkId = ChunkId.from(tagName, startInclusiveBucketedTime + (this.config.timeBucketWidth * this.config.timeBucketTolerance), this.config);
            const displacedChunk = await this.chunkLinkRegistry.getRelated(defaultChunkId.logicalChunkId);
            const allChunks = new Array<chunkInfo>();
            for (const [logicalChunkId, value] of Object.entries(displacedChunk)) {
                allChunks.push({ id: logicalChunkId, insertTime: value.bucketedTime });
            }
            allChunks.push({ id: defaultChunkId.logicalChunkId, insertTime: defaultChunkId.timeBucketed[0] });
            allChunks.push({ id: LHSToleranceChunkId.logicalChunkId, insertTime: LHSToleranceChunkId.timeBucketed[0] });
            allChunks.push({ id: RHSToleranceChunkId.logicalChunkId, insertTime: RHSToleranceChunkId.timeBucketed[0] });


            for (const chunk of allChunks) {
                this.updateQueryPlan(tagName, chunk.id, diskIndexFn, tempResults);
                orderedChunkIds.push(chunk);
            }
            visitedChunkIds.set(defaultChunkId.logicalChunkId, allChunks.map(chunk => chunk.id));
        }
        visitedChunkIds.clear();

        const results = new Map<logicalChunkId, Map<tagName, Map<setDiskPath, diskIndex>>>();
        orderedChunkIds = orderedChunkIds.sort((a, b) => a.insertTime - b.insertTime);//Ascending according to insert time.
        for (let index = orderedChunkIds.length - 1; index >= 0; index--) {//But we need Descending cause of M.V.C.C which writes updated versions of samples to latest time.
            results.set(orderedChunkIds[index].id, tempResults.get(orderedChunkIds[index].id));
        }
        return { groupedTags: tagsGroupedByLogicalChunkId, plan: results };
    }

    public async[Symbol.asyncDispose]() {
        await this.chunkLinkRegistry[Symbol.asyncDispose]();
        if (this.selfWorker !== undefined) {
            this.selfWorker[Symbol.dispose]();
        }
        const payload = {
            header: HeaderPayload.Shutdown,
            subHeader: "Shutdown called from Dispose",
            payload: ''
        } as IThreadCommunication<string>;
        for (let index = 0; index < this.externalWorkers.length; index++) {
            this.externalWorkers[index].postMessage(serialize(payload));
            this.externalWorkers[index] = null;
        }
    }
}