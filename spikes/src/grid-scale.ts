import { Chunk } from "./chunk.js";
import { ChunkId } from "./chunk-id.js";
import { TConfig } from "./t-config.js";
import { ChunkLinker } from "./chunk-linker.js";
import { cpus } from "node:os";
import { Worker } from "node:worker_threads";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { ChunkCache } from "./chunk-cache.js";
import { BackgroundThread } from "./background-thread.js";
import { deserialize, IThreadCommunication } from "./threads/i-thread-communication.js";

export type logicalChunkId = string;
export type diskIndex = number;
export type tagName = string;
export type samples = number[];
export type setDiskPath = string;
export type upsertPlan<rowType> = { chunkAllocations: Map<logicalChunkId, Map<diskIndex, Map<tagName, rowType[]>>>, chunkDisplacements: Map<logicalChunkId, { insertTimeBucketed: number, related: Set<logicalChunkId> }> };

export class GridScaleBase<rowType> {

    private readonly chunkCache: ChunkCache;
    protected readonly chunkLinkRegistry: ChunkLinker

    constructor(private readonly config: TConfig) {
        this.chunkCache = new ChunkCache(config, config.maxDBOpen, 10);
        this.chunkLinkRegistry = new ChunkLinker(config);
    }

    protected upsertPlan<rawRowType extends Array<any>>(sampleSets: Map<tagName, rawRowType>, insertTime: number, formatCallback: (dataToFormat: samples, insertTime: number) => rowType[]): upsertPlan<rowType> {
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

    public async readQueryPlanPageByTime(tagNames: tagName[], startInclusiveTime: number)
        : Promise<Map<logicalChunkId, Map<tagName, Map<setDiskPath, diskIndex>>>> {
        type chunkInfo = { id: logicalChunkId, insertTime: number };
        const startInclusiveBucketedTime = ChunkId.bucket(startInclusiveTime, this.config.timeBucketWidth);
        const tempResults = new Map<logicalChunkId, Map<tagName, Map<setDiskPath, diskIndex>>>();
        let orderedChunkIds = new Array<chunkInfo>();

        for (let tagIndex = 0; tagIndex < tagNames.length; tagIndex++) {
            const tagName = tagNames[tagIndex];
            const defaultChunkId = ChunkId.from(tagName, startInclusiveBucketedTime, this.config);
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
                const tagDiskAccess = tempResults.get(chunk.id) || new Map<tagName, Map<setDiskPath, diskIndex>>();
                const diskAccess = tagDiskAccess.get(tagName) || new Map<setDiskPath, diskIndex>();
                this.config.setPaths.forEach((diskPaths, setPath) => {
                    const diskIndex = defaultChunkId.tagNameMod(diskPaths.length);
                    diskAccess.set(setPath, diskIndex);
                });
                tagDiskAccess.set(tagName, diskAccess);
                tempResults.set(chunk.id, tagDiskAccess);
                orderedChunkIds.push(chunk);
            }
        }
        const results = new Map<logicalChunkId, Map<tagName, Map<setDiskPath, diskIndex>>>();
        orderedChunkIds = orderedChunkIds.sort((a, b) => a.insertTime - b.insertTime);//Ascending according to insert time.
        for (let index = orderedChunkIds.length - 1; index >= 0; index--) {//But we need Descending cause of M.V.C.C which writes updated versions of samples to latest time.
            results.set(orderedChunkIds[index].id, tempResults.get(orderedChunkIds[index].id));
        }
        return results;
    }

    public getChunk(logicalChunkId: logicalChunkId): Chunk {
        return this.chunkCache.getChunk(logicalChunkId);
    }

    public async [Symbol.asyncDispose]() {
        this.chunkCache[Symbol.dispose]();
        await this.chunkLinkRegistry[Symbol.asyncDispose]();
    }
}

export class GridScaleWriter<rowType extends Array<any>> extends GridScaleBase<rowType> {

    private readonly selfStoreThread: BackgroundThread;
    private readonly workers = new Array<Worker>();

    constructor(config: TConfig, workerCount: number) {
        super(config);
        const totalWorkers = Math.max(cpus.length, Math.min(0, workerCount));
        const workerFilePath = join(dirname(fileURLToPath(import.meta.url)), './threads/long-running-thread.js');
        for (let index = 0; index < totalWorkers; index++) {
            this.workers.push(new Worker(workerFilePath, { workerData: { config, cacheLimit: config.maxDBOpen, bulkDropLimit: 10 } }));
        }
        if (this.workers.length === 0) {
            this.selfStoreThread = new BackgroundThread();
            this.selfStoreThread.initialize(`${process.pid}`, { config, cacheLimit: config.maxDBOpen, bulkDropLimit: 10 });
        }
    }

    public async store<rawRowType extends Array<any>>(rawData: Map<tagName, rawRowType>, rowTransformer: (dataToFormat: rawRowType, insertTime: number) => rowType[], insertTime = Date.now()): Promise<void> {
        const writerPartIdentifier = `${process.pid}`;
        console.time("Split Operation")
        const upsertPlan = super.upsertPlan(rawData, insertTime, rowTransformer);
        console.timeEnd("Split Operation");

        console.time("Write Operation");
        if (this.selfStoreThread !== undefined) {
            const payload = {
                header: 'Execute',
                subHeader: 'Store',
                payload: upsertPlan
            } as IThreadCommunication<upsertPlan<rowType>>;
            this.selfStoreThread.work(payload);
        }
        else {
            const chunkSize = Math.ceil(upsertPlan.chunkAllocations.size / this.workers.length);
            const workLoad = Array.from(upsertPlan.chunkAllocations.entries());
            const workerHandles = new Array<Promise<IThreadCommunication<unknown>>>();
            for (let workerIndex = 0; workerIndex < this.workers.length; workerIndex++) {
                workerHandles.push(new Promise<IThreadCommunication<unknown>>((resolve, reject) => {
                    const worker = this.workers[workerIndex];
                    const payload = {
                        header: 'Execute',
                        subHeader: 'Store',
                        payload: { chunkAllocations: new Map(workLoad.splice(0, chunkSize)), chunkDisplacements: new Map() }
                    } as IThreadCommunication<upsertPlan<rowType>>;
                    worker.postMessage(JSON.stringify(payload));
                    worker.once('message', (message) => {
                        worker.removeAllListeners();
                        const comMessage = deserialize(message);
                        if (comMessage.subHeader.toLowerCase() === "error") {
                            reject(new Error(comMessage.payload));
                        }
                        else {
                            resolve(comMessage);
                        }
                    });
                    worker.on('error', (error) => {
                        worker.removeAllListeners();
                        reject(error);
                    });
                }));
            }
            await Promise.all(workerHandles);
        }
        console.timeEnd("Write Operation");

        //13.373s
        console.time("Link Operation");
        let displacedChunks = 0;
        for (const [indexedLogicalChunkId, logicalChunkIds] of upsertPlan.chunkDisplacements) {
            await this.chunkLinkRegistry.link(indexedLogicalChunkId, Array.from(logicalChunkIds.related.values()), logicalChunkIds.insertTimeBucketed, `${writerPartIdentifier}-${insertTime}`);
            displacedChunks += logicalChunkIds.related.size;
        }
        console.timeEnd("Link Operation");

        console.log(`Fragmentation: ${((displacedChunks / upsertPlan.chunkAllocations.size) * 100).toFixed(0)}% ,Total Chunks: ${upsertPlan.chunkAllocations.size}`);
    }

    public async [Symbol.asyncDispose]() {
        await super[Symbol.asyncDispose]();
        if (this.selfStoreThread !== undefined) {
            this.selfStoreThread[Symbol.dispose]();
        }
        const payload = {
            header: "Shutdown",
            subHeader: "Shutdown",
            payload: ''
        } as IThreadCommunication<string>;
        for (let index = 0; index < this.workers.length; index++) {
            this.workers[index].postMessage(JSON.stringify(payload));
            this.workers[index] = null;
        }
    }

}