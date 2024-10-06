// import { bucketString, bucketTime, generateFileName, generateOrFetchIdentity } from "./utilities.js";

// export {
//     bucketString,
//     bucketTime,
//     generateOrFetchIdentity,
//     generateFileName
// } 

import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';
import { cpus } from "node:os"
import { fileURLToPath } from 'url';
import { IRedisHashMap, IWriterOutput, updateDisplacedData, writeData } from "./writer.js";
import { createClient } from 'redis';
import { dbFileNameBuilder, MD5Calculator } from './chunk-calculator.js';
import { join } from 'node:path';
import { readdirSync } from 'node:fs';
import Database from 'better-sqlite3';

interface WorkerInput {
    id: string,
    timeBucketWidth: number,
    tagBucketWidth: number,
    redisConnection: string,
    type: "writer" | "reader";
    prefix: string,
    sep: string
}

interface WorkerInputWriter extends WorkerInput {
    disksPaths: string[],
    data: [string, number[]][],
}

interface WorkerInputReader extends WorkerInput {
    setPaths: Map<string, string[]>,
    tags: string[],
    rangeInclusiveExclusive: [number, number],
}

interface WorkerOutput {
    id: string,
    steps: Map<string, number>,
    details: IWriterOutput
}

const totalTags = 50000;
const totalSamplesPerTag = 1;
const singleInsertTime = 1.984;    //Insert Operation: 1.984ms keeps changing according to IOPS
const currentFilePath = fileURLToPath(import.meta.url);
if (isMainThread) {
    function generateRandomString(length: number): string {
        const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_+[]{}|;:,.<>?';
        let result = '';
        const charactersLength = characters.length;
        for (let i = 0; i < length; i++) {
            result += characters.charAt(Math.floor(Math.random() * charactersLength));
        }
        return result;
    }

    console.time("Generate Operation");
    const data = new Map<string, number[]>();
    const samples = new Array<number>();
    for (let tags = 0; tags < totalTags; tags++) {
        const tagName = generateRandomString(255);
        if (samples.length === 0) {
            for (let time = 0; time < totalSamplesPerTag; time++) {
                samples.push(time * 1000);
                samples.push(Math.floor(Math.random() * 100));
            }
        }
        data.set(tagName, samples);
    }
    console.timeEnd("Generate Operation");

    console.time("Operation");
    const totalCPUs = 1;//cpus().length;
    const workerHandles = new Array<Promise<WorkerOutput>>();
    const chunkSize = Math.ceil(data.size / totalCPUs);
    const dataEntries = Array.from(data.entries());
    for (let thread = 0; thread < totalCPUs; thread++) {
        workerHandles.push(new Promise<WorkerOutput>((resolve, reject) => {
            const workerInput = {
                id: `${process.pid}-${thread}`,
                timeBucketWidth: 86400000,
                tagBucketWidth: 50000,
                redisConnection: 'redis://localhost:6379',
                prefix: "D",
                sep: "|",
                type: "reader"
            } as WorkerInput
            if (workerInput.type === "writer") {
                //Writer
                (workerInput as WorkerInputWriter).disksPaths = [`../data/scaled-set1/disk1`, `../data/scaled-set1/disk2`, `../data/scaled-set1/disk3`, `../data/scaled-set1/disk4`, `../data/scaled-set1/disk5`];
                (workerInput as WorkerInputWriter).data = dataEntries.slice(thread * chunkSize, (thread + 1) * chunkSize);
            }
            else {
                //Reader
                (workerInput as WorkerInputReader).setPaths = new Map<string, string[]>([["../data/scaled-set1", ["disk1", "disk2", "disk3", "disk4", "disk5"]]]);
                // (workerInput as WorkerInputReader).setPaths.set("../data/scaled-set1", ["disk1", "disk2", "disk3", "disk4", "disk5"]);
                (workerInput as WorkerInputReader).tags = dataEntries.slice(thread * chunkSize, (thread + 1) * chunkSize).map(_ => _[0]);
                (workerInput as WorkerInputReader).rangeInclusiveExclusive = [0, 86400000];
            }
            const worker = new Worker(currentFilePath, { workerData: workerInput });
            worker.on('message', resolve);
            worker.on('error', reject);
            worker.on('exit', (code) => {
                if (code !== 0)
                    reject(new Error(`Worker stopped with exit code ${code}`));
            });
        }));
    }
    const totalSamples = totalTags * totalSamplesPerTag;
    console.log(`H-Scaled Instances: ${totalCPUs} for ${totalSamples} samples, Time:${(totalSamples * singleInsertTime) / 1000}sec , Est Time: ${((totalSamples * singleInsertTime) / totalCPUs) / 1000}sec`);
    const results = (await Promise.allSettled(workerHandles))
    const failed = [];
    results.forEach((r) => {
        if (r.status === 'fulfilled') {
            console.log(`Id: ${r.value.id}`);
            console.log(`Steps:`);
            console.table(Array.from(r.value.steps.entries()).map((_: Record<string, any>) => ({ "Name": _[0], "Time(ms)": _[1] })));

            if (r.value.details !== null) {
                console.log(`Details:`);
                console.log(`Total Chunks Written :${r.value.details.chunks.size}, Total Displaced Data: ${r.value.details.dataDisplacements.size}`);
                // console.table(Array.from(r.value.details.chunks.entries()).map((_: Record<string, any>) => _[0]));
            }
            console.log();
        }
        else {
            failed.push(r);
        }
    });

    if (failed.length > 0) console.table(failed);
    console.timeEnd("Operation");

} else {
    const client = await createClient({ url: (workerData as WorkerInput).redisConnection })
        .on('error', err => console.log('Redis Client Error', err))
        .connect();
    const redisClient = {
        set: async (key: string, fieldValues: string[]) => {
            await client.HSET(key, fieldValues);
        },
        get: async (key: string, fieldsOnly = true) => {
            if (fieldsOnly === true) {
                return client.HKEYS(key) || [];
            }
            else {
                return [];
            }
        },
        del: async (key: string[]) => {
            //console.log(`DEL: ${key}`);
        }
    } as IRedisHashMap;

    if ((workerData as WorkerInput).type === "writer") {
        const input = workerData as WorkerInputWriter;
        const steps = new Map<string, number>();

        let startTime = Date.now();
        const fileName = dbFileNameBuilder(input.id.toString());
        const wOutput = writeData(input.disksPaths, input.data, input.tagBucketWidth, input.timeBucketWidth, new Set<string>(), fileName, input.prefix, input.sep);
        steps.set("writeData", Date.now() - startTime);

        startTime = Date.now();
        if (wOutput.dataDisplacements.size > 0) {
            await updateDisplacedData(wOutput.dataDisplacements, input.id, redisClient);
        }
        steps.set("updateDisplacements", Date.now() - startTime);

        const result = {
            id: input.id,
            steps: steps,
            details: wOutput
        } as WorkerOutput;
        parentPort.postMessage(result);
    }
    else {
        const input = workerData as WorkerInputReader;
        const dbFileNameRegex = new RegExp(dbFileNameBuilder("*"));
        const steps = new Map<string, number>();

        for await (const chunk of paginateByTime(input.tags, input.rangeInclusiveExclusive, input.timeBucketWidth, input.tagBucketWidth, input.prefix, input.sep, input.setPaths, redisClient)) {
            const cursors = readPage(chunk, query, dbFileNameRegex);
            const resultCursor = mergeSortIterators(cursors, filterByInsertTimeAndSortBySampleTime);
            for (const resultPage of resultCursor) {
                console.log(resultPage);
            }
        }

        const result = {
            id: input.id,
            steps: steps,
            details: null
        } as WorkerOutput;

        parentPort.postMessage(result);

    }

    await client.disconnect();
}

export interface IPaginatedChunk {
    queryPlan: Map<string, { tagNames: string[], orderedLogicalDirectoryPaths: Map<string, Set<string>> }>,
    timeRangeInclusiveExclusive: [number, number];
}

async function* paginateByTime(tagNames: string[], timeRangeInclusiveExclusive: [number, number], timeBucketWidth: number, tagBucketWidth: number, prefix: string, sep: string, setPaths: Map<string, string[]>, redisClient: IRedisHashMap): AsyncGenerator<IPaginatedChunk> {

    for (let timeIndex = timeRangeInclusiveExclusive[0]; timeIndex < timeRangeInclusiveExclusive[1]; timeIndex += timeBucketWidth) {
        const results = new Map<string, { tagNames: string[], orderedLogicalDirectoryPaths: Map<string, Set<string>> }>();
        for (let tagIndex = 0; tagIndex < tagNames.length; tagIndex++) {
            const tagName = tagNames[tagIndex];
            const chunkId = MD5Calculator(tagName, timeIndex, tagBucketWidth, timeBucketWidth);
            const logicalChunkId = chunkId.logicalChunkId(prefix, sep);
            //Skip if already exists, just add tag
            if (results.has(logicalChunkId)) {
                results.get(logicalChunkId)?.tagNames.push(tagName);
            }
            else {
                const allLogicalChunkIds = await redisClient.get(logicalChunkId, true);
                allLogicalChunkIds.push(logicalChunkId);
                const sortedChunkDirectoryPaths = new Map(
                    allLogicalChunkIds
                        .sort((a, b) => a[0].localeCompare(b[0]))//TODO:Sorting might be broken
                        .reverse()
                        .map((logicalChunkId) => {
                            const directoryPaths = new Array<string>();
                            setPaths.forEach((disks, setPath) => {
                                const diskIndex = chunkId.limitIndex(disks.length);
                                directoryPaths.push(join(setPath, disks[diskIndex], logicalChunkId));
                            });
                            return [logicalChunkId, new Set(directoryPaths)]
                        }));
                results.set(logicalChunkId, { tagNames: [tagName], orderedLogicalDirectoryPaths: sortedChunkDirectoryPaths });
            }
        }

        yield { queryPlan: results, timeRangeInclusiveExclusive: [timeIndex, timeIndex + timeBucketWidth] };
    }
}

function filterByInsertTimeAndSortBySampleTime(frame: any[]): [number, number] {
    if (frame.length === 0) return [-1, -1];
    let minIndex = -1, purgeIndex = -1;
    for (let i = 1; i < frame.length; i++) {
        if (frame[i] === null) continue;
        if (minIndex === -1) {
            minIndex = i;
        }
        else {
            if (frame[i].sampleTime < frame[minIndex].sampleTime) {
                minIndex = i;
            }
            else if (frame[i].sampleTime === frame[minIndex].sampleTime) {
                if (frame[i].insertTime < frame[minIndex].insertTime) {
                    purgeIndex = i;
                }
                else if (frame[i].insertTime > frame[minIndex].insertTime) {
                    purgeIndex = minIndex;
                    minIndex = i;
                }
                else {
                    purgeIndex = i;
                }
            }
        }
    }
    return [minIndex, purgeIndex];
};

function query(tagNames: string[], startInclusive: number, endExclusive: number): string {
    return tagNames
        .map(tagName => {
            const tagNamesInHex = Buffer.from(tagName, "utf-8").toString('hex');
            return `SELECT * FROM [${tagNamesInHex}] WHERE sampleTime >= ${startInclusive} AND sampleTime < ${endExclusive} ORDER BY sampleTime ASC;`;
        })
        .join("\n UNION ALL \n");

}

function readPage(page: IPaginatedChunk, query: (tableName: string[], startInclusive: number, endExclusive: number) => string, dbFileNameExp: RegExp): Array<IterableIterator<unknown>> {
    const resultPointers = new Array<IterableIterator<unknown>>();
    for (const [logicalChunkId, chunkInfo] of page.queryPlan.entries()) {
        for (const directoryPaths of chunkInfo.orderedLogicalDirectoryPaths.values()) {
            for (const directoryPath of directoryPaths) {
                let matchingDatabases = new Array<string>();
                try {
                    matchingDatabases = readdirSync(directoryPath, { recursive: false, withFileTypes: true })
                        .filter(dirent => dbFileNameExp.test(dirent.name) && dirent.isFile())
                        .map(dirent => join(directoryPath, dirent.name));
                }
                catch (e) {
                    if (e.code === 'ENOENT') {
                        continue;//No Directory exists so no DB or Data
                    }
                    else {
                        throw e;
                    }
                }
                for (let index = 0; index < matchingDatabases.length; index++) {
                    const dbFilePath = matchingDatabases[index];
                    let db: Database.Database;
                    try {
                        db = new Database(dbFilePath, { readonly: true, fileMustExist: true });
                    }
                    catch (e) {
                        console.log(`DB Open Failed for ${directoryPath}`);
                        continue;//No DB or Data
                    }
                    const sqlQuery = query(chunkInfo.tagNames, page.timeRangeInclusiveExclusive[0], page.timeRangeInclusiveExclusive[1]);
                    resultPointers.push(db.prepare(sqlQuery).iterate());
                }
            }
        }
    }
    return resultPointers;
}

function* mergeSortIterators<T>(iterators: IterableIterator<T>[], compareFunction: (elements: T | null[]) => [number, number]): IterableIterator<T> {
    const compareFrame = iterators.map(_ => _.next().value || null);
    let nullCounter = compareFrame.length;
    while (nullCounter > 0) {
        const [yieldIndex, purgeIndex] = compareFunction(compareFrame);
        if (yieldIndex === -1) break;
        yield compareFrame[yieldIndex];

        compareFrame[yieldIndex] = null;
        compareFrame[yieldIndex] = iterators[yieldIndex].next().value || null;
        if (compareFrame[yieldIndex] === null) {
            nullCounter--;
        }

        if (purgeIndex !== -1) {
            compareFrame[purgeIndex] = null;
            compareFrame[purgeIndex] = iterators[purgeIndex].next().value || null;
            if (compareFrame[purgeIndex] === null) {
                nullCounter--;
            }
        }
    }
}

// export interface ISample {
//     sampleTime: number,
//     insertTime: number,
//     nValue: number,
//     oValue: string
// }

//Performance(Write): 12 Threads 5000*86400=432000000 in 3:32.931 (m:ss.mmm) [19GB on Disk]
//Performance(Write): 12 Threads 50000*1=50000 in 2:20.134 (m:ss.mmm) [write data:138642ms, updateDisplacements:168ms]

