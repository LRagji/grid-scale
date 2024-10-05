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
import { IWriterOutput, writeData } from "./writer.js";
import { createClient } from 'redis';

interface WorkerInput {
    id: number,
    disksPaths: string[],
    data: [string, number[]][],
    timeBucketWidth: number,
    tagBucketWidth: number,
    redisConnection: string
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
        const tagName = Buffer.from(generateRandomString(255)).toString('hex');
        if (samples.length === 0) {
            for (let time = 0; time < totalSamplesPerTag; time++) {
                samples.push(time * 1000);
                samples.push(Math.floor(Math.random() * 100));
            }
        }
        data.set(tagName, samples);
    }
    console.timeEnd("Generate Operation");

    console.time("Save Operation");
    const totalCPUs = cpus().length;
    const workerHandles = new Array<Promise<WorkerOutput>>();
    const chunkSize = Math.ceil(data.size / totalCPUs);
    const dataEntries = Array.from(data.entries());
    for (let thread = 0; thread < totalCPUs; thread++) {
        workerHandles.push(new Promise<WorkerOutput>((resolve, reject) => {
            const worker = new Worker(currentFilePath, {
                workerData: {
                    id: thread,
                    disksPaths: [`../data/scaled-set1/disk1`, `../data/scaled-set1/disk2`, `../data/scaled-set1/disk3`, `../data/scaled-set1/disk4`, `../data/scaled-set1/disk5`],
                    data: dataEntries.slice(thread * chunkSize, (thread + 1) * chunkSize),
                    timeBucketWidth: 86400000,
                    tagBucketWidth: 50000,
                    redisConnection: 'redis://localhost:6379'
                } as WorkerInput
            });
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
            // console.log(`Details:`);
            // console.table(Array.from(r.value.details.chunks.entries()).map((_: Record<string, any>) => _[0]));
            console.log();
        }
        else {
            failed.push(r);
        }
    });

    if (failed.length > 0) console.table(failed);
    console.timeEnd("Save Operation");
    // const client = await createClient({ url: 'redis://localhost:6379' })
    //     .on('error', err => console.log('Redis Client Error', err))
    //     .connect();
    // await client.HSET("key", ["field1", "value1", "field2", "value2"]);
    // let val = await client.HGET("key", "field1");
    // console.log(val);
    // val = await client.HGET("key", "field2");
    // console.log(val);
    // await client.disconnect();

} else {

    const input = workerData as WorkerInput;
    const writerIdentity = `W-${input.id}`;

    const steps = new Map<string, number>();

    let startTime = Date.now();
    const wOutput = writeData(writerIdentity, input.disksPaths, input.data, input.tagBucketWidth, input.timeBucketWidth, new Set<string>());
    steps.set("writeData", Date.now() - startTime);

    startTime = Date.now();
    const client = await createClient({ url: input.redisConnection })
        .on('error', err => console.log('Redis Client Error', err))
        .connect();
    if (wOutput.dataDisplacements.size > 0) {
        await updateDisplacedData(wOutput.dataDisplacements, writerIdentity, {
            set: async (key: string, values: string[]) => {
                await client.HSET(key, values);
            },
            get: async (key: string[]) => {
                //console.log(`GET: ${key}`);
                return null;
            },
            del: async (key: string[]) => {
                //console.log(`DEL: ${key}`);
            }
        });
    }
    await client.disconnect();
    steps.set("updateDisplacements", Date.now() - startTime);

    const result = {
        id: writerIdentity,
        steps: steps,
        details: wOutput
    } as WorkerOutput;
    parentPort.postMessage(result);
}

export interface IRedisHashMap {
    set(key: string, values: string[]): Promise<void>;
    get(key: string[]): Promise<string[] | null>;
    del(key: string[]): Promise<void>;
}



async function updateDisplacedData(displacedData: Map<string, Set<string>>, writerIdentity: string, redisConnection: IRedisHashMap): Promise<void> {
    let handles = Array<Promise<void>>();
    displacedData.forEach(async (value, key) => {
        const values = [];
        for (const item of value) {
            values.push(item);
            values.push(writerIdentity);
        }
        handles.push(redisConnection.set(key, values));
        if (handles.length === 1000) {
            await Promise.all(handles);
            handles = [];
        }
    });
    await Promise.all(handles);
}
//Performance: 12 Threads 5000*86400=432000000 in 3:32.931 (m:ss.mmm) 19GB on Disk

