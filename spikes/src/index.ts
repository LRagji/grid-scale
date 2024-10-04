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

interface WorkerInput {
    id: number,
    disksPaths: string[],
    data: [string, number[]][],
    timeBucketWidth: number,
    tagBucketWidth: number
}

interface WorkerOutput {
    id: number,
    time: number,
    output: IWriterOutput
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

    console.time("Insert Operation");
    const totalCPUs = cpus().length;
    const workerHandles = new Array<Promise<{ id: number, time: number }>>();
    const chunkSize = Math.ceil(data.size / totalCPUs);
    const dataEntries = Array.from(data.entries());
    for (let thread = 0; thread < totalCPUs; thread++) {
        workerHandles.push(new Promise<WorkerOutput>((resolve, reject) => {
            const worker = new Worker(currentFilePath, {
                workerData: {
                    id: thread,
                    disksPaths: [`../data/scale1/disk1`, `../data/scale1/disk2`, `../data/scale1/disk3`, `../data/scale1/disk4`, `../data/scale1/disk5`],
                    data: dataEntries.slice(thread * chunkSize, (thread + 1) * chunkSize),
                    timeBucketWidth: 86400000,
                    tagBucketWidth: 50000,
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
    const results = (await Promise.allSettled(workerHandles)).map(r => r.status === 'fulfilled' ? r.value : r);
    console.table(results);
    console.timeEnd("Insert Operation");


} else {

    const input = workerData as WorkerInput;
    const startTime = Date.now();

    const wOutput = writeData(`W-${input.id}`, input.disksPaths, input.data, input.tagBucketWidth, input.timeBucketWidth, new Set<string>());

    const result = {
        id: input.id,
        time: Date.now() - startTime,
        output: wOutput
    } as WorkerOutput;
    parentPort.postMessage(result);
}

//Performance: 12 Threads 5000*86400=432000000 in 3:32.931 (m:ss.mmm) 19GB on Disk
