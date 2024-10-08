import { TConfig } from "./t-config.js";
import { GridScale, samples, tagName } from "./grid-scale.js";
import { ChunkLinker } from "./chunk-linker.js";


// Invoke an Instance of chunk-container
// Invoke an Instance of chunk-linker
// Accept REST calls
// Accept WebSocket calls
// For Write collect all chunks and set data parallel.
// For Read generate a query plan and get data parallel.

function generateRandomString(length: number): string {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_+[]{}|;:,.<>?';
    let result = '';
    const charactersLength = characters.length;
    for (let i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
    return result;
}

const totalTags = 50000;
const totalSamplesPerTag = 1;
const generatedData = new Map<string, number[]>();
const config: TConfig = {
    setPaths: new Map<string, string[]>([["../data/high-speed-1", ["disk1", "disk2", "disk3", "disk4", "disk5"]]]),
    activePath: "../data/high-speed-1",
    tagBucketWidth: 50000,
    timeBucketWidth: 86400000,
    fileNamePre: "ts",
    fileNamePost: ".db",
    timeBucketTolerance: 1,
    activeCalculatorIndex: 0,
    maxDBOpen: 1000,
    logicalChunkPrefix: "D",
    logicalChunkSeperator: "|",
    redisConnection: 'redis://localhost:6379',
}
const gridScale = new GridScale(config);
const linker = new ChunkLinker(config);

//1.050s
console.time("Generate Operation");
const samples = new Array<number>();
for (let tags = 0; tags < totalTags; tags++) {
    const tagName = generateRandomString(255);
    if (samples.length === 0) {
        for (let time = 0; time < totalSamplesPerTag; time++) {
            samples.push(time * 1000);
            samples.push(Math.floor(Math.random() * 100));
        }
    }
    generatedData.set(tagName, samples);
}
console.timeEnd("Generate Operation");

//1.178s
console.time("Split Operation")
const chunkGroups = gridScale.allocateChunks(generatedData);
console.timeEnd("Split Operation");

//5:21.305 (m:ss.mmm)
console.time("Write Operation");
const selfId = `${process.pid}-${1}`;
const insertTime = Date.now();
for (const [logicalChunkId, diskIndexSamplesMap] of chunkGroups) {
    const chunk = gridScale.getChunk(logicalChunkId);
    for (const [diskIndex, diskSampleSets] of diskIndexSamplesMap) {
        const sampleSet = new Map<tagName, number[][]>();
        for (const [tagName, samples] of diskSampleSets) {
            const formattedSamples = new Array<number[]>();
            for (let index = 0; index < samples.length; index += 2) {
                formattedSamples.push([samples[index], insertTime, samples[index + 1]]);
            }
            sampleSet.set(tagName, formattedSamples);
        }
        chunk.set(diskIndex, sampleSet, selfId);
    }
}
console.timeEnd("Write Operation");


console.time("Link Operation");
//await linker.link();
console.timeEnd("Link Operation");


console.time("Close Operation");
gridScale[Symbol.dispose]();
await linker[Symbol.asyncDispose]();
console.timeEnd("Close Operation");

console.log("Total Chunks: ", chunkGroups.size);

