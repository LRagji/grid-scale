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
    readerThreads: 10,
    writerThreads: 10
}
const insertTime = Date.now();
const selfId = `${process.pid}-${1}`;
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

//1.041s
console.time("Split Operation")
function formatSamples(input: samples, insertTime: number): number[][] {
    const returnValues = new Array<number[]>();
    for (let index = 0; index < samples.length; index += 2) {
        returnValues.push([input[index], insertTime, input[index + 1]]);
    }
    return returnValues;
}
const chunkInfo = gridScale.allocateChunksWithFormattedSamples<number[]>(generatedData, insertTime, formatSamples);
console.timeEnd("Split Operation");

//6:22.962 (m:ss.mmm)
console.time("Write Operation");
for (const [logicalChunkId, diskIndexSamplesMap] of chunkInfo.chunkAllocations) {
    const chunk = gridScale.getChunk(logicalChunkId);
    for (const [diskIndex, diskSampleSets] of diskIndexSamplesMap) {
        chunk.set(diskIndex, diskSampleSets, selfId);
    }
}
console.timeEnd("Write Operation");

//13.373s
console.time("Link Operation");
let displacedChunks = 0;
for (const [indexedLogicalChunkId, logicalChunkIds] of chunkInfo.chunkDisplacements) {
    await linker.link(indexedLogicalChunkId, Array.from(logicalChunkIds.values()), selfId);
    displacedChunks += logicalChunkIds.size;
}
console.timeEnd("Link Operation");


console.time("Close Operation");
gridScale[Symbol.dispose]();
await linker[Symbol.asyncDispose]();
console.timeEnd("Close Operation");

console.log(`Fragmentation: ${((displacedChunks / chunkInfo.chunkAllocations.size) * 100).toFixed(0)}% ,Total Chunks: ${chunkInfo.chunkAllocations.size}`);

// Generate Operation: 1.017s
// Split Operation: 1.041s
// Write Operation: 6:22.962 (m:ss.mmm)
// Link Operation: 13.373s
// Close Operation: 1.801s
// Fragmentation: 100%,Total Chunks: 50000