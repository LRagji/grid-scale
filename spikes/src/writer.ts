import { TConfig } from "./t-config.js";
import { GridScaleBase, GridScaleWriter, samples } from "./grid-scale.js";
import { ChunkLinker } from "./chunk-linker.js";
import { CommonConfig, generateRandomSamples } from "./utils.js";


// Invoke an Instance of chunk-container
// Invoke an Instance of chunk-linker
// Accept REST calls
// Accept WebSocket calls
// For Write collect all chunks and set data parallel.
// For Read generate a query plan and get data parallel.


const totalTags = 1000;
const totalSamplesPerTag = 10;
const config: TConfig = CommonConfig()
const insertTime = Date.now();
//const selfId = `${process.pid}-${1}`;
const gridScale = new GridScaleWriter<number[]>(config, 1);
//const linker = new ChunkLinker(config);

function formatSamples(input: samples, insertTime: number): number[][] {
    const returnValues = new Array<number[]>();
    for (let index = 0; index < input.length; index += 2) {
        returnValues.push([input[index], insertTime, input[index + 1]]);
    }
    return returnValues;
}

//1.050s
console.time("Generate Operation");
const generatedData = generateRandomSamples(totalTags, totalSamplesPerTag);
console.timeEnd("Generate Operation");

console.time("Total")
await gridScale.store<number[]>(generatedData, formatSamples, insertTime);
console.timeEnd("Total")

await gridScale[Symbol.asyncDispose]();

// //1.041s
// console.time("Split Operation")
// const chunkInfo = gridScale.upsertPlan(generatedData, insertTime, formatSamples);
// console.timeEnd("Split Operation");

// //6:22.962 (m:ss.mmm)
// console.time("Write Operation");
// for (const [logicalChunkId, diskIndexSamplesMap] of chunkInfo.chunkAllocations) {
//     const chunk = gridScale.getChunk(logicalChunkId);
//     for (const [diskIndex, diskSampleSets] of diskIndexSamplesMap) {
//         chunk.set(diskIndex, diskSampleSets, selfId);
//     }
// }
// console.timeEnd("Write Operation");

// //13.373s
// console.time("Link Operation");
// let displacedChunks = 0;
// for (const [indexedLogicalChunkId, logicalChunkIds] of chunkInfo.chunkDisplacements) {
//     await linker.link(indexedLogicalChunkId, Array.from(logicalChunkIds.related.values()), logicalChunkIds.insertTimeBucketed, selfId);
//     displacedChunks += logicalChunkIds.related.size;
// }
// console.timeEnd("Link Operation");


// console.time("Close Operation");
// await gridScale[Symbol.asyncDispose]();
// await linker[Symbol.asyncDispose]();
// console.timeEnd("Close Operation");

// console.log(`Fragmentation: ${((displacedChunks / chunkInfo.chunkAllocations.size) * 100).toFixed(0)}% ,Total Chunks: ${chunkInfo.chunkAllocations.size}`);



// Generate Operation: 1.017s
// Split Operation: 1.041s
// Write Operation: 6:22.962 (m:ss.mmm)
// Link Operation: 13.373s
// Close Operation: 1.801s
// Fragmentation: 100%,Total Chunks: 50000