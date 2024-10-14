import { TConfig } from "./t-config.js";
import { GridScaleBase, samples } from "./grid-scale.js";
import { CommonConfig, generateRandomSamples } from "./utils.js";


// Invoke an Instance of chunk-container
// Invoke an Instance of chunk-linker
// Accept REST calls
// Accept WebSocket calls
// For Write collect all chunks and set data parallel.
// For Read generate a query plan and get data parallel.

console.log(`Started`);

const totalTags = 50000;
const totalSamplesPerTag = 1;
const config: TConfig = CommonConfig()
const insertTime = Date.now();
const gridScale = new GridScaleBase<number[]>(config, 10);

function formatSamples(input: samples, insertTime: number): number[][] {
    const returnValues = new Array<number[]>();
    for (let index = 0; index < input.length; index += 2) {
        returnValues.push([input[index], insertTime, input[index + 1]]);
    }
    return returnValues;
}

const generatedData = generateRandomSamples(totalTags, totalSamplesPerTag);

console.time("Total")
const diagnostics = new Map<string, number>();
await gridScale.store<number[]>(generatedData, formatSamples, insertTime, diagnostics);
console.timeEnd("Total")

for (const [key, value] of diagnostics) {
    console.log(`${key} ${value}`);
}

await gridScale[Symbol.asyncDispose]();





// Generate Operation: 1.017s
// Split Operation: 1.041s
// Write Operation: 6:22.962 (m:ss.mmm)
// Link Operation: 13.373s
// Close Operation: 1.801s
// Fragmentation: 100%,Total Chunks: 50000

// Single Thread
// Generate Operation: 12.966ms
// Split Operation: 117.912ms
// Write Operation: 25.318s
// Link Operation: 65.455ms
// Fragmentation: 100% ,Total Chunks: 100
// Total: 25.503s

// 10 Threads
// Generate Operation: 18.07ms
// Split Operation: 127.049ms
// Write Operation: 5.455s
// Link Operation: 60.543ms
// Fragmentation: 100 % , Total Chunks: 100
// Total: 5.644s