import { generateRandomSamples } from "../utils.js";
import { RedisHashMap } from "../../non-volatile-hash-map/redis-hash-map.js";
import { StringToNumberAlgos } from "../../string-to-number-algos.js";
import { GridScaleFactory } from "../../grid-scale-factory.js";
import { GridScaleConfig } from "../../grid-scale-config.js";


// Invoke an Instance of chunk-container
// Invoke an Instance of chunk-linker
// Accept REST calls
// Accept WebSocket calls
// For Write collect all chunks and set data parallel.
// For Read generate a query plan and get data parallel.

const threads = 10;
const redisConnectionString = "redis://localhost:6379";
const stringToNumberAlgo = StringToNumberAlgos[2];
const gsConfig = new GridScaleConfig();
gsConfig.workerCount = threads;
console.log(`Started with ${threads} threads`);

const totalTags = 100;
const totalSamplesPerTag = 86400;
const insertTime = Date.now();
const chunkRegistry = new RedisHashMap(redisConnectionString);
await chunkRegistry.initialize();
const gridScale = await GridScaleFactory.create(chunkRegistry, new URL("../../chunk/chunk-sqlite.js", import.meta.url), stringToNumberAlgo, gsConfig);

const insertTimeCol = (time: number, tag: string) => insertTime;
const numericCol = (time: number, tag: string) => Math.floor(Math.random() * 1000);
const otherCol = (time: number, tag: string) => null;

const generatedData = generateRandomSamples(totalTags, totalSamplesPerTag, [insertTimeCol, numericCol, otherCol]);

console.time("Total")
const diagnostics = new Map<string, number>();
await gridScale.store(generatedData, insertTime, diagnostics);
console.timeEnd("Total")

for (const [key, value] of diagnostics) {
    console.log(`${key} ${value}`);
}

console.time("Close Operation");
await (chunkRegistry[Symbol.asyncDispose] && chunkRegistry[Symbol.asyncDispose]() || Promise.resolve(chunkRegistry[Symbol.dispose] && chunkRegistry[Symbol.dispose]()));
await gridScale[Symbol.asyncDispose]();
console.timeEnd("Close Operation");





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