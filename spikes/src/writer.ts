import { TConfig } from "./t-config.js";
import { GridScale } from "./grid-scale.js";
import { CommonConfig, frameMerge, generateRandomSamples } from "./utils.js";
import { RedisHashMap } from "./non-volatile-hash-map/redis-hash-map.js";
import { ChunkPlanner } from "./chunk-planner.js";
import { ChunkCache } from "./chunk-cache.js";
import { ChunkSqlite } from "./chunk/chunk-sqlite.js";
import { kWayMerge } from "./merge/k-way-merge.js";


// Invoke an Instance of chunk-container
// Invoke an Instance of chunk-linker
// Accept REST calls
// Accept WebSocket calls
// For Write collect all chunks and set data parallel.
// For Read generate a query plan and get data parallel.

const threads = 10;
console.log(`Started with ${threads} threads`);

const totalTags = 50000;
const totalSamplesPerTag = 1;
const config: TConfig = CommonConfig()
const insertTime = Date.now();
const fileRegex = new RegExp("^" + config.fileNamePre + "[a-z0-9-]+\\" + config.fileNamePost + "$");//`^ts[a-z0-9]+\\.db$`;
const chunkRegistry = new RedisHashMap(config.redisConnection);
await chunkRegistry.initialize();
const chunkPlanner = new ChunkPlanner(chunkRegistry, config);
const chunkCache = new ChunkCache<ChunkSqlite>(ChunkSqlite, 30, 10, <T>(cursors) => kWayMerge<T>(cursors, frameMerge<T>), fileRegex);
const gridScale = new GridScale(chunkRegistry, chunkPlanner, chunkCache, (identity) => config.fileNamePre + identity + config.fileNamePost, <T>(cursors) => kWayMerge<T>(cursors, frameMerge<T>));

const insertTimeCol = (time: number, tag: string) => insertTime;
const numericCol = (time: number, tag: string) => Math.floor(Math.random() * 1000);
const otherCol = (time: number, tag: string) => null;

const generatedData = generateRandomSamples(totalTags, totalSamplesPerTag, [insertTimeCol, numericCol, otherCol]);

console.time("Total")
const diagnostics = new Map<string, number>();
await gridScale.store(generatedData, 4, 0, insertTime, diagnostics);
console.timeEnd("Total")

for (const [key, value] of diagnostics) {
    console.log(`${key} ${value}`);
}

console.time("Close Operation");
await (chunkRegistry[Symbol.asyncDispose] && chunkRegistry[Symbol.asyncDispose]() || Promise.resolve(chunkRegistry[Symbol.dispose] && chunkRegistry[Symbol.dispose]()));
await (chunkPlanner[Symbol.asyncDispose] && chunkPlanner[Symbol.asyncDispose]() || Promise.resolve(chunkPlanner[Symbol.dispose] && chunkPlanner[Symbol.dispose]()));
await (chunkCache[Symbol.asyncDispose] && chunkCache[Symbol.asyncDispose]() || Promise.resolve(chunkCache[Symbol.dispose] && chunkCache[Symbol.dispose]()));
await (gridScale[Symbol.asyncDispose] && gridScale[Symbol.asyncDispose]() || Promise.resolve(gridScale[Symbol.dispose] && gridScale[Symbol.dispose]()));
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