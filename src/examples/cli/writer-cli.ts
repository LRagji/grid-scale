import { formatKB, formatMB, generateRandomSamples, trackMemory } from "../utils.js";
import * as v8 from 'v8';
import { RedisHashMap } from "../../non-volatile-hash-map/redis-hash-map.js";
import { GridScaleFactory } from "../../grid-scale-factory.js";
import { GridScaleConfig } from "../../types/grid-scale-config.js";
import { ChunkMetaRegistry } from "../chunk-meta-implementation/chunk-meta-registry.js";


// Invoke an Instance of chunk-container
// Invoke an Instance of chunk-linker
// Accept REST calls
// Accept WebSocket calls
// For Write collect all chunks and set data parallel.
// For Read generate a query plan and get data parallel.
const stats = { heapPeakMemory: 0, rssPeakMemory: 0 };
const trackMemoryFunc = trackMemory.bind(stats);
trackMemoryFunc.stats = stats;
const interval = setInterval(trackMemoryFunc, 1000); // Check memory usage every 1 second

const threads = 10;
const gsConfig = new GridScaleConfig();
gsConfig.workerCount = threads;

const totalTags = 100;
const totalSamplesPerTag = 86400;
let insertTime = Date.now();
// insertTime = insertTime - (insertTime % 86400000);
// insertTime = insertTime + 86400000;
const chunkLinkerRedisConnectionString = "redis://localhost:6379";
const chunkRelations = new RedisHashMap(chunkLinkerRedisConnectionString, "chunk-linker-");
await chunkRelations.initialize();
const lambdaCacheRedisConnectionString = "redis://localhost:6380";
const lambdaCache = new RedisHashMap(lambdaCacheRedisConnectionString, "lambda-cache-");
await lambdaCache.initialize();
const chunkMetaRedisConnectionString = "redis://localhost:6381";
const chunkMetaRegistry = new ChunkMetaRegistry(chunkMetaRedisConnectionString, "chunk-meta-");
await chunkMetaRegistry.initialize();
const gridScale = await GridScaleFactory.create(chunkRelations, new URL("../chunk-factory-implementation/cached-chunk-factory.js", import.meta.url), chunkMetaRegistry, lambdaCache, gsConfig);

trackMemoryFunc();
//v8.writeHeapSnapshot();
console.log(`Started with ${threads} threads @ ${formatMB(formatKB(trackMemoryFunc.stats.heapPeakMemory)).toFixed(1)} heap used & ${formatMB(formatKB(trackMemoryFunc.stats.rssPeakMemory)).toFixed(1)} rss`);

const insertTimeCol = (time: number, tag: bigint) => insertTime;
const numericCol = (time: number, tag: bigint) => parseInt(`${time}.${tag.toString()}`)//Math.floor(Math.random() * 1000);
const otherCol = (time: number, tag: bigint) => null;



console.time("Total")
//v8.writeHeapSnapshot();
const results = {};

for (let i = 0; i < 10; i++) {
    const generatedData = generateRandomSamples(totalTags, totalSamplesPerTag, i * gsConfig.TagBucketWidth, 0, [insertTimeCol, numericCol, otherCol]);
    const time = Date.now();
    const diagnostics = new Map<string, any>();
    await gridScale.store(generatedData, insertTime, diagnostics);
    diagnostics.set("totalTime", Date.now() - time);
    diagnostics.set("totalRecords", `${totalTags} X ${totalSamplesPerTag}`);
    diagnostics.set("maxRSS", formatMB(formatKB(trackMemoryFunc.stats.rssPeakMemory)).toFixed(1) + "MB");
    diagnostics.set("maxHeap", formatMB(formatKB(trackMemoryFunc.stats.heapPeakMemory)).toFixed(1) + "MB");
    const workerDiagnostics = diagnostics.get("workersPlan") as string[] ?? [];
    diagnostics.set("workersPlan", `Total:${threads}`);
    results[`Run ${i}`] = Object.fromEntries(diagnostics.entries());
    for (const [idx, workerDiagnostic] of workerDiagnostics.entries()) {
        results[`Run ${i}-${idx}`] = { "workersPlan": workerDiagnostic };
    }
}

console.timeEnd("Total");
//v8.writeHeapSnapshot();
console.table(results);

console.time("Close Operation");
await (chunkRelations[Symbol.asyncDispose] && chunkRelations[Symbol.asyncDispose]() || Promise.resolve(chunkRelations[Symbol.dispose] && chunkRelations[Symbol.dispose]()));
await (lambdaCache[Symbol.asyncDispose] && lambdaCache[Symbol.asyncDispose]() || Promise.resolve(lambdaCache[Symbol.dispose] && lambdaCache[Symbol.dispose]()));
await (chunkMetaRegistry[Symbol.asyncDispose] && chunkMetaRegistry[Symbol.asyncDispose]() || Promise.resolve(chunkMetaRegistry[Symbol.dispose] && chunkMetaRegistry[Symbol.dispose]()));
await gridScale[Symbol.asyncDispose]();
console.timeEnd("Close Operation");

clearInterval(interval);
//v8.writeHeapSnapshot();
console.log(`Heap Peak Memory: ${formatMB(formatKB(trackMemoryFunc.stats.heapPeakMemory)).toFixed(1)}MB`);
console.log(`RSS Peak Memory: ${formatMB(formatKB(trackMemoryFunc.stats.rssPeakMemory)).toFixed(1)}MB`);




// Started with 0 threads @ 21.4 heap used & 74.0 rss
// Total: 16.306s
// ┌─────────┬─────────────────────────────────────────────────────┬──────────┬───────────┬──────────┬───────────┬───────────────┬───────────┬───────────┐
// │ (index) │                     workersPlan                     │ planTime │ writeTime │ linkTime │ totalTime │ totalRecords  │  maxRSS   │  maxHeap  │
// ├─────────┼─────────────────────────────────────────────────────┼──────────┼───────────┼──────────┼───────────┼───────────────┼───────────┼───────────┤
// │  Run 0  │                      'Total:0'                      │   1798   │   14506   │    2     │   16306   │ '100 X 86400' │ '891.6MB' │ '790.7MB' │
// │ Run 0-0 │ 'worker:0 plan:0 shards:1 tags:100 records:8640000' │          │           │          │           │               │           │           │
// └─────────┴─────────────────────────────────────────────────────┴──────────┴───────────┴──────────┴───────────┴───────────────┴───────────┴───────────┘
// Close Operation: 6.285ms
// Heap Peak Memory: 790.7MB
// RSS Peak Memory: 891.6MB