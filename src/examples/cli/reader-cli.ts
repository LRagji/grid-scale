import { RedisHashMap } from "../../non-volatile-hash-map/redis-hash-map.js";
import { generateTagNames } from "../utils.js";
import { StringToNumberAlgos } from "../../string-to-number-algos.js";
import { GridScaleFactory } from "../../grid-scale-factory.js";
import { GridScaleConfig } from "../../grid-scale-config.js";

//Query Plan
//Read
//Merge
let heapPeakMemory = 0;
let rssPeakMemory = 0;

function trackMemory() {
    const memoryUsage = process.memoryUsage();
    heapPeakMemory = Math.max(heapPeakMemory, memoryUsage.heapUsed);
    rssPeakMemory = Math.max(rssPeakMemory, memoryUsage.rss);
}

function formatMemory(memory: number) {
    return `${(memory / 1024 / 1024).toFixed(2)} MB`;
}

const interval = setInterval(trackMemory, 1000); // Check memory usage every 1 second
const threads = 10;
const redisConnectionString = "redis://localhost:6379";
const stringToNumberAlgo = StringToNumberAlgos[2];
const gsConfig = new GridScaleConfig();
gsConfig.workerCount = threads;
trackMemory();
console.log(`Started with ${threads} threads @ ${formatMemory(heapPeakMemory)} heap used & ${formatMemory(rssPeakMemory)} rss`);

const chunkRegistry = new RedisHashMap(redisConnectionString);
await chunkRegistry.initialize();
const gridScale = await GridScaleFactory.create(chunkRegistry, new URL("../chunk-implementation/chunk-sqlite.js", import.meta.url), stringToNumberAlgo, gsConfig);

const totalTags = 1000;
const startInclusiveTime = 0;//Date.now();
const endExclusiveTime = 86400000//startInclusiveTime + config.timeBucketWidth;

const tagNames = generateTagNames(totalTags, 1);

console.time("Total")
const results = [];

for (let i = 0; i < 1; i++) {
    const time = Date.now();
    const resultTagNames = new Set<string>();
    const diagnostics = new Map<string, number>();
    const rowCursor = gridScale.iteratorByTimePage(tagNames, startInclusiveTime, i + endExclusiveTime, `Q[${i}]`, 10000, diagnostics);
    let counter = 0;
    for await (const row of rowCursor) {
        //console.log(row);
        if (counter % 1000 === 0) {
            trackMemory();
        }
        resultTagNames.add(row[4]);
        counter++;
        //break;
    }
    results.push([Date.now() - time, resultTagNames.size, ...diagnostics.entries()]);
}

console.timeEnd("Total")
console.table(results);


console.time("Close Operation");
await (chunkRegistry[Symbol.asyncDispose] && chunkRegistry[Symbol.asyncDispose]() || Promise.resolve(chunkRegistry[Symbol.dispose] && chunkRegistry[Symbol.dispose]()));
await gridScale[Symbol.asyncDispose]();
console.timeEnd("Close Operation");

clearInterval(interval);
console.log(`Heap Peak Memory: ${formatMemory(heapPeakMemory)}`);
console.log(`RSS Peak Memory: ${formatMemory(rssPeakMemory)}`);

// Started with 10 threads 500 tags & 86400000 time range
// Total: 2:41.787 (m:ss.mmm)
// ┌─────────┬────────┬─────┬───────────────────┬─────────────────────────┐
// │ (index) │ 0      │ 1   │ 2                 │ 3                       │
// ├─────────┼────────┼─────┼───────────────────┼─────────────────────────┤
// │ 0       │ 161789 │ 500 │ [ 'planTime', 6 ] │ [ 'yieldTime', 161783 ] │
// └─────────┴────────┴─────┴───────────────────┴─────────────────────────┘

// Started with 0 threads 500 tags & 86400000 time range
// Total: 1:53.256 (m:ss.mmm)
// ┌─────────┬────────┬─────┬───────────────────┬─────────────────────────┐
// │ (index) │ 0      │ 1   │ 2                 │ 3                       │
// ├─────────┼────────┼─────┼───────────────────┼─────────────────────────┤
// │ 0       │ 113262 │ 500 │ [ 'planTime', 6 ] │ [ 'yieldTime', 113255 ] │
// └─────────┴────────┴─────┴───────────────────┴─────────────────────────┘