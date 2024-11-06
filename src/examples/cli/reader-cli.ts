import { RedisHashMap } from "../../non-volatile-hash-map/redis-hash-map.js";
import { formatKB, formatMB, generateTagNames, trackMemory } from "../utils.js";
import { StringToBigIntAlgos } from "../../string-to-number-algos.js";
import { GridScaleFactory } from "../../grid-scale-factory.js";
import { GridScaleConfig } from "../../grid-scale-config.js";

//Query Plan
//Read
//Merge
const stats = { heapPeakMemory: 0, rssPeakMemory: 0 };
const trackMemoryFunc = trackMemory.bind(stats);
trackMemoryFunc.stats = stats;
const interval = setInterval(trackMemoryFunc, 1000); // Check memory usage every 1 second

const threads = 10;
const redisConnectionString = "redis://localhost:6379";
const stringToNumberAlgo = StringToBigIntAlgos[0];
const gsConfig = new GridScaleConfig();
gsConfig.workerCount = threads;
const chunkRegistry = new RedisHashMap(redisConnectionString);
await chunkRegistry.initialize();
const gridScale = await GridScaleFactory.create(chunkRegistry, new URL("../chunk-implementation/chunk-sqlite.js", import.meta.url), stringToNumberAlgo, gsConfig);

trackMemoryFunc();
console.log(`Started with ${threads} threads @ ${formatMB(formatKB(trackMemoryFunc.stats.heapPeakMemory)).toFixed(1)} heap used & ${formatMB(formatKB(trackMemoryFunc.stats.rssPeakMemory)).toFixed(1)} rss`);
const totalTags = 100;
const startInclusiveTime = 0;//Date.now();
const endExclusiveTime = 86400000//startInclusiveTime + config.timeBucketWidth;

const tagNames = generateTagNames(totalTags, 1);

console.time("Total")
const results = {};

for (let i = 0; i < 1; i++) {
    const time = Date.now();
    const resultTagNames = new Set<string>();
    const diagnostics = new Map<string, any>();
    const pageCursor = gridScale.iteratorByTimePage(tagNames, startInclusiveTime, i + endExclusiveTime, `Q[${i}]`, 10000, diagnostics);
    for await (const page of pageCursor) {
        trackMemoryFunc();
        for (const row of page) {
            resultTagNames.add(row[4]);
        }
        //break;
    }
    diagnostics.set("totalTime", Date.now() - time);
    diagnostics.set("totalTags", resultTagNames.size);
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
console.table(results);




console.time("Close Operation");
await (chunkRegistry[Symbol.asyncDispose] && chunkRegistry[Symbol.asyncDispose]() || Promise.resolve(chunkRegistry[Symbol.dispose] && chunkRegistry[Symbol.dispose]()));
await gridScale[Symbol.asyncDispose]();
console.timeEnd("Close Operation");

clearInterval(interval);
console.log(`Heap Peak Memory: ${formatMB(formatKB(trackMemoryFunc.stats.heapPeakMemory)).toFixed(1)}MB`);
console.log(`RSS Peak Memory: ${formatMB(formatKB(trackMemoryFunc.stats.rssPeakMemory)).toFixed(1)}MB`);

// Started with 10 threads @ 21.7 heap used & 171.6 rss 100 tags 86400000 time range
// Total: 1:15.131 (m:ss.mmm)
// ┌─────────┬─────────────────────────────────────┬──────────┬─────────────┬────────────┬───────────┬───────────┬───────────┬────────────┬───────────┐
// │ (index) │             workersPlan             │ planTime │ pageCounter │ rowCounter │ yieldTime │ totalTime │ totalTags │   maxRSS   │  maxHeap  │
// ├─────────┼─────────────────────────────────────┼──────────┼─────────────┼────────────┼───────────┼───────────┼───────────┼────────────┼───────────┤
// │  Run 0  │             'Total:10'              │    4     │     867     │  8640000   │   25215   │   25219   │    100    │ '1155.6MB' │ '490.0MB' │
// │ Run 0-0 │ 'worker:6 plan:0 shards:20 tags:90' │          │             │            │           │           │           │            │           │
// │ Run 0-1 │ 'worker:8 plan:0 shards:20 tags:10' │          │             │            │           │           │           │            │           │
// │  Run 1  │             'Total:10'              │    6     │     867     │  8640000   │   25178   │   25184   │    100    │ '1155.6MB' │ '490.0MB' │
// │ Run 1-0 │ 'worker:6 plan:0 shards:20 tags:90' │          │             │            │           │           │           │            │           │
// │ Run 1-1 │ 'worker:8 plan:0 shards:20 tags:10' │          │             │            │           │           │           │            │           │
// │  Run 2  │             'Total:10'              │    6     │     867     │  8640000   │   24721   │   24727   │    100    │ '1360.9MB' │ '490.0MB' │
// │ Run 2-0 │ 'worker:6 plan:0 shards:20 tags:90' │          │             │            │           │           │           │            │           │
// │ Run 2-1 │ 'worker:8 plan:0 shards:20 tags:10' │          │             │            │           │           │           │            │           │
// └─────────┴─────────────────────────────────────┴──────────┴─────────────┴────────────┴───────────┴───────────┴───────────┴────────────┴───────────┘
// Close Operation: 5.133ms
// Heap Peak Memory: 490.0MB
// RSS Peak Memory: 1360.9MB.

// Started with 0 threads @ 21.4 heap used & 73.4 rss 100 tags 86400000 time range
// Total: 54.363s
// ┌─────────┬─────────────────────────────────────┬──────────┬─────────────┬────────────┬───────────┬───────────┬───────────┬───────────┬───────────┐
// │ (index) │             workersPlan             │ planTime │ pageCounter │ rowCounter │ yieldTime │ totalTime │ totalTags │  maxRSS   │  maxHeap  │
// ├─────────┼─────────────────────────────────────┼──────────┼─────────────┼────────────┼───────────┼───────────┼───────────┼───────────┼───────────┤
// │  Run 0  │              'Total:0'              │    6     │     866     │  8640000   │   18605   │   18611   │    100    │ '566.5MB' │ '102.2MB' │
// │ Run 0-0 │ 'worker:0 plan:0 shards:20 tags:10' │          │             │            │           │           │           │           │           │
// │ Run 0-1 │ 'worker:0 plan:1 shards:20 tags:90' │          │             │            │           │           │           │           │           │
// │  Run 1  │              'Total:0'              │    7     │     866     │  8640000   │   18005   │   18012   │    100    │ '716.5MB' │ '102.2MB' │
// │ Run 1-0 │ 'worker:0 plan:0 shards:20 tags:10' │          │             │            │           │           │           │           │           │
// │ Run 1-1 │ 'worker:0 plan:1 shards:20 tags:90' │          │             │            │           │           │           │           │           │
// │  Run 2  │              'Total:0'              │    4     │     866     │  8640000   │   17735   │   17739   │    100    │ '776.8MB' │ '102.2MB' │
// │ Run 2-0 │ 'worker:0 plan:0 shards:20 tags:10' │          │             │            │           │           │           │           │           │
// │ Run 2-1 │ 'worker:0 plan:1 shards:20 tags:90' │          │             │            │           │           │           │           │           │
// └─────────┴─────────────────────────────────────┴──────────┴─────────────┴────────────┴───────────┴───────────┴───────────┴───────────┴───────────┘
// Close Operation: 56.18ms
// Heap Peak Memory: 102.2MB
// RSS Peak Memory: 776.8MB