import { RedisHashMap } from "../../non-volatile-hash-map/redis-hash-map.js";
import { formatKB, formatMB, generateTagNames, trackMemory } from "../utils.js";
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
const gsConfig = new GridScaleConfig();
gsConfig.workerCount = threads;
const chunkRelations = new RedisHashMap(redisConnectionString);
await chunkRelations.initialize();
const gridScale = await GridScaleFactory.create(chunkRelations, new URL("../chunk-factory-implementation/cached-chunk-factory.js", import.meta.url), gsConfig);

trackMemoryFunc();
console.log(`Started with ${threads} threads @ ${formatMB(formatKB(trackMemoryFunc.stats.heapPeakMemory)).toFixed(1)} heap used & ${formatMB(formatKB(trackMemoryFunc.stats.rssPeakMemory)).toFixed(1)} rss`);
const totalTags = 100;
const startInclusiveTime = 0;//Date.now();
const endExclusiveTime = 86400000//startInclusiveTime + config.timeBucketWidth;

const tagIds = generateTagNames(totalTags, 1);

console.time("Total")
const results = {};

for (let i = 0; i < 1; i++) {
    const time = Date.now();
    const resultTagIds = new Set<string>();
    const diagnostics = new Map<string, any>();
    const pageCursor = gridScale.iteratorByTimePage(tagIds, startInclusiveTime, i + endExclusiveTime, `Q[${i}]`, 10000, diagnostics);
    for await (const page of pageCursor) {
        trackMemoryFunc();
        for (const row of page) {
            resultTagIds.add(row[4]);
        }
        //break;
    }
    diagnostics.set("totalTime", Date.now() - time);
    diagnostics.set("totalTags", resultTagIds.size);
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
await (chunkRelations[Symbol.asyncDispose] && chunkRelations[Symbol.asyncDispose]() || Promise.resolve(chunkRelations[Symbol.dispose] && chunkRelations[Symbol.dispose]()));
await gridScale[Symbol.asyncDispose]();
console.timeEnd("Close Operation");

clearInterval(interval);
console.log(`Heap Peak Memory: ${formatMB(formatKB(trackMemoryFunc.stats.heapPeakMemory)).toFixed(1)}MB`);
console.log(`RSS Peak Memory: ${formatMB(formatKB(trackMemoryFunc.stats.rssPeakMemory)).toFixed(1)}MB`);

// Started with 10 threads @ 21.7 heap used & 172.8 rss with 100 tags and 0 -> 86400000 time range
// Total: 27.746s
// ┌─────────┬─────────────────────────────────────┬──────────┬─────────────┬────────────┬───────────┬───────────┬───────────┬───────────┬──────────┐
// │ (index) │             workersPlan             │ planTime │ pageCounter │ rowCounter │ yieldTime │ totalTime │ totalTags │  maxRSS   │ maxHeap  │
// ├─────────┼─────────────────────────────────────┼──────────┼─────────────┼────────────┼───────────┼───────────┼───────────┼───────────┼──────────┤
// │  Run 0  │             'Total:10'              │    3     │     865     │  8640000   │   27742   │   27746   │    100    │ '633.9MB' │ '61.7MB' │
// │ Run 0-0 │ 'worker:7 plan:0 shards:2 tags:100' │          │             │            │           │           │           │           │          │
// └─────────┴─────────────────────────────────────┴──────────┴─────────────┴────────────┴───────────┴───────────┴───────────┴───────────┴──────────┘
// Close Operation: 7.148ms
// Heap Peak Memory: 61.7MB
// RSS Peak Memory: 633.9MB