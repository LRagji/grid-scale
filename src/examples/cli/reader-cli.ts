import { ChunkPlanner } from "../../chunk-planner.js";
import { GridScale } from "../../grid-scale.js";
import { RedisHashMap } from "../../non-volatile-hash-map/redis-hash-map.js";
import { TConfig } from "../t-config.js";
import { CommonConfig, generateTagNames } from "../utils.js";
import { StatefulProxyManager } from "node-apparatus";
import { fileURLToPath } from "node:url";
import { StringToNumberAlgos } from "../../string-to-number-algos.js";

//Query Plan
//Read
//Merge
const threads = 0;
console.log(`Started with ${threads} threads`);

const config: TConfig = CommonConfig();
const chunkRegistry = new RedisHashMap(config.redisConnection);
await chunkRegistry.initialize();
const chunkPlanner = new ChunkPlanner(chunkRegistry, StringToNumberAlgos[config.activeCalculatorIndex], config.tagBucketWidth, config.timeBucketWidth, config.logicalChunkPrefix, config.logicalChunkSeparator, config.timeBucketTolerance, config.activePath, config.setPaths);
const workerFilePath = fileURLToPath(new URL("../../grid-thread-plugin.js", import.meta.url));
const proxies = new StatefulProxyManager(threads, workerFilePath);
await proxies.initialize();
for (let idx = 0; idx < proxies.WorkerCount; idx++) {
    await proxies.invokeMethod("initialize", [`${process.pid.toString()}-${idx}`, config.fileNamePre, config.fileNamePost, config.maxDBOpen, 4, 0], idx);
}
const gridScale = new GridScale(chunkRegistry, chunkPlanner, proxies);
const totalTags = 500;
const startInclusiveTime = 0;//Date.now();
const endExclusiveTime = 86400000//startInclusiveTime + config.timeBucketWidth;

const tagNames = generateTagNames(totalTags, 1);

console.time("Total")
const results = [];

for (let i = 0; i < 1; i++) {
    const time = Date.now();
    const resultTagNames = new Set<string>();
    const diagnostics = new Map<string, number>();
    const rowCursor = gridScale.iteratorByTimePage(tagNames, startInclusiveTime, i + endExclusiveTime, `Q[${i}]`, undefined, diagnostics);
    for await (const row of rowCursor) {
        //console.log(row);
        resultTagNames.add(row[4]);
        //break;
    }
    results.push([Date.now() - time, resultTagNames.size, ...diagnostics.entries()]);
}

console.timeEnd("Total")
console.table(results);


console.time("Close Operation");
await (chunkRegistry[Symbol.asyncDispose] && chunkRegistry[Symbol.asyncDispose]() || Promise.resolve(chunkRegistry[Symbol.dispose] && chunkRegistry[Symbol.dispose]()));
await (chunkPlanner[Symbol.asyncDispose] && chunkPlanner[Symbol.asyncDispose]() || Promise.resolve(chunkPlanner[Symbol.dispose] && chunkPlanner[Symbol.dispose]()));
await (gridScale[Symbol.asyncDispose] && gridScale[Symbol.asyncDispose]() || Promise.resolve(gridScale[Symbol.dispose] && gridScale[Symbol.dispose]()));
await proxies[Symbol.asyncDispose]();
console.timeEnd("Close Operation");


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