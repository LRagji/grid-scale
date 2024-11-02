import { RedisHashMap } from "../../non-volatile-hash-map/redis-hash-map.js";
import { generateTagNames } from "../utils.js";
import { StringToNumberAlgos } from "../../string-to-number-algos.js";
import { GridScaleFactory } from "../../grid-scale-factory.js";
import { GridScaleConfig } from "../../grid-scale-config.js";

//Query Plan
//Read
//Merge
const threads = 10;
const redisConnectionString = "redis://localhost:6379";
const stringToNumberAlgo = StringToNumberAlgos[2];
const gsConfig = new GridScaleConfig();
gsConfig.workerCount = threads;
console.log(`Started with ${threads} threads`);

const chunkRegistry = new RedisHashMap(redisConnectionString);
await chunkRegistry.initialize();
const gridScale = await GridScaleFactory.create(chunkRegistry, stringToNumberAlgo, gsConfig);

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
await gridScale[Symbol.asyncDispose]();
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