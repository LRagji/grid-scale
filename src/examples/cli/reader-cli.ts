import { RedisHashMap } from "../../non-volatile-hash-map/redis-hash-map.js";
import { formatKB, formatMB, generateTagNames, trackMemory } from "../utils.js";
import { GridScaleFactory } from "../../grid-scale-factory.js";
import { GridScaleConfig } from "../../grid-scale-config.js";
import { ChunkMetaRegistry } from "../chunk-meta-implementation/chunk-meta-registry.js";


//Query Plan
//Read
//Merge
const stats = { heapPeakMemory: 0, rssPeakMemory: 0 };
const trackMemoryFunc = trackMemory.bind(stats);
trackMemoryFunc.stats = stats;
const interval = setInterval(trackMemoryFunc, 1000); // Check memory usage every 1 second

const threads = 10;
const gsConfig = new GridScaleConfig();
gsConfig.workerCount = threads;
const chunkLinkerRedisConnectionString = "redis://localhost:6379";
const chunkRelations = new RedisHashMap(chunkLinkerRedisConnectionString, "chunk-linker-");
await chunkRelations.initialize();
const chunkCacheRedisConnectionString = "redis://localhost:6380";
const chunkCache = new RedisHashMap(chunkCacheRedisConnectionString, "chunk-cache-");
await chunkCache.initialize();
const chunkMetaRedisConnectionString = "redis://localhost:6381";
const chunkMetaRegistry = new ChunkMetaRegistry(chunkMetaRedisConnectionString, "chunk-meta-");
await chunkMetaRegistry.initialize();
const gridScale = await GridScaleFactory.create(chunkRelations, new URL("../chunk-factory-implementation/cached-chunk-factory.js", import.meta.url), chunkMetaRegistry, chunkCache, gsConfig);

trackMemoryFunc();
console.log(`Started with ${threads} threads @ ${formatMB(formatKB(trackMemoryFunc.stats.heapPeakMemory)).toFixed(1)} heap used & ${formatMB(formatKB(trackMemoryFunc.stats.rssPeakMemory)).toFixed(1)} rss`);
const totalTags = gsConfig.TagBucketWidth * 1;
const startInclusiveTime = 0;//Date.now();
const endExclusiveTime = gsConfig.TimeBucketWidth * 1//startInclusiveTime + config.timeBucketWidth;

const tagIds = generateTagNames(0, totalTags, 1);

console.time("Total")
const consoleTableResults = {};
const countPerTagFunction = (first: boolean, last: boolean, page: any[][], acc: Map<string, number>) => {
    const returnObject = {
        yield: false,
        yieldValue: null,
        accumulator: acc
    };
    if (first === true) {
        return returnObject;
    }
    if (last === true) {
        console.log("Total Tags: ", acc.size);
        for (const [tagId, count] of acc.entries()) {
            if (count === 86400) {
                acc.delete(tagId);
            }
        }
        returnObject.yield = true;
        returnObject.yieldValue = Array.from(acc.entries());
        returnObject.accumulator.clear();
        return returnObject;
    }
    for (const row of page) {
        const tagId = row[0];
        const existingCount = row[1];
        const count = acc.get(tagId) ?? 0;
        acc.set(tagId, count + existingCount);
    }
    return returnObject;
};

const multiThreadDirector = (timePages: [number, number][], tagPages: bigint[][], previousTimeStep: [number, number] | undefined, previousTagStep: bigint[] | undefined) => {
    // let timeStepIndex = timePages.indexOf(previousTimeStep);
    // if (timeStepIndex === -1) {
    //     return { nextTimeStep: timePages[0], nextTagStep: tagPages.flat() };
    // }
    // timeStepIndex++;
    // if (timeStepIndex >= timePages.length) {
    //     return { nextTimeStep: undefined, nextTagStep: undefined };
    // }

    if (previousTagStep === undefined && previousTimeStep === undefined) {
        const minTime = Math.min(...timePages.flat());
        const maxTime = Math.max(...timePages.flat());
        return { nextTimeStep: [minTime, maxTime] as [number, number], nextTagStep: tagPages.flat() };
    }
    else {
        return { nextTimeStep: undefined, nextTagStep: undefined };
    }
};

const mapLambda = new URL("../lambda/tag-sample-count.js", import.meta.url);
for (let i = 0; i < 1; i++) {
    const time = Date.now();
    const resultTagIds = new Set<string>();
    //const timePages = gridScale.fetchTimePages(startInclusiveTime, i + endExclusiveTime);

    let tagCounts = new Map<string, number>();
    //while (timePages.length > 0) {
    const diagnostics = new Array<Map<string, any>>();
    //const timePage = timePages.shift();
    //const result = gridScale.iteratorByTimePageWithAggregate(tagIds, timePage[0], timePage[1], countPerTagFunction, tagCounts, `Q[${i}]`, 10000, diagnostics);
    //for await (const processedRow of result) {
    //  tagCounts = new Map<string, any>(processedRow as [string, number][]);
    //}
    const pageCursor = gridScale.iterator(tagIds, startInclusiveTime, endExclusiveTime, undefined, multiThreadDirector, 10000, mapLambda, countPerTagFunction, tagCounts, false, diagnostics);
    for await (const page of pageCursor) {
        trackMemoryFunc();
        tagCounts = new Map<string, any>(page as [string, number][]);
        // for (const row of page) {
        //     resultTagIds.add(row[4]);
        // }
        //break;
    }

    //}
    for (const [stepIdx, stepDiagnostics] of diagnostics.entries()) {
        const workerDiagnostics = stepDiagnostics.get("workersPlan") as string[] ?? [];
        stepDiagnostics.set("step", stepIdx);
        stepDiagnostics.set("workersPlan", `Total:${threads}`);
        stepDiagnostics.set("totalTime", Date.now() - time);
        stepDiagnostics.set("totalTags", resultTagIds.size);
        stepDiagnostics.set("maxRSS", formatMB(formatKB(trackMemoryFunc.stats.rssPeakMemory)).toFixed(1) + "MB");
        stepDiagnostics.set("maxHeap", formatMB(formatKB(trackMemoryFunc.stats.heapPeakMemory)).toFixed(1) + "MB");
        consoleTableResults[`Run:${i} S:${stepIdx}`] = Object.fromEntries(stepDiagnostics.entries());
        for (const [idx, workerDiagnostic] of workerDiagnostics.entries()) {
            consoleTableResults[`Run:${i} S:${stepIdx} W:${idx}`] = { "workersPlan": workerDiagnostic, "step": stepIdx };
        }
    }


    if (tagCounts.size > 0) {
        console.log(`${tagCounts.size} Tags without 86400 samples.`);
    }
    else {
        console.log(`All tags have 86400 samples.`);
    }
}

console.timeEnd("Total");
console.table(consoleTableResults);



console.time("Close Operation");
await (chunkRelations[Symbol.asyncDispose] && chunkRelations[Symbol.asyncDispose]() || Promise.resolve(chunkRelations[Symbol.dispose] && chunkRelations[Symbol.dispose]()));
await (chunkCache[Symbol.asyncDispose] && chunkCache[Symbol.asyncDispose]() || Promise.resolve(chunkCache[Symbol.dispose] && chunkCache[Symbol.dispose]()));
await (chunkMetaRegistry[Symbol.asyncDispose] && chunkMetaRegistry[Symbol.asyncDispose]() || Promise.resolve(chunkMetaRegistry[Symbol.dispose] && chunkMetaRegistry[Symbol.dispose]()));
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