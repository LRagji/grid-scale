import { RedisHashMap } from "../../non-volatile-hash-map/redis-hash-map.js";
import { formatKB, formatMB, generateTagNames, trackMemory } from "../utils.js";
import { GridScaleFactory } from "../../grid-scale-factory.js";
import { GridScaleConfig } from "../../types/grid-scale-config.js";
import { ChunkMetaRegistry } from "../chunk-meta-implementation/chunk-meta-registry.js";
import { IteratorPaginationConfig } from "../../types/iterator-pagination-config.js";
import { IteratorLambdaConfig } from "../../types/iterator-lambda-config.js";
import { IteratorPlanConfig } from "../../types/iterator-plan-config.js";
import { IteratorCacheConfig } from "../../types/iterator-cache-config.js";
import countPerTagFunction from "../lambda/tag-sample-count.js";

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
console.log(`Started with ${threads} threads @ ${formatMB(formatKB(trackMemoryFunc.stats.heapPeakMemory)).toFixed(1)} heap used & ${formatMB(formatKB(trackMemoryFunc.stats.rssPeakMemory)).toFixed(1)} rss |${new Date().toString()}|`);
const totalTags = gsConfig.TagBucketWidth * 10;
const startInclusiveTime = 0;//Date.now();
const endExclusiveTime = gsConfig.TimeBucketWidth * 2//startInclusiveTime + config.timeBucketWidth;
let expectedSampleCount = (endExclusiveTime - startInclusiveTime) / 1000;
expectedSampleCount = Math.min(86400, expectedSampleCount)

const tagIds = generateTagNames(0, totalTags, 1);


console.time("Total")
const consoleTableResults = {};

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

const planConfig = new IteratorPlanConfig();
const cacheConfig = new IteratorCacheConfig();

const paginationConfig = new IteratorPaginationConfig();
paginationConfig.TimeStepSize = gsConfig.TimeBucketWidth;
paginationConfig.TagStepSize = gsConfig.TagBucketWidth;
paginationConfig.nextStepCallback = multiThreadDirector;

const lambdaConfig = new IteratorLambdaConfig<Map<string, number>>();
lambdaConfig.aggregateLambda = countPerTagFunction;


for (let i = 0; i < 1; i++) {
    const time = Date.now();
    const resultTagIds = new Set<string>();
    //const timePages = gridScale.fetchTimePages(startInclusiveTime, i + endExclusiveTime);

    lambdaConfig.accumulator = new Map<string, number>();
    //while (timePages.length > 0) {
    planConfig.diagnostics = new Array<Map<string, any>>();
    //const timePage = timePages.shift();
    //const result = gridScale.iteratorByTimePageWithAggregate(tagIds, timePage[0], timePage[1], countPerTagFunction, tagCounts, `Q[${i}]`, 10000, diagnostics);
    //for await (const processedRow of result) {
    //  tagCounts = new Map<string, any>(processedRow as [string, number][]);
    //}
    const pageCursor = gridScale.iterator(tagIds, startInclusiveTime, endExclusiveTime, planConfig, paginationConfig, cacheConfig, lambdaConfig);
    for await (const page of pageCursor) {
        trackMemoryFunc();
        lambdaConfig.accumulator = new Map<string, any>(page as [string, number][]);
        console.log("Total Tags: ", lambdaConfig.accumulator.size);

        for (const [tagId, count] of lambdaConfig.accumulator.entries()) {
            const delta = Math.abs(expectedSampleCount - count);
            if (delta <= 1) {
                lambdaConfig.accumulator.delete(tagId);
            }
        }

        // for (const row of page) {
        //     resultTagIds.add(row[4]);
        // }
        //break;
    }

    //}
    for (const [stepIdx, stepDiagnostics] of planConfig.diagnostics.entries()) {
        const workerDiagnostics = stepDiagnostics.get("workersPlan") as string[] ?? [];
        stepDiagnostics.set("step", stepIdx);
        stepDiagnostics.set("workersPlan", `Threads:${threads} Tasks:${workerDiagnostics.length}`);
        stepDiagnostics.set("totalTime", Date.now() - time);
        stepDiagnostics.set("totalTags", resultTagIds.size);
        stepDiagnostics.set("maxRSS", formatMB(formatKB(trackMemoryFunc.stats.rssPeakMemory)).toFixed(1) + "MB");
        stepDiagnostics.set("maxHeap", formatMB(formatKB(trackMemoryFunc.stats.heapPeakMemory)).toFixed(1) + "MB");
        consoleTableResults[`Run:${i} S:${stepIdx}`] = Object.fromEntries(stepDiagnostics.entries());
        if (workerDiagnostics.length > 1000) { continue; }
        for (const [idx, workerDiagnostic] of workerDiagnostics.entries()) {
            consoleTableResults[`Run:${i} S:${stepIdx} W:${idx}`] = { "workersPlan": workerDiagnostic };
        }
    }


    if (lambdaConfig.accumulator.size > 0) {
        console.log(`${lambdaConfig.accumulator.size} Tags without ${expectedSampleCount} samples.`);
    }
    else {
        console.log(`All tags have ${expectedSampleCount} samples.`);
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