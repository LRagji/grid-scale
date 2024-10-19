import { ChunkCache } from "./chunk-cache.js";
import { ChunkPlanner } from "./chunk-planner.js";
import { ChunkSqlite } from "./chunk/chunk-sqlite.js";
import { GridScale } from "./grid-scale.js";
import { kWayMerge } from "./merge/k-way-merge.js";
import { RedisHashMap } from "./non-volatile-hash-map/redis-hash-map.js";
import { TConfig } from "./t-config.js";
import { CommonConfig, frameMerge, generateTagNames } from "./utils.js";

//Query Plan
//Read
//Merge
const threads = 10;
console.log(`Started with ${threads} threads`);

const config: TConfig = CommonConfig();
const fileRegex = new RegExp("^" + config.fileNamePre + "[a-z0-9-]+\\" + config.fileNamePost + "$");//`^ts[a-z0-9]+\\.db$`;
const chunkRegistry = new RedisHashMap(config.redisConnection);
await chunkRegistry.initialize();
const chunkPlanner = new ChunkPlanner(chunkRegistry, config);
const chunkCache = new ChunkCache<ChunkSqlite>(ChunkSqlite, 30, 10, <T>(cursors) => kWayMerge<T>(cursors, frameMerge<T>), fileRegex);
const gridScale = new GridScale(chunkRegistry, chunkPlanner, chunkCache, (identity) => config.fileNamePre + identity + config.fileNamePost, <T>(cursors) => kWayMerge<T>(cursors, frameMerge<T>));
const totalTags = 50000;
const startInclusiveTime = 0;//Date.now();
const endExclusiveTime = 1//startInclusiveTime + config.timeBucketWidth;

const tagNames = generateTagNames(totalTags, 1);

console.time("Total")
const resultTagNames = new Set<string>();
const diagnostics = new Map<string, number>();
const rowCursor = gridScale.iteratorByTimePage(tagNames, startInclusiveTime, endExclusiveTime, diagnostics);
for await (const row of rowCursor) {
    //console.log(row);
    resultTagNames.add(row[4]);
    //break;
}
console.log(`Total Tags: ${resultTagNames.size}`);
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