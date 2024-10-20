import { ChunkPlanner } from "./chunk-planner.js";
import { GridScale } from "./grid-scale.js";
import { GridThreadPlugin } from "./multi-threads/grid-thread-plugin.js";
import { RedisHashMap } from "./non-volatile-hash-map/redis-hash-map.js";
import { TConfig } from "./t-config.js";
import { CommonConfig, generateTagNames } from "./utils.js";

//Query Plan
//Read
//Merge
const threads = 10;
console.log(`Started with ${threads} threads`);

const config: TConfig = CommonConfig();
const chunkRegistry = new RedisHashMap(config.redisConnection);
await chunkRegistry.initialize();
const chunkPlanner = new ChunkPlanner(chunkRegistry, config);
const gridThreadPlugin = new GridThreadPlugin(process.pid.toString(), config.fileNamePre, config.fileNamePost, config.maxDBOpen, 4, 0);
const gridScale = new GridScale(chunkRegistry, chunkPlanner, gridThreadPlugin);
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
await (gridScale[Symbol.asyncDispose] && gridScale[Symbol.asyncDispose]() || Promise.resolve(gridScale[Symbol.dispose] && gridScale[Symbol.dispose]()));
await gridThreadPlugin[Symbol.asyncDispose]();
console.timeEnd("Close Operation");