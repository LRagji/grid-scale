import { ChunkId } from "./chunk-id.js";
import { ChunkLinker } from "./chunk-linker.js";
import { GridScale, logicalChunkId, tagName } from "./grid-scale.js";
import { kWayMerge } from "./k-way-merge.js";
import { TConfig } from "./t-config.js";

const config: TConfig = {
    setPaths: new Map<string, string[]>([["../data/high-speed-1", ["disk1", "disk2", "disk3", "disk4", "disk5"]]]),
    activePath: "../data/high-speed-1",
    tagBucketWidth: 50000,
    timeBucketWidth: 86400000,
    fileNamePre: "ts",
    fileNamePost: ".db",
    timeBucketTolerance: 1,
    activeCalculatorIndex: 0,
    maxDBOpen: 1000,
    logicalChunkPrefix: "D",
    logicalChunkSeperator: "|",
    redisConnection: 'redis://localhost:6379',
    readerThreads: 10,
    writerThreads: 10
}
const gridScale = new GridScale(config);

//const dbFileNameRegex = new RegExp("^" + input.config.fileNamePre + "[a-z0-9]+\\" + input.config.fileNamePost + "$");//`^ts[a-z0-9]+\\.db$`;

//Query Plan
//Read
//Merge
function generateRandomString(length: number): string {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_+[]{}|;:,.<>?';
    let result = '';
    const charactersLength = characters.length;
    for (let i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
    return result;
}
const totalTags = 50000;
const startInclusiveTime = Date.now();
const endExclusiveTime = startInclusiveTime + config.timeBucketWidth;
const tagNames = new Array<string>();

console.time("Generate Operation");
for (let tags = 0; tags < totalTags; tags++) {
    const tagName = generateRandomString(255);
    tagNames.push(tagName);
}
console.timeEnd("Generate Operation");

console.time("QueryPlan Operation")
const queryPlan = await gridScale.queryPlanPerTimeBucketWidth(tagNames, startInclusiveTime);
console.timeEnd("QueryPlan Operation");

console.time("Read Operation")
const resultCursor = new Map<tagName, IterableIterator<unknown>[]>();
for (const [chunkId, tagDiskInfo] of queryPlan) {
    const chunk = gridScale.getChunk(chunkId);
    for (const [tagName, diskInfo] of tagDiskInfo) {
        const cursors = resultCursor.get(tagName) || new Array<IterableIterator<unknown>>();
        cursors.push(...chunk.get(diskInfo, [tagName], startInclusiveTime, endExclusiveTime));
        resultCursor.set(tagName, cursors);
    }
}
console.timeEnd("Read Operation");

console.time("Merge Operation");
const frameParseFunction = (elements: any[]): { yieldIndex: number, purgeIndexes: number[] } => {
    return { yieldIndex: 0, purgeIndexes: [] };
};
resultCursor.forEach((cursors, tagName) => {
    if (cursors.length === 0) {
        return;
    }
    console.log(`Merging ${cursors.length} cursors for ${tagName}`);
    for (const data of kWayMerge(cursors, frameParseFunction)) {
        console.log(data);
    }
    console.log("")
});
console.timeEnd("Merge Operation");

//1.813ms
console.time("Close Operation");
await gridScale[Symbol.asyncDispose]();
console.timeEnd("Close Operation");