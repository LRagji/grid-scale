import { GridScale, tagName } from "./grid-scale.js";
import { kWayMerge } from "./k-way-merge.js";
import { TConfig } from "./t-config.js";
import { CommonConfig, generateTagNames } from "./utils.js";

//Query Plan
//Read
//Merge

const config: TConfig = CommonConfig();
const gridScale = new GridScale(config);
const totalTags = 50000;
const startInclusiveTime = 0;//Date.now();
const endExclusiveTime = startInclusiveTime + config.timeBucketWidth;

console.time("Generate Operation");
const tagNames = generateTagNames(totalTags);
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
    let purgeIndexes = [];
    let yieldIndex = -1;
    elements.forEach((element, index) => {
        if (element == null || Array.isArray(element) === false || (Array.isArray(element) && element.length === 0)) {
            purgeIndexes.push(index);
        }
        if (index === 0) {
            yieldIndex = index;
        }
        else {
            //TagName need to be compared
            if (element[4] === elements[yieldIndex][4] && element[0] < elements[yieldIndex][0]) {
                yieldIndex = index;
            }
            else if (element[4] === elements[yieldIndex][4] && element[0] === elements[yieldIndex][0]) {
                //Compare Insert time in descending order MVCC
                if (elements[1] > elements[yieldIndex][1]) {
                    purgeIndexes.push(yieldIndex);
                    yieldIndex = index;
                }
                else {
                    purgeIndexes.push(index);
                }
            }
        }

    });
    return { yieldIndex, purgeIndexes };
};
resultCursor.forEach((cursors, tagName) => {
    if (cursors.length === 0) {
        return;
    }
    const details = Array.from(kWayMerge(cursors, frameParseFunction));
    // console.log(`Merging ${cursors.length} cursors for ${tagName} Data Length: ${details.length}`);

    // for (const data of kWayMerge(cursors, frameParseFunction)) {
    //      console.log(data);
    // }
    //console.log("")
});
console.timeEnd("Merge Operation");

//1.813ms
console.time("Close Operation");
await gridScale[Symbol.asyncDispose]();
console.timeEnd("Close Operation");