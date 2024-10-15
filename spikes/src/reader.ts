import { GridScaleBase } from "./grid-scale.js";
import { TConfig } from "./t-config.js";
import { CommonConfig, generateTagNames } from "./utils.js";

//Query Plan
//Read
//Merge
const threads = 10;
console.log(`Started with ${threads} threads`);

const config: TConfig = CommonConfig();
const gridScale = new GridScaleBase<any[]>(config, threads);
const totalTags = 50000;
const startInclusiveTime = 0;//Date.now();
const endExclusiveTime = 1//startInclusiveTime + config.timeBucketWidth;

const tagNames = generateTagNames(totalTags, 1);

console.time("Total")
const resultTagNames = new Set<string>();
const diagnostics = new Map<string, number>();
const pageCursor = gridScale.read(tagNames, startInclusiveTime, endExclusiveTime, diagnostics);
for await (const tagCursor of pageCursor) {// Iterates through time pages
    for await (const dataRow of tagCursor) { //Iterates through logical chunk pages.
        resultTagNames.add(dataRow[4]);
    }
    //break;
}
console.log(`Total Tags: ${resultTagNames.size}`);
console.timeEnd("Total")
for (const [key, value] of diagnostics) {
    console.log(`${key} ${value}`);
}

console.time("Close Operation");
await gridScale[Symbol.asyncDispose]();
console.timeEnd("Close Operation");