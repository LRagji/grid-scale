import { GridScaleBase } from "./grid-scale.js";
import { TConfig } from "./t-config.js";
import { CommonConfig, generateTagNames } from "./utils.js";

//Query Plan
//Read
//Merge

const config: TConfig = CommonConfig();
const gridScale = new GridScaleBase<any[]>(config, 0);
const totalTags = 50000;
const startInclusiveTime = 0;//Date.now();
const endExclusiveTime = 1//startInclusiveTime + config.timeBucketWidth;

console.time("Generate Operation");
const tagNames = generateTagNames(totalTags);
console.timeEnd("Generate Operation");

console.time("Total")
const diagnostics = new Map<string, number>();
const pageCursor = gridScale.read(tagNames, startInclusiveTime, endExclusiveTime, diagnostics);
for await (const tagCursor of pageCursor) {// Iterates through time pages
    for (const dataRow of tagCursor) { //Iterates through logical chunk pages.
        console.log(dataRow);
    }
    break;
}
console.timeEnd("Total")
for (const [key, value] of diagnostics) {
    console.log(`${key} ${value}`);
}

console.time("Close Operation");
await gridScale[Symbol.asyncDispose]();
console.timeEnd("Close Operation");