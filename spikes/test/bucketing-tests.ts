// //import sinon from "sinon";
// import assert from "node:assert";
// import { beforeEach, describe, it, afterEach } from "node:test";

// import { bucketString, bucketTime, generateFileName, generateOrFetchIdentity } from "../src/index.js";

// /**
// * Generates a random string of 255 printable characters.
// * @returns A random string of 255 printable characters.
// */
// function generateRandomString(length: number): string {
//     const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_+[]{}|;:,.<>?';
//     let result = '';
//     const charactersLength = characters.length;
//     for (let i = 0; i < length; i++) {
//         result += characters.charAt(Math.floor(Math.random() * charactersLength));
//     }
//     return result;
// }


// describe("bucketing", () => {

//     // afterEach(() => {
//     //     sinon.reset();
//     // })

//     it("Should bucket same string with correct output", () => {
//         const plainString = generateRandomString(255);
//         const bucketWidth = 1000;
//         const bucketRun1 = bucketString(plainString, bucketWidth);
//         const bucketRun2 = bucketString(plainString, bucketWidth);

//         assert.strictEqual(bucketRun1, bucketRun2);
//     });

//     it("Should bucket unique string with correctly", () => {
//         const plainString1 = "OpeerationA/Tag1";
//         const plainString2 = "OpeerationA/Tag2"
//         const bucketWidth = 1000;
//         const bucketRun1 = bucketString(plainString1, bucketWidth);
//         const bucketRun2 = bucketString(plainString2, bucketWidth);

//         assert.notStrictEqual(bucketRun1, bucketRun2);
//     });

//     it("Should bucket same time with correct output", () => {
//         const time = Date.now();
//         const bucketWidth = 1000;
//         const bucketRun1 = bucketTime(time, bucketWidth);
//         const bucketRun2 = bucketTime(time, bucketWidth);

//         assert.strictEqual(bucketRun1, bucketRun2);
//     });

//     it("Should bucket different time with correct output", () => {
//         const time = Date.now();
//         const bucketWidth = 1000;
//         const bucketRun1 = bucketTime(time, bucketWidth);
//         const bucketRun2 = bucketTime(time + bucketWidth + 100, bucketWidth);

//         assert.notStrictEqual(bucketRun1, bucketRun2);
//     });

//     it("Should generate unique string when process memory is empty", () => {
//         const id1 = generateOrFetchIdentity(new Map());
//         const id2 = generateOrFetchIdentity(new Map());

//         assert.notStrictEqual(id1, id2);
//     });

//     it("Should generate and retrieve same identity when process memory is not empty", () => {
//         const processMemory = new Map();
//         const id1 = generateOrFetchIdentity(processMemory);
//         const id2 = generateOrFetchIdentity(processMemory);

//         assert.strictEqual(id1, id2);
//     });

//     it("Should generate same filename with same parameters", () => {
//         const processMemory = new Map();
//         const timeWidth = 100;
//         const tagWidth = 100;

//         const f1 = generateFileName(100, "Tag1", tagWidth, timeWidth, processMemory);
//         const f2 = generateFileName(100, "Tag1", tagWidth, timeWidth, processMemory);

//         assert.strictEqual(f1, f2);
//     });

//     it("Should generate different filename with same parameters", () => {
//         const processMemory = new Map();
//         const timeWidth = 100;
//         const tagWidth = 100;

//         const f1 = generateFileName(100, "Tag1", tagWidth, timeWidth, processMemory);
//         const f2 = generateFileName(100, "Tag2", tagWidth, timeWidth, processMemory);

//         assert.notStrictEqual(f1, f2);
//     });

// });