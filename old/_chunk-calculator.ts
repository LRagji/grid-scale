// import { createHash } from "node:crypto";
// import { IChunkId } from "./chunk.js";

// export type ChunkCalculator = (tagName: string, time: number, tagBucketWidth: number, timeBucketWidth: number) => IChunkId;

// function MD5Calculator(tagName: string, time: number, tagBucketWidth: number, timeBucketWidth: number): IChunkId {
//     const hashAlgorithm = 'md5';
//     const hashBuffer = createHash(hashAlgorithm)
//         .update(tagName)
//         .digest();
//     const bucket1 = hashBuffer.readInt32LE(0) - (hashBuffer.readInt32LE(0) % tagBucketWidth);
//     const bucket2 = hashBuffer.readInt32LE(4) - (hashBuffer.readInt32LE(4) % tagBucketWidth);
//     const bucket3 = hashBuffer.readInt32LE(8) - (hashBuffer.readInt32LE(8) % tagBucketWidth);
//     const bucket4 = hashBuffer.readInt32LE(12) - (hashBuffer.readInt32LE(12) % tagBucketWidth);

//     return {
//         tagHash: hashBuffer,
//         tagPart: `${bucket1}${bucket2}${bucket3}${bucket4}`,
//         timePart: time - (time % timeBucketWidth),
//         limitIndex: function (limits: number): number {
//             let bucket1 = (this.tagHash.readInt32LE(0) % limits);
//             let bucket2 = (this.tagHash.readInt32LE(4) % limits);
//             let bucket3 = (this.tagHash.readInt32LE(8) % limits);
//             let bucket4 = (this.tagHash.readInt32LE(12) % limits);
//             return Math.abs(bucket1 + bucket2 + bucket3 + bucket4) % limits;
//         },
//         logicalChunkId: function (prefix: string, sep: string) {
//             return `${prefix}${sep}${this.tagPart}${sep}${this.timePart}`;
//         }
//     }
// }

// function DJB2Calculator(tagName: string, time: number, tagBucketWidth: number, timeBucketWidth: number): IChunkId {
//     const tHash = djb2Hash(tagName);
//     const tagPart = (tHash - (tHash % tagBucketWidth)).toString();
//     const timePart = time - (time % timeBucketWidth);

//     return {
//         tagHash: new Uint8Array(0) as Buffer,
//         tagPart,
//         timePart,
//         limitIndex: function (limits: number) { return (parseInt(this.tagPart)) % limits },
//         logicalChunkId: function (prefix: string, sep: string) { return `${prefix}${sep}${this.tagPart}${sep}${this.timePart}` }
//     };
// }

// function djb2Hash(str: string): number {
//     let hash = 5381;
//     for (let i = 0; i < str.length; i++) {
//         hash = ((hash << 5) + hash) + str.charCodeAt(i); // hash * 33 + c
//     }
//     return hash >>> 0; // Ensure the hash is a positive integer
// }


// export const Calculators: ChunkCalculator[] = [
//     MD5Calculator,
//     DJB2Calculator
// ]