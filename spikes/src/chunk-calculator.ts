import { createHash } from "node:crypto";

export interface IChunkId {
    tagHash: Buffer;
    tagPart: string;
    timePart: number;
    limitIndex: (limits: number) => number;
    logicalChunkId: (prefix: string, sep: string) => string;
}

export function MD5Calculator(tagName: string, time: number, tagBucketWidth: number, timeBucketWidth: number): IChunkId {
    const hashAlgorithm = 'md5';
    const hashBuffer = createHash(hashAlgorithm)
        .update(tagName)
        .digest();
    const bucket1 = hashBuffer.readInt32LE(0) - (hashBuffer.readInt32LE(0) % tagBucketWidth);
    const bucket2 = hashBuffer.readInt32LE(4) - (hashBuffer.readInt32LE(4) % tagBucketWidth);
    const bucket3 = hashBuffer.readInt32LE(8) - (hashBuffer.readInt32LE(8) % tagBucketWidth);
    const bucket4 = hashBuffer.readInt32LE(12) - (hashBuffer.readInt32LE(12) % tagBucketWidth);

    return {
        tagHash: hashBuffer,
        tagPart: `${bucket1}${bucket2}${bucket3}${bucket4}`,
        timePart: time - (time % timeBucketWidth),
        limitIndex: function (limits: number): number {
            let bucket1 = (this.tagHash.readInt32LE(0) % limits);
            let bucket2 = (this.tagHash.readInt32LE(4) % limits);
            let bucket3 = (this.tagHash.readInt32LE(8) % limits);
            let bucket4 = (this.tagHash.readInt32LE(12) % limits);
            return Math.abs(bucket1 + bucket2 + bucket3 + bucket4) % limits;
        },
        logicalChunkId: function (prefix: string, sep: string) {
            return `${prefix}${sep}${this.tagPart}${sep}${this.timePart}`;
        }
    }
}