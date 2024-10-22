import { StringToNumberAlgos } from './string-to-number-algos.js';

export class ChunkId {
    public readonly logicalChunkId: string;
    public readonly tagNameBucketed: number[];
    public readonly timeBucketed: number[];

    public static bucket(input: number, width: number): number {
        return input - (input % width);
    }

    public static from(tagName: string, time: number, stringToNumberAlgosIndex: number, tagBucketWidth: number, timeBucketWidth: number, logicalChunkPrefix: string, logicalChunkSeperator: string): ChunkId {
        const tagHash = StringToNumberAlgos[stringToNumberAlgosIndex](tagName);
        return new ChunkId(tagHash, [time], tagBucketWidth, timeBucketWidth, logicalChunkPrefix, logicalChunkSeperator);
    }

    public tagNameMod(limit: number): number {
        return Math.abs(this.tagHash.reduce((acc, val) => acc + val, 0)) % limit;
    }

    public timeMod(limit: number): number {
        return Math.abs(this.timeHash.reduce((acc, val) => acc + val, 0)) % limit;
    }

    constructor(private readonly tagHash: number[], private readonly timeHash: number[], tagBucketWidth: number, timeBucketWidth: number, logicalChunkPrefix: string, logicalChunkSeperator: string) {
        this.tagNameBucketed = this.tagHash.map(val => ChunkId.bucket(val, tagBucketWidth));
        this.timeBucketed = this.timeHash.map(val => ChunkId.bucket(val, timeBucketWidth));
        this.logicalChunkId = `${logicalChunkPrefix}${logicalChunkSeperator}${this.tagNameBucketed.join("")}${logicalChunkSeperator}${this.timeBucketed.join("")}`;
    }
}