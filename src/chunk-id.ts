export class ChunkId {
    public readonly logicalChunkId: string;
    private readonly tagNameBucketed: number[];
    public readonly timeBucketed: number[];

    public static bucket(input: number, width: number): number {
        return input - (input % width);
    }

    public static unCompressWithinLimits(input: number[], limit: number): number[] {
        return input.map(val => ChunkId.bucket(val, limit));
    }

    public static compressWithinLimits(input: number[], limit: number): number {
        const sum = ChunkId.sumArray(input);
        return Math.abs(sum) % limit;
    }

    private static sumArray(input: number[]): number {
        return input.reduce((acc, val) => acc + val, 0);
    }

    public static from(tagName: string, time: number, stringToNumber: (string) => number[], tagBucketWidth: number, timeBucketWidth: number, logicalChunkPrefix: string, logicalChunkSeparator: string): ChunkId {
        const tagHash = stringToNumber(tagName);
        return new ChunkId(tagHash, [time], tagBucketWidth, timeBucketWidth, logicalChunkPrefix, logicalChunkSeparator);
    }

    public tagCompressWithinLimits(limit: number): number {
        return ChunkId.compressWithinLimits(this.tagHash, limit);
    }

    public timeCompressWithinLimits(limit: number): number {
        return ChunkId.compressWithinLimits(this.timeHash, limit);
    }

    constructor(private readonly tagHash: number[], private readonly timeHash: number[], tagBucketWidth: number, timeBucketWidth: number, logicalChunkPrefix: string, logicalChunkSeparator: string) {
        this.tagNameBucketed = ChunkId.unCompressWithinLimits(this.tagHash, tagBucketWidth);
        this.timeBucketed = ChunkId.unCompressWithinLimits(this.timeHash, timeBucketWidth);
        this.logicalChunkId = `${logicalChunkPrefix}${logicalChunkSeparator}${this.tagNameBucketed.join("")}${logicalChunkSeparator}${this.timeBucketed.join("")}`;
    }
}