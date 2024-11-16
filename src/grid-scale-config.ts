import { randomUUID } from "node:crypto";

export class GridScaleConfig {

    public identity: string = randomUUID();
    public workerCount = 10;
    private tagBucketWidth = 2 ** 9;//512;
    private timeBucketWidth = (2 ** 26) * 1000;//Close to 1 Day in milliseconds
    public logicalChunkPrefix = "l";
    public logicalChunkSeparator = "-";
    public timeBucketTolerance = 1;
    public shardSets = new Map<string, string[]>([["./data/high-speed-1", ["disk1", "disk2", "disk3", "disk4", "disk5"]]]);
    public writerActiveShard = this.shardSets.keys().next().value;

    public set TagBucketWidth(tagBucketWidth: number) {
        // Check if tagBucketWidth is a power of 2
        if (tagBucketWidth <= 0 || (tagBucketWidth & (tagBucketWidth - 1)) !== 0) {
            throw new Error("'tagBucketWidth' must be a power of 2 and greater than 0");
        }
        this.tagBucketWidth = tagBucketWidth;
    }

    public get TagBucketWidth(): number {
        return this.tagBucketWidth;
    }

    public set TimeBucketWidth(timeBucketWidth: number) {
        // Check if timeBucketWidth is a power of 2
        if (timeBucketWidth <= 0 || (timeBucketWidth & (timeBucketWidth - 1)) !== 0) {
            throw new Error("'timeBucketWidth' must be a power of 2 and greater than 0");
        }
        this.timeBucketWidth = timeBucketWidth;
    }

    public get TimeBucketWidth(): number {
        return this.timeBucketWidth;
    }
}