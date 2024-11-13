import { randomUUID } from "node:crypto";

export class GridScaleConfig {

    public identity: string = randomUUID();
    public workerCount = 10;
    public tagBucketWidth = 500;
    public timeBucketWidth = 86400 * 1000;
    public logicalChunkPrefix = "l";
    public logicalChunkSeparator = "-";
    public timeBucketTolerance = 1;
    public shardSets = new Map<string, string[]>([["./data/high-speed-1", ["disk1", "disk2", "disk3", "disk4", "disk5"]]]);
    public writerActiveShard = this.shardSets.keys().next().value;
}