export type TConfig = {
    setPaths: Map<string, string[]>,
    activePath: string
    tagBucketWidth: number;
    timeBucketWidth: number;
    fileNamePre: string;
    fileNamePost: string;
    timeBucketTolerance: number;
    activeCalculatorIndex: number;
    maxDBOpen: number;
    logicalChunkPrefix: string;
    logicalChunkSeparator: string;
    redisConnection: string;
}