import { join } from "node:path";
import { bigIntMax, bigIntMin, bucket, logicalChunkId, DJB2StringToNumber, bucketInt } from "./chunk-math.js";
import { INonVolatileHashMap } from "./non-volatile-hash-map/i-non-volatile-hash-map.js";
import { DistributedIterationPlan } from "./types/distributed-iteration-plan.js";
import { DistributedUpsertPlan } from "./types/distributed-upsert-plan.js";

export class ChunkPlanner {

    constructor(
        private readonly chunkLinkRegistry: INonVolatileHashMap,
        private readonly tagBucketWidth: number,
        private readonly timeBucketWidth: number,
        private readonly logicalChunkPrefix: string,
        private readonly logicalChunkSeparator: string,
        private readonly timeBucketTolerance,
        private readonly writersShardPath: string,
        private readonly shardSets: Map<string, string[]>) { }

    public planUpserts(recordSet: Map<bigint, any[]>, recordSize: number, timeIndex: number, insertTimeIndex: number, insertTime: number, distributionCardinality: number): DistributedUpsertPlan {
        //Plan AIM:Intention of the plan is to touch one chunk at a time with all writes included so that we reduce data fragmentation and IOPS
        const computedPlan = { chunkAllocations: new Map<string, Map<string, any[]>>(), chunkDisplacements: new Map<string, [string, Set<string>]>() };
        const timeBucketWidthBigInt = BigInt(this.timeBucketWidth);
        const toleranceWidth = BigInt(this.timeBucketWidth * this.timeBucketTolerance);
        const insertTimeBucketed = bucket(BigInt(insertTime), timeBucketWidthBigInt);
        const minimumToleranceByInsertTime = insertTimeBucketed - toleranceWidth;
        const maximumToleranceByInsertTime = insertTimeBucketed + toleranceWidth;
        const shardsLength = BigInt(this.shardSets.get(this.writersShardPath).length);
        for (const [tagId, records] of recordSet) {
            const tagBucketed = bucket(tagId, BigInt(this.tagBucketWidth));
            const logicalChunkIdByInsertTime = logicalChunkId([tagBucketed, insertTimeBucketed], this.logicalChunkPrefix, this.logicalChunkSeparator);
            const diskIndex = tagBucketed % shardsLength;
            const connectionPath = join(this.writersShardPath, this.shardSets.get(this.writersShardPath)[Number(diskIndex)], logicalChunkIdByInsertTime);
            const tagNameRecordSetMap = computedPlan.chunkAllocations.get(connectionPath) || new Map<string, any[]>();
            let minimumByRecordTime = BigInt(Number.MAX_SAFE_INTEGER);
            let maximumByRecordTime = BigInt(Number.MIN_SAFE_INTEGER);
            const existingRows = tagNameRecordSetMap.get(tagId.toString()) || new Array<any>();
            for (let recordIndex = 0; recordIndex < records.length; recordIndex += recordSize) {
                //Chunk Allocations
                const record = records.slice(recordIndex, recordIndex + recordSize);
                record[insertTimeIndex] = insertTime;
                existingRows.push(record);
                //Chunk Misplacement's
                const recordTime = BigInt(record[timeIndex]);
                if (recordTime < minimumByRecordTime || recordTime > maximumByRecordTime) {
                    const recordTimeBucketed = bucket(BigInt(recordTime), timeBucketWidthBigInt);
                    const logicalChunkIdByRecordTime = logicalChunkId([tagBucketed, recordTimeBucketed], this.logicalChunkPrefix, this.logicalChunkSeparator);
                    if (recordTimeBucketed < minimumToleranceByInsertTime || recordTimeBucketed > maximumToleranceByInsertTime) {
                        const displacements = computedPlan.chunkDisplacements.get(logicalChunkIdByRecordTime) || [insertTimeBucketed.toString(), new Set<string>()];
                        displacements[1].add(logicalChunkIdByInsertTime);
                        computedPlan.chunkDisplacements.set(logicalChunkIdByRecordTime, displacements);
                        minimumByRecordTime = bigIntMin(minimumByRecordTime, recordTimeBucketed);
                        maximumByRecordTime = bigIntMax(maximumByRecordTime, recordTimeBucketed + timeBucketWidthBigInt);
                    }
                }
            }
            tagNameRecordSetMap.set(tagId.toString(), existingRows);
            computedPlan.chunkAllocations.set(connectionPath, tagNameRecordSetMap);
        }
        //Affinity
        const affinityDistribution = new Array<[string, Map<string, any[]>][]>(distributionCardinality);
        for (const [connectionPath, rows] of computedPlan.chunkAllocations) {
            const index = DJB2StringToNumber(connectionPath) % affinityDistribution.length;
            const existingPlans = affinityDistribution[index] || [];
            existingPlans.push([connectionPath, rows]);
            affinityDistribution[index] = existingPlans;
        }

        return {
            chunkAllocations: affinityDistribution,
            chunkDisplacements: computedPlan.chunkDisplacements
        };
    }

    public async planRangeIterationByTime(tags: bigint[], startInclusiveTime: number, endExclusiveTime: number, distributionCardinality: number): Promise<DistributedIterationPlan> {
        //Aim: Give a complete horizontal for chunk to be read by one thread for MVCC & Fragmentation reduction via K-Way merge also to reduce IOPS
        if (bucketInt(startInclusiveTime, this.timeBucketWidth) !== bucketInt(endExclusiveTime - 1, this.timeBucketWidth)) {
            throw new Error(`Range iteration must be within the same time bucket[${startInclusiveTime},${endExclusiveTime}]`);
        }
        const startInclusiveBucketedTime = bucket(BigInt(startInclusiveTime), BigInt(this.timeBucketWidth));
        const tagsGroupedByLogicalChunkId = new Map<string, [Set<string>, Set<string>]>();
        const logicalIdCache = new Map<string, Set<string>>();

        for (const tag of tags) {
            const tagBucketed = bucket(tag, BigInt(this.tagBucketWidth));
            const chunkIdByRecordTime = logicalChunkId([tagBucketed, startInclusiveBucketedTime], this.logicalChunkPrefix, this.logicalChunkSeparator);

            if (logicalIdCache.has(chunkIdByRecordTime) === false) {
                const toleranceChunks = new Array<string>();
                for (let i = 0; i < this.timeBucketTolerance; i++) {
                    const LHSToleranceChunkId = logicalChunkId([tagBucketed, startInclusiveBucketedTime - BigInt(this.timeBucketWidth * i)], this.logicalChunkPrefix, this.logicalChunkSeparator);
                    const RHSToleranceChunkId = logicalChunkId([tagBucketed, startInclusiveBucketedTime + BigInt(this.timeBucketWidth * i)], this.logicalChunkPrefix, this.logicalChunkSeparator);
                    toleranceChunks.push(LHSToleranceChunkId, RHSToleranceChunkId);
                }
                const displacedChunks = await this.chunkLinkRegistry.getFields(chunkIdByRecordTime);
                logicalIdCache.set(chunkIdByRecordTime, new Set<string>([...displacedChunks, chunkIdByRecordTime, ...toleranceChunks]));
            }

            const existingTagsAndConnectionPaths = tagsGroupedByLogicalChunkId.get(chunkIdByRecordTime) || [new Set<string>(), new Set<string>()];
            for (const [setPath, diskPaths] of this.shardSets) {
                const diskIndex = tagBucketed % BigInt(diskPaths.length);
                for (const logicalId of logicalIdCache.get(chunkIdByRecordTime)) {
                    const connectionPath = join(setPath, diskPaths[Number(diskIndex)], logicalId);
                    existingTagsAndConnectionPaths[0].add(connectionPath);
                }
                existingTagsAndConnectionPaths[1].add(tag.toString());
            }
            tagsGroupedByLogicalChunkId.set(chunkIdByRecordTime, existingTagsAndConnectionPaths);
        }
        //Affinity
        const affinityDistribution = new Array<[Set<string>, Set<string>][]>(distributionCardinality);
        for (const [logicalId, connectionPathsAndTags] of tagsGroupedByLogicalChunkId) {
            const index = DJB2StringToNumber(logicalId) % affinityDistribution.length;
            const existingPlans = affinityDistribution[index] || [];
            existingPlans.push(connectionPathsAndTags);
            affinityDistribution[index] = existingPlans;
        }
        return {
            affinityDistributedChunkReads: affinityDistribution,
            planEndTime: Number(startInclusiveBucketedTime) + this.timeBucketWidth,
            planStartTime: Number(startInclusiveBucketedTime),
            requestedStartTime: startInclusiveTime,
            requestedEndTime: endExclusiveTime
        };
    }

    public decomposeTimeByPages(startInclusiveTime: number, endExclusiveTime: number): [number, number][] {

        if (startInclusiveTime >= endExclusiveTime) {
            throw new Error("Invalid time range, end cannot be less than or equal to start");
        }

        if (startInclusiveTime < 0 || endExclusiveTime < 1) {
            throw new Error("Invalid time range, start and end must be positive and greater than 0 & 1 respectively.");
        }

        const result: [number, number][] = [];
        let currentStart = startInclusiveTime;

        while (currentStart < endExclusiveTime) {
            const currentEnd = Math.min(currentStart + this.timeBucketWidth, endExclusiveTime);
            result.push([currentStart, currentEnd]);
            currentStart = currentEnd;
        }

        return result;
    }

    public decomposeTagsByPages(tagList: bigint[]): bigint[][] {
        const tagBucketWidth = BigInt(this.tagBucketWidth);
        const tagBuckets = new Map<bigint, bigint[]>();
        for (const tag of tagList) {
            const tagBucketed = bucket(tag, tagBucketWidth);
            const existingBucket = tagBuckets.get(tagBucketed) ?? new Array<bigint>();
            existingBucket.push(tag);
            tagBuckets.set(tagBucketed, existingBucket);
        }
        return Array.from(tagBuckets.values());
    }

}