import { join } from "node:path";
import { bigIntMax, bigIntMin, bucket, logicalChunkId, DJB2StringToNumber } from "./chunk-math.js";
import { INonVolatileHashMap } from "./non-volatile-hash-map/i-non-volatile-hash-map.js";
import { DistributedIterationPlan } from "./types/distributed-iteration-plan.js";
import { DistributedUpsertPlan } from "./types/distributed-upsert-plan.js";
import { IChunkMetadata } from "./chunk-metadata/i-chunk-metadata.js";
import { metaKeyBirth, metaKeyLastWrite } from "./types/meta-keys.js";

export class ChunkPlanner {

    constructor(
        private readonly chunkLinkRegistry: INonVolatileHashMap,
        private readonly tagBucketWidth: number,
        private readonly timeBucketWidth: number,
        private readonly logicalChunkPrefix: string,
        private readonly logicalChunkSeparator: string,
        private readonly timeBucketTolerance,
        private readonly writersShardPath: string,
        private readonly shardSets: Map<string, string[]>,
        private readonly chunkMetaRegistry: IChunkMetadata) { }

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

    public async planRange(tags: bigint[], startInclusiveTime: number, endExclusiveTime: number, timeStepSize: number, tagStepSize: number, distributionCardinality: number, affinityBasedDistribution = true, timeToLive = Number.MAX_SAFE_INTEGER): Promise<DistributedIterationPlan> {
        const batchedTags = this.decomposeByTagPages(tags, tagStepSize);
        const batchedTimeRanges = this.decomposeByTimePages(startInclusiveTime, endExclusiveTime, timeStepSize);
        const tagBucketWidth = BigInt(this.tagBucketWidth);
        const timeBucketWidth = BigInt(this.timeBucketWidth);
        const affinityDistribution = new Array<[Set<string>, Set<string>, [number, number], number][]>(distributionCardinality);
        const defaultValueString = "-1;"
        let countChunks = 0;

        //Plan for reading
        const processedLogicalIds = new Map<string, [Set<string>, number, number]>();
        for (const timeRange of batchedTimeRanges) {
            const startInclusiveBucketedTime = bucket(BigInt(timeRange[0]), timeBucketWidth);
            for (const tagBatch of batchedTags) {
                const representativeTag = tagBatch[0];
                const tagBucketed = bucket(representativeTag, tagBucketWidth);
                const chunkIdByRecordTime = logicalChunkId([tagBucketed, startInclusiveBucketedTime], this.logicalChunkPrefix, this.logicalChunkSeparator);

                //Collect all self & related chunks
                if (processedLogicalIds.has(chunkIdByRecordTime) === false) {
                    const toleranceChunks = new Array<string>();
                    for (let i = 1; i <= this.timeBucketTolerance; i++) {
                        const LHSToleranceChunkId = logicalChunkId([tagBucketed, startInclusiveBucketedTime - BigInt(this.timeBucketWidth * i)], this.logicalChunkPrefix, this.logicalChunkSeparator);
                        const RHSToleranceChunkId = logicalChunkId([tagBucketed, startInclusiveBucketedTime + BigInt(this.timeBucketWidth * i)], this.logicalChunkPrefix, this.logicalChunkSeparator);
                        toleranceChunks.push(LHSToleranceChunkId, RHSToleranceChunkId);
                    }
                    const displacedChunks = await this.chunkLinkRegistry.getFields(chunkIdByRecordTime);
                    const allChunkIds = new Set<string>([...displacedChunks, chunkIdByRecordTime, ...toleranceChunks]);

                    //Construct set paths
                    const setPaths = new Array<string>();
                    for (const [setPath, diskPaths] of this.shardSets) {
                        const diskIndex = tagBucketed % BigInt(diskPaths.length);
                        const connectionPath = join(setPath, diskPaths[Number(diskIndex)]);
                        setPaths.push(connectionPath);
                    }

                    //Join connection paths with logical ids
                    const connectionPaths = new Set<string>();
                    for (const logicalId of allChunkIds) {
                        for (const setPath of setPaths) {
                            const connectionPath = join(setPath, logicalId);
                            connectionPaths.add(connectionPath);
                        }
                    }
                    setPaths.length = 0;
                    allChunkIds.clear();

                    //Prune for TTL and for unknown last write chunks
                    let connectionPathsArray = Array.from(connectionPaths.keys());
                    const defaultValueInt = parseInt(defaultValueString, 10);
                    const metaBirthFetch = await this.chunkMetaRegistry.metadataGet(connectionPathsArray, metaKeyBirth, defaultValueString);
                    for (const [connectionPath, metaValues] of metaBirthFetch) {
                        const birthDate = metaValues.reduce((acc: number, val: string | null) => Math.max(acc, parseInt(val ?? defaultValueString, 10)), Number.MIN_SAFE_INTEGER);
                        const age = Date.now() - birthDate;
                        if (age > timeToLive || birthDate === defaultValueInt) {
                            connectionPaths.delete(connectionPath);
                        }
                    }
                    connectionPathsArray = Array.from(connectionPaths.keys());


                    //Add info for last write
                    const metaLastWriteFetch = await this.chunkMetaRegistry.metadataGet(connectionPathsArray, metaKeyLastWrite, defaultValueString);
                    let lastWrite = parseInt(defaultValueString, 10);
                    for (const [connectionPath, metaValues] of metaLastWriteFetch) {
                        const internalLastWrite = metaValues.reduce((acc: number, val: string | null) => Math.max(acc, parseInt(val ?? defaultValueString, 10)), Number.MIN_SAFE_INTEGER);
                        lastWrite = Math.max(lastWrite, internalLastWrite);
                    }

                    //Affinity
                    const workerIndex = (affinityBasedDistribution === true ? DJB2StringToNumber(chunkIdByRecordTime) : countChunks) % affinityDistribution.length;

                    processedLogicalIds.set(chunkIdByRecordTime, [connectionPaths, lastWrite, workerIndex]);
                }

                const [connectionPaths, lastWrite, workerIndex] = processedLogicalIds.get(chunkIdByRecordTime);

                const existingPlans = affinityDistribution[workerIndex] ?? [];
                const tagsStringSet = new Set<string>(tagBatch.map(tag => tag.toString()));
                existingPlans.push([connectionPaths, tagsStringSet, timeRange, lastWrite]);
                affinityDistribution[workerIndex] = existingPlans;
                countChunks++;

            }
        }


        return {
            affinityDistributedChunkReads: affinityDistribution,
            requestedStartTime: startInclusiveTime,
            requestedEndTime: endExclusiveTime
        };
    }

    public decomposeByTimePages(startInclusiveTime: number, endExclusiveTime: number, stepSize: number = this.timeBucketWidth): [number, number][] {

        if (startInclusiveTime >= endExclusiveTime) {
            throw new Error("Invalid time range, end cannot be less than or equal to start");
        }

        if (startInclusiveTime < 0 || endExclusiveTime < 1) {
            throw new Error("Invalid time range, start and end must be positive and greater than 0 & 1 respectively.");
        }

        if (stepSize <= 1 || (stepSize & (stepSize - 1)) !== 0) {
            throw new Error("'stepSize' must be a power of 2 and greater than 1");
        }

        stepSize = Math.min(stepSize, this.timeBucketWidth);// You can't have a step size greater than the bucket width

        const result: [number, number][] = [];
        let currentStart = startInclusiveTime;

        while (currentStart < endExclusiveTime) {
            const currentEnd = Math.min(currentStart + stepSize, endExclusiveTime);
            result.push([currentStart, currentEnd]);
            currentStart = currentEnd;
        }

        return result;
    }

    public decomposeByTagPages(tagList: bigint[], stepSize = this.tagBucketWidth): bigint[][] {
        if (stepSize <= 1 || (stepSize & (stepSize - 1)) !== 0) {
            throw new Error("'stepSize' must be a power of 2 and greater than 1");
            //This has to be a power of 2 and smaller than the bucket width for correct windows to align within the bucket width.
        }

        stepSize = Math.min(stepSize, this.tagBucketWidth);// You can't have a step size greater than the bucket width

        const tagBuckets = new Map<bigint, bigint[]>();
        const stepSizeBigint = BigInt(stepSize);

        for (const tag of tagList) {
            const tagBucketed = bucket(tag, stepSizeBigint);
            const existingBucket = tagBuckets.get(tagBucketed) ?? new Array<bigint>();
            existingBucket.push(tag);
            tagBuckets.set(tagBucketed, existingBucket);
        }

        return Array.from(tagBuckets.values());
    }

    public singleTimeWiseStepDirector(timePages: [number, number][], tagPages: bigint[][], previousTimeStep: [number, number] | undefined, previousTagStep: bigint[] | undefined) {
        let timeStepIndex = timePages.indexOf(previousTimeStep);
        let tagStepIndex = tagPages.indexOf(previousTagStep);
        if (timeStepIndex === -1 || tagStepIndex === -1) {
            return { nextTimeStep: timePages[0], nextTagStep: tagPages[0] };
        }
        timeStepIndex++;
        if (timeStepIndex >= timePages.length) {
            timeStepIndex = 0;
            tagStepIndex++;
            if (tagStepIndex >= tagPages.length) {
                return { nextTimeStep: undefined, nextTagStep: undefined };
            }
        }
        return { nextTimeStep: timePages[timeStepIndex], nextTagStep: tagPages[tagStepIndex] };
    }

}