import { join } from "node:path";
import { ChunkId } from "./chunk-id.js";
import { INonVolatileHashMap } from "./non-volatile-hash-map/i-non-volatile-hash-map.js";
import { TConfig } from "./t-config.js";
import { DistributedIterationPlan } from "./types/distributed-iteration-plan.js";
import { DistributedUpsertPlan } from "./types/distributed-upsert-plan.js";

export class ChunkPlanner {

    constructor(private readonly chunkLinkRegistry: INonVolatileHashMap, private readonly config: TConfig) { }

    public planUpserts(recordSet: Map<string, any[]>, recordSize: number, timeIndex: number, insertTime: number): DistributedUpsertPlan {
        //Plan AIM:Intention of the plan is to touch one chunk at a time with all writes included so that we reduce data fragmentation and IOPS
        const returnPlan: DistributedUpsertPlan = { chunkAllocations: new Map<string, Map<string, any[]>>(), chunkDisplacements: new Map<string, [number, Set<string>]>() };
        for (const [tagName, records] of recordSet) {
            const chunkIdByInsertTime = ChunkId.from(tagName, insertTime, this.config);
            const diskIndex = chunkIdByInsertTime.tagNameMod(this.config.setPaths.get(this.config.activePath).length);
            const connectionPath = join(this.config.activePath, this.config.setPaths.get(this.config.activePath)[diskIndex], chunkIdByInsertTime.logicalChunkId);
            const tagNameRecordSetMap = returnPlan.chunkAllocations.get(connectionPath) || new Map<string, any[]>();
            for (let recordIndex = 0; recordIndex < records.length; recordIndex += recordSize) {
                //Chunk Allocations
                const record = records.slice(recordIndex, recordIndex + recordSize);
                const existingRows = tagNameRecordSetMap.get(tagName) || new Array<any>();
                existingRows.push(record);
                tagNameRecordSetMap.set(tagName, existingRows);
                //Chunk Misplacement's
                const chunkIdByRecordTime = ChunkId.from(tagName, record[timeIndex], this.config);
                const toleranceWidth = this.config.timeBucketWidth * this.config.timeBucketTolerance;
                const minimumTolerance = chunkIdByInsertTime.timeBucketed[0] - toleranceWidth;
                const maximumTolerance = chunkIdByInsertTime.timeBucketed[0] + toleranceWidth;
                if (chunkIdByRecordTime.timeBucketed[0] < minimumTolerance || chunkIdByRecordTime.timeBucketed[0] > maximumTolerance) {
                    const displacements = returnPlan.chunkDisplacements.get(chunkIdByRecordTime.logicalChunkId) || [chunkIdByInsertTime.timeBucketed[0], new Set<string>()];
                    displacements[1].add(chunkIdByInsertTime.logicalChunkId);
                    returnPlan.chunkDisplacements.set(chunkIdByRecordTime.logicalChunkId, displacements);
                }
            }
            returnPlan.chunkAllocations.set(connectionPath, tagNameRecordSetMap);
        }
        return returnPlan;
    }

    public async planRangeIterationByTime(tags: string[], startInclusiveTime: number, endExclusiveTime: number): Promise<DistributedIterationPlan> {
        //Aim: Give a complete horizontal for chunk to be read by one thread for MVCC & Fragmentation reduction via K-Way merge also to reduce IOPS
        const startInclusiveBucketedTime = ChunkId.bucket(startInclusiveTime, this.config.timeBucketWidth);
        const tagsGroupedByLogicalChunkId = new Map<string, [Set<string>, Set<string>]>();
        const logicalIdCache = new Map<string, Set<string>>();

        for (const tag of tags) {
            const chunkIdByRecordTime = ChunkId.from(tag, startInclusiveBucketedTime, this.config);

            if (logicalIdCache.has(chunkIdByRecordTime.logicalChunkId) === false) {
                const LHSToleranceChunkId = ChunkId.from(tag, startInclusiveBucketedTime - (this.config.timeBucketWidth * this.config.timeBucketTolerance), this.config);
                const RHSToleranceChunkId = ChunkId.from(tag, startInclusiveBucketedTime + (this.config.timeBucketWidth * this.config.timeBucketTolerance), this.config);
                const displacedChunks = await this.chunkLinkRegistry.getFields(chunkIdByRecordTime.logicalChunkId);
                logicalIdCache.set(chunkIdByRecordTime.logicalChunkId, new Set<string>([...displacedChunks, chunkIdByRecordTime.logicalChunkId, LHSToleranceChunkId.logicalChunkId, RHSToleranceChunkId.logicalChunkId]));
            }

            const existingTagsAndConnectionPaths = tagsGroupedByLogicalChunkId.get(chunkIdByRecordTime.logicalChunkId) || [new Set<string>(), new Set<string>()];
            for (const [setPath, diskPaths] of this.config.setPaths) {
                const diskIndex = chunkIdByRecordTime.tagNameMod(diskPaths.length);
                for (const logicalId of logicalIdCache.get(chunkIdByRecordTime.logicalChunkId)) {
                    const connectionPath = join(setPath, diskPaths[diskIndex], logicalId);
                    existingTagsAndConnectionPaths[0].add(connectionPath);
                }
                existingTagsAndConnectionPaths[1].add(tag);
            }
            tagsGroupedByLogicalChunkId.set(chunkIdByRecordTime.logicalChunkId, existingTagsAndConnectionPaths);

        }
        return {
            chunkReads: Array.from(tagsGroupedByLogicalChunkId.values()),
            planEndTime: startInclusiveBucketedTime + this.config.timeBucketWidth,
            planStartTime: startInclusiveBucketedTime,
            requestedStartTime: startInclusiveTime,
            requestedEndTime: endExclusiveTime
        };
    }

}