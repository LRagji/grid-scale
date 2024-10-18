export type DistributedUpsertPlan = {
    //[ConnectionPath, TagName, Records[]]
    chunkAllocations: Map<string, Map<string, any[][]>>,
    //[ChunkID, [mvccSortBucket, RelatedChunkIDs]
    chunkDisplacements: Map<string, [number, Set<string>]>
}