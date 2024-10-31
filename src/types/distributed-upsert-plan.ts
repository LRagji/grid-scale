export type DistributedUpsertPlan = {
    //[ConnectionPath, TagName, Records[]][]
    chunkAllocations: [string, Map<string, any[]>][][],
    //[ChunkID, [mvccSortBucket, RelatedChunkIDs]
    chunkDisplacements: Map<string, [number, Set<string>]>
}