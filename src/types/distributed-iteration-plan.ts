export type DistributedIterationPlan = {
    //[[ConnectionPaths] [TagNames],[start, end],lastWriteMetadata]
    affinityDistributedChunkReads: [Set<string>, Set<string>, [number, number], number][][],
    requestedStartTime: number,
    requestedEndTime: number
}