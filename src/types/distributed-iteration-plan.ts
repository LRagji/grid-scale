export type DistributedIterationPlan = {
    //[[ConnectionPaths] [TagNames],[start, end]]
    affinityDistributedChunkReads: [Set<string>, Set<string>, [number, number]][][],
    requestedStartTime: number,
    requestedEndTime: number
}