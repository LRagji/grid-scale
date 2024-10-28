export type DistributedIterationPlan = {
    //[[ConnectionPath][TagNames]]
    affinityDistributedChunkReads: [Set<string>, Set<string>][][],
    planEndTime: number,
    planStartTime: number,
    requestedStartTime: number,
    requestedEndTime: number
}