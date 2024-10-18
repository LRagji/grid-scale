export type DistributedIterationPlan = {
    //[[ConnectionPath][TagNames]]
    chunkReads: [Set<string>, Set<string>][],
    planEndTime: number,
    planStartTime: number,
    requestedStartTime: number,
    requestedEndTime: number
}