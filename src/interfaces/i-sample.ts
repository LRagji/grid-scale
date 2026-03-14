export interface ISample {
    tag: string,
    ts: number, //UTC_EPOCH_MS
    pld: {
        nV: number //Mandatory
    }
}

export interface IScoredSample extends ISample {
    pageRank: number,
    writeScore: number
}

export interface ISampleSet {
    tag: string,
    samples: ISample[]
    minTs: number,
    maxTs: number,
    count: number
}