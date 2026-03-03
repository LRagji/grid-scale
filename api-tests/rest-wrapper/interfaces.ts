export interface IFetchRequest {
    fetch: {
        object: "value" | "full",
        valueAggregateOperator: "sum" | "avg" | "count" | "custom",
        valueAggregateCustomOperator?: string
    },
    tagsFilter: {
        in: string[]
    },
    timeFilter: {
        startInclusiveTime: number,
        endExclusiveTime: number,
        latest: boolean
    },
    valueFilter: {
        type: "between" | "lt" | "gt" | "in",
        lt: number,
        gt: number,
        in: number[]
    },
    payloadFilter: {
        filters: [
            {
                propertyName: "",
                numericFilter: {
                    type: "between" | "lt" | "gt",
                    lt: number,
                    gt: number
                },
                stringFilter: {
                    in: string[],
                    equal: string,
                    contains: string
                }
            }
        ],
        sequence: (number | "&" | "|")[]
    }
}
