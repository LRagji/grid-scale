import { Serializable } from "node-apparatus";

export type AggregateLambda<T> = (first: boolean, last: boolean, inputPage: any[][] | null, accumulator: T | undefined) => { yield: boolean, yieldValue: any, accumulator: any };
export type WindowLambda = (page: Serializable[][]) => Serializable[];

export class IteratorLambdaConfig<T> {
    public windowLambdaPath: URL | undefined = undefined;
    public aggregateLambda: AggregateLambda<T> | undefined = undefined;
    public accumulator: T | undefined = undefined;
}