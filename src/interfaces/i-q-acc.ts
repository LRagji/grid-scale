import { IDimensionalElement } from "./i-dimensional-element.js";
import { IDimensionalQuery } from "./i-dimensional-query.js";

export interface IMetadata {
    [key: string]: string | number | boolean;
}

export interface IPolicy {
    name: string;
    evaluate(meta: IMetadata): Promise<boolean>;
}

export interface IPolicyEvaluator<ActionCallbackType extends (policyMeta: any) => Promise<void>, EvaluationMetaType> {

    initialize(actionCallback: ActionCallbackType): void;

    registerPolicy(policy: IPolicy): void;

    evaluatePolicies(meta: EvaluationMetaType): string[];

}

export interface IQAcc {

    initialize(containerFactory: IQContainerFactory, containerMetadata: IMetadata,
        flushPolicyEvaluator: IPolicyEvaluator<(flushedContainerIds: string[]) => Promise<void>, IMetadata>): Promise<void>;

    accumulate(elements: IDimensionalElement[]): Promise<void>;

    query(query: IDimensionalQuery, maxElementsCount: number): Promise<IDimensionalElement[]>;

}

export interface IQContainer {

    readonly id: string;

    initialize(containerMetadata: IMetadata): Promise<void>;

    accumulate(elements: IDimensionalElement[]): Promise<void>;

    query(query: IDimensionalQuery, maxElementsCount: number): Promise<IDimensionalElement[]>;
}

export interface IQContainerFactory {

    createContainer(containerId: string, containerMetadata: IMetadata): Promise<IQContainer>;
    fetchContainer(containerId: string): Promise<IQContainer | null>;

}