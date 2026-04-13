import { randomUUID } from "node:crypto";
import { IDimensionalElement } from "../interfaces/i-dimensional-element.js";
import { IDimensionalQuery } from "../interfaces/i-dimensional-query.js";
import { IMetadata, IPolicyEvaluator, IQAcc, IQContainer, IQContainerFactory } from "../interfaces/i-q-acc.js";

export class LocalQAccumulator implements IQAcc {

    private static readonly totalElementsMetaKey = "totalElementsCount";
    private static readonly totalElementsByteSizeMetaKey = "totalElementsSizeBytes";
    private static readonly containerCreationTimeInMsMetaKey = "containerCreationTimeInMs";
    private static readonly containerLastAccumulatedTimeInMsMetaKey = "containerLastAccumulatedTimeInMs";
    private static readonly containerIdMetaKey = "containerId";

    private containerFactory: IQContainerFactory | null = null;
    private containerMetadata: IMetadata = {};
    private flushPolicyEvaluator: IPolicyEvaluator<IMetadata> | null = null;
    private currentContainer: IQContainer | null = null;
    private initialized = false;
    private flushInProgress = false;
    private policyState = {
        [LocalQAccumulator.totalElementsMetaKey]: 0,
        [LocalQAccumulator.totalElementsByteSizeMetaKey]: 0,
        [LocalQAccumulator.containerCreationTimeInMsMetaKey]: 0,
        [LocalQAccumulator.containerLastAccumulatedTimeInMsMetaKey]: 0,
        [LocalQAccumulator.containerIdMetaKey]: ""
    } as IMetadata


    constructor(
        private readonly elementSizeEstimator: (elements: IDimensionalElement[]) => number = (elements: IDimensionalElement[]) => Buffer.byteLength(JSON.stringify(elements), "utf8"),
        private readonly onFlushedContainers: (flushReasons: string[], flushedContainerIds: string[]) => Promise<void> = async (flushReasons: string[], flushedContainerIds: string[]) => { console.log(`Flushed containers: ${flushedContainerIds.join(", ")} for reasons: ${flushReasons.join(", ")}`) }) { }

    public async initialize(containerFactory: IQContainerFactory, containerMetadata: IMetadata, flushPolicyEvaluator: IPolicyEvaluator<IMetadata>): Promise<void> {

        if (containerFactory === null) {
            throw new Error("containerFactory cannot be null.");
        }

        this.containerFactory = containerFactory;
        this.containerMetadata = { ...containerMetadata };
        this.flushPolicyEvaluator = flushPolicyEvaluator;

        this.flushPolicyEvaluator.initialize(async (flushReasons: string[]) => {
            const flushedContainerIds = await this.flushCurrentContainer();
            if (flushedContainerIds.length > 0) {
                await this.onFlushedContainers(flushReasons, flushedContainerIds);
            }
        });

        this.initialized = true;
    }

    public async accumulate(elements: IDimensionalElement[]): Promise<void> {
        this.ensureInitialized();

        if (elements.length === 0) {
            return;
        }

        if (this.currentContainer === null) {
            this.currentContainer = await this.createNewContainer();
        }

        await this.currentContainer!.accumulate(elements);

        (this.policyState[LocalQAccumulator.totalElementsMetaKey] as number) += elements.length;
        (this.policyState[LocalQAccumulator.totalElementsByteSizeMetaKey] as number) += this.elementSizeEstimator(elements);
        (this.policyState[LocalQAccumulator.containerLastAccumulatedTimeInMsMetaKey] as number) = Date.now();


        await this.flushPolicyEvaluator!.evaluatePolicies(this.policyState);
    }

    public async query(query: IDimensionalQuery, maxElementsCount: number): Promise<IDimensionalElement[]> {
        this.ensureInitialized();

        return await this.currentContainer!.query(query, maxElementsCount);
    }

    private ensureInitialized(): void {
        if (this.initialized === false) {
            throw new Error("LocalQAccumulator must be initialized before use.");
        }
    }

    private async createNewContainer(): Promise<IQContainer> {
        const containerId = randomUUID();
        const created = await this.containerFactory!.createContainer(containerId, this.containerMetadata);
        await created.initialize(this.containerMetadata);

        (this.policyState[LocalQAccumulator.totalElementsMetaKey] as number) = 0;
        (this.policyState[LocalQAccumulator.totalElementsByteSizeMetaKey] as number) = 0;
        (this.policyState[LocalQAccumulator.containerCreationTimeInMsMetaKey] as number) = Date.now();
        (this.policyState[LocalQAccumulator.containerLastAccumulatedTimeInMsMetaKey] as number) = 0;
        (this.policyState[LocalQAccumulator.containerIdMetaKey] as string) = containerId;
        return created;
    }

    private async flushCurrentContainer(): Promise<string[]> {
        if (this.flushInProgress) {
            return [];
        }

        this.flushInProgress = true;
        try {
            const previousContainer = this.currentContainer;
            const nextContainer = await this.createNewContainer();
            this.currentContainer = nextContainer;
            return previousContainer ? [previousContainer.id] : [];
        } finally {
            this.flushInProgress = false;
        }
    }
}