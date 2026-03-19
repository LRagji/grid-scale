import { IRDriver, RedisKeywords } from "../interfaces/i-r-driver.js";
import { ISortedElement } from "../interfaces/i-sorted-element.js";
import { IKeyBuilder, RKeyBuilder } from "./r-key-builder.js";

export type PageRankedElement = ISortedElement & { pageRank: number };

export class RPage {
    constructor(
        private readonly redisDriver: IRDriver,
        public readonly pageBaseKey: string,
        //Defaults.
        private readonly keyBuilder: IKeyBuilder = new RKeyBuilder()
    ) { }

    public async dumpDataToPage(mutableElements: ISortedElement[], mvccCounterStart: number): Promise<void> {

        const pageUpsertCommands = new Map<string, string[]>();
        let elementCounter = mvccCounterStart;

        for (const element of mutableElements) {
            const elementGroupKey = this.keyBuilder.groupKey(this.pageBaseKey, element.gk);
            const existingCommands = pageUpsertCommands.get(elementGroupKey) || [RedisKeywords.ZADD, elementGroupKey];
            element.sn = elementCounter;// We can do a structure clone here, but to avoid perf overhead we will mutate the original element.
            existingCommands.push(element.elementRank.toString(), JSON.stringify(element));
            pageUpsertCommands.set(elementGroupKey, existingCommands);
            elementCounter++;
        }

        await this.redisDriver.usingRedisDriver<void>([...pageUpsertCommands.values()], 'DumpDataToPage', 'pipeline')
    }

    public async fetchElementsByRange(groupKeys: string[], pageRank: number, startInclusiveRank: number, endExclusiveRank: number, maxElementsPerGroup: number): Promise<Map<string, Map<number, PageRankedElement>>> {

        const commands: string[][] = [];
        for (const groupKey of groupKeys) {
            const finalKey = this.keyBuilder.groupKey(this.pageBaseKey, groupKey);
            commands.push([RedisKeywords.ZRANGE, finalKey, startInclusiveRank.toString(), endExclusiveRank.toString(), RedisKeywords.BYSCORE, RedisKeywords.LIMIT, "0", maxElementsPerGroup.toString()]);
        }

        const responses = await this.redisDriver.usingRedisDriver<string[][]>(commands, 'FetchElementsForPagesInRange', "pipeline");

        const rankedResults = new Map<string, Map<number, PageRankedElement>>();
        for (let i = 0; i < responses.length; i++) {
            const elements = responses[i] ?? [];
            //const pageRank = indexedResponseContext[i];
            for (let i = 0; i < elements.length; i++) {
                const element = JSON.parse(elements[i]) as ISortedElement;
                const existingGroupedElements = rankedResults.get(element.gk) ?? new Map<number, PageRankedElement>();
                const existingElement = existingGroupedElements.get(element.elementRank);
                const newElementAddition = existingElement === undefined && existingGroupedElements.size < maxElementsPerGroup;
                const existingElementWithinPageUpdate = existingElement !== undefined && existingElement.sn < element.sn; //Update within same page, This may also mean we may have less samples as they were updated of the same timestamp.

                if (newElementAddition || existingElementWithinPageUpdate) {
                    existingGroupedElements.set(element.elementRank, { ...element, pageRank });
                    rankedResults.set(element.gk, existingGroupedElements);
                    continue;
                }
            }
        }

        return rankedResults;
    }

    public async dumpPageData(): Promise<Map<string, Map<number, PageRankedElement>>> {
        throw new Error("Method not implemented. This is a placeholder for future implementation if needed.");
    }

    public async purgePage(expireAfterInMilliseconds: number = 60 * 1000): Promise<void> {
        const expireCommands: string[][] = [];
        for (const group of await this.groupsInPage()) {
            expireCommands.push([RedisKeywords.PEXPIRE, this.keyBuilder.groupKey(this.pageBaseKey, group), expireAfterInMilliseconds.toString()]);//Expire in specified time, this is to avoid blocking calls to redis and also give some buffer time for any ongoing fetches to complete.
        }
        this.redisDriver.usingRedisDriver<void>(expireCommands, 'PurgePage', 'run');
    }

    public async groupsInPage(): Promise<string[]> {
        // const pattern = this.keyBuilder.groupKey(this.pageBaseKey, '*');
        // const groupKeys = await this.redisDriver.usingRedisDriver<string[]>([[RedisKeywords.KEYS, pattern]], 'GroupsInPage', 'run');
        // return groupKeys.map(k => k.replace(this.pageBaseKey + ':', ''));
        return [];
    }
}