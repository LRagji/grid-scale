import { IRDriver, RedisKeywords } from "../interfaces/i-r-driver.js";
import { ISortedElement } from "../interfaces/i-sorted-element.js";
import { Utilities } from "../utilities.js";
import { IKeyBuilder, RKeyBuilder } from "./r-key-builder.js";

export type PageRankedElement = ISortedElement & { pageRank: number };

export class RPage {
    constructor(
        private readonly redisDriver: IRDriver,
        public readonly pageBaseKey: string,
        //Defaults.
        private readonly keyBuilder: IKeyBuilder = new RKeyBuilder(),
        private readonly groupListTTLInMs: number = 24 * 60 * 60 * 1000 // 24 hours, This is to ensure that even if there are some issues with page purging, we won't have stale data hanging around indefinitely.
    ) { }

    public async dumpDataToPage(mutableElements: ISortedElement[], mvccCounterStart: number): Promise<void> {

        const pageUpsertCommands = new Map<string, string[]>();
        let elementCounter = mvccCounterStart;

        for (const element of mutableElements) {
            const elementGroupKey = this.keyBuilder.dimensionKey(this.pageBaseKey, element.gk);
            const existingCommands = pageUpsertCommands.get(elementGroupKey) || [RedisKeywords.ZADD, elementGroupKey];
            element.sn = elementCounter;// We can do a structure clone here, but to avoid perf overhead we will mutate the original element.
            existingCommands.push(element.elementRank.toString(), JSON.stringify(element));
            pageUpsertCommands.set(elementGroupKey, existingCommands);
            elementCounter++;
        }
        const groupListKey = this.keyBuilder.pageDimensionsDict(this.pageBaseKey, ""); // You might want to pass a specific dimension name here
        const commands = [...pageUpsertCommands.values()];
        commands.push([RedisKeywords.SADD, groupListKey, ...[...pageUpsertCommands.keys()]]);//Add all groups to group list for the page, this will help us in fetching and purging data for the page.
        commands.push([RedisKeywords.PEXPIRE, groupListKey, `${this.groupListTTLInMs}`]);

        await this.redisDriver.usingRedisDriver<void>(commands, 'DumpDataToPage', 'pipeline')
    }

    public async fetchElementsByRange(groupKeys: string[], pageRank: number, startInclusiveRank: number, endExclusiveRank: number, maxElementsPerGroup: number): Promise<Map<string, Map<number, PageRankedElement>>> {

        if (groupKeys.length === 0 || startInclusiveRank === endExclusiveRank) {
            return new Map<string, Map<number, PageRankedElement>>();
        }

        if (startInclusiveRank < 0 || endExclusiveRank <= startInclusiveRank) {
            throw new Error("Invalid rank range. Start rank must be non-negative and less than end rank. Currently, start rank is " + startInclusiveRank.toString() + " and end rank is " + endExclusiveRank.toString() + ".");
        }

        if (maxElementsPerGroup <= 0) {
            throw new Error("Max elements per group must be greater than 0. Currently, it is set to " + maxElementsPerGroup.toString() + ".");
        }

        if (startInclusiveRank >= Utilities.u48In3 || endExclusiveRank > Utilities.u48In3) {
            throw new Error("Rank values must be less than " + Utilities.u48In3.toString() + ". Currently, start rank is " + startInclusiveRank.toString() + " and end rank is " + endExclusiveRank.toString() + ".");
        }

        const finalGroupKeys = groupKeys.map(gk => this.keyBuilder.dimensionKey(this.pageBaseKey, gk));

        return await this.fetchElementsFromRedis(finalGroupKeys, startInclusiveRank, endExclusiveRank, maxElementsPerGroup, pageRank);
    }

    private async fetchElementsFromRedis(redisKeys: string[], startInclusiveRank: number, endExclusiveRank: number, maxElementsPerGroup: number, pageRank: number) {
        const commands = redisKeys
            .map(redisKey => [RedisKeywords.ZRANGE, redisKey, startInclusiveRank.toString(), endExclusiveRank.toString(), RedisKeywords.BYSCORE,
            ...(maxElementsPerGroup <= 0 ? [] : [RedisKeywords.LIMIT, "0", maxElementsPerGroup.toString()])]);

        const responses = await this.redisDriver.usingRedisDriver<string[][]>(commands, 'FetchElementsForPagesInRange', "pipeline");

        const rankedResults = new Map<string, Map<number, PageRankedElement>>();
        for (let i = 0; i < responses.length; i++) {
            const elements = responses[i] ?? [];
            for (let i = 0; i < elements.length; i++) {
                const element = JSON.parse(elements[i]) as ISortedElement;
                const existingGroupedElements = rankedResults.get(element.gk) ?? new Map<number, PageRankedElement>();
                const existingElement = existingGroupedElements.get(element.elementRank);
                const newElementAddition = existingElement === undefined && (existingGroupedElements.size < maxElementsPerGroup || maxElementsPerGroup <= 0); //New element addition, we can add if we have not reached the max elements per group limit.
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

    public async dumpDataFromPage(): Promise<Map<string, Map<number, PageRankedElement>>> {
        const redisKeys = await this.groupsInPage();
        return this.fetchElementsByRange(redisKeys, 0, 0, -1, -1);
    }

    public async purgePage(expireAfterInMilliseconds: number = 60 * 1000): Promise<void> {
        const expireCommands: string[][] = [];
        for (const group of await this.groupsInPage()) {
            expireCommands.push([RedisKeywords.PEXPIRE, this.keyBuilder.dimensionKey(this.pageBaseKey, group), expireAfterInMilliseconds.toString()]);//Expire in specified time, this is to avoid blocking calls to redis and also give some buffer time for any ongoing fetches to complete.
        }
        this.redisDriver.usingRedisDriver<void>(expireCommands, 'PurgePage', 'run');
    }

    private async groupsInPage(): Promise<string[]> {
        const groupListKey = this.keyBuilder.pageDimensionsDict(this.pageBaseKey, "");
        const groups = await this.redisDriver.usingRedisDriver<string[]>([[RedisKeywords.SMEMBERS, groupListKey]], 'FetchGroupsForPage', 'run');
        return groups;
    }
}