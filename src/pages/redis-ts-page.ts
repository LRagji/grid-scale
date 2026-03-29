import { IDimensionalElement } from "../interfaces/i-dimensional-element.js";
import { IDimensionalQuery } from "../interfaces/i-dimensional-query.js";
import { IPageInfo } from "../interfaces/i-page-info.js";
import { IPage } from "../interfaces/i-page.js";
import { IRDriver, RedisKeywords } from "../interfaces/i-r-driver.js";
import { IKeyBuilder, RKeyBuilder } from "../redis-wal/r-key-builder.js";
import { Utilities } from "../utilities.js";

export class TimeseriesSample implements IDimensionalElement {

    public get dim(): { "tag": string; "time": number; } {
        return {
            "tag": this.tagName,
            "time": this.time
        };
    }

    public get globalIdentityHash(): string {
        return `${this.dim.tag}-${this.dim.time}`;
    }

    constructor(private tagName: string, private time: number, public pld: any, public mvccId: number = -1) {
    }
}


export class RedisTsPage implements IPage {

    public get info(): IPageInfo {
        return this._pageInfo;
    }

    public get pageType(): string {
        return "REDIS_TIMESERIES_PAGE";
    }

    constructor(private readonly _pageInfo: IPageInfo,
        private readonly redisDriver: IRDriver,
        private readonly keyBuilder: IKeyBuilder = new RKeyBuilder(),
        private readonly pageDimensionsDicTTLms: number = 24 * 60 * 60 * 1000, // 24 hours,
        private readonly dimensionNameForGrouping: string = "tag"
    ) { }

    public async upsertElements(elements: TimeseriesSample[], sequenceStart: number): Promise<void> {
        const pageUpsertCommands = new Map<string, string[]>();
        let elementCounter = sequenceStart;

        for (const element of elements) {
            const elementGroupKey = this.keyBuilder.dimensionKey(this._pageInfo.pageKey, element.dim.tag);
            const existingCommands = pageUpsertCommands.get(elementGroupKey) || [RedisKeywords.ZADD, elementGroupKey];
            element.mvccId = elementCounter;// We can do a structure clone here, but to avoid perf overhead we will mutate the original element.
            existingCommands.push(element.dim.time.toString(), JSON.stringify(element));
            pageUpsertCommands.set(elementGroupKey, existingCommands);
            elementCounter++;
        }
        const groupListKey = this.keyBuilder.pageDimensionsDict(this._pageInfo.pageKey, this.dimensionNameForGrouping);
        const commands = [...pageUpsertCommands.values()];
        commands.push([RedisKeywords.SADD, groupListKey, ...[...pageUpsertCommands.keys()]]);//Add all groups to group list for the page, this will help us in fetching and purging data for the page.
        commands.push([RedisKeywords.PEXPIRE, groupListKey, `${this.pageDimensionsDicTTLms}`]);

        await this.redisDriver.usingRedisDriver<void>(commands, 'DumpDataToPage', 'pipeline')
    }

    public queryElementsByDimensions(query: IDimensionalQuery): Promise<TimeseriesSample[]> {
        throw new Error("Method not implemented.");
    }

    public async dumpPage(): Promise<TimeseriesSample[]> {
        const redisKeys = await this.groupsInPage();
        return this.fetchElementsByRange(redisKeys, 0, 0, -1);
    }

    public async fetchElementsByRange(groupKeys: string[], startInclusiveRank: number, endExclusiveRank: number, maxElementsPerGroup: number): Promise<TimeseriesSample[]> {
        if (groupKeys.length === 0 || startInclusiveRank === endExclusiveRank) {
            return new Array<TimeseriesSample>();
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

        if (endExclusiveRank <= 0) {
            throw new Error("End rank must be greater than 0. Currently, it is set to " + endExclusiveRank.toString() + ".");
        }

        if (endExclusiveRank <= startInclusiveRank) {
            throw new Error("End rank must be greater than start rank. Currently, start rank is " + startInclusiveRank.toString() + " and end rank is " + endExclusiveRank.toString() + ".");
        }

        const finalGroupKeys = groupKeys.map(gk => this.keyBuilder.dimensionKey(this._pageInfo.pageKey, gk));

        return await this.fetchElementsFromRedis(finalGroupKeys, startInclusiveRank, endExclusiveRank, maxElementsPerGroup);
    }

    public async purgePage(expireAfterInMilliseconds: number = 60 * 1000): Promise<void> {
        const expireCommands: string[][] = [];
        for (const group of await this.groupsInPage()) {
            expireCommands.push([RedisKeywords.PEXPIRE, this.keyBuilder.dimensionKey(this._pageInfo.pageKey, group), expireAfterInMilliseconds.toString()]);//Expire in specified time, this is to avoid blocking calls to redis and also give some buffer time for any ongoing fetches to complete.
        }
        expireCommands.push([RedisKeywords.PEXPIRE, this.keyBuilder.pageDimensionsDict(this._pageInfo.pageKey, this.dimensionNameForGrouping), expireAfterInMilliseconds.toString()]);//Expire group list as well.
        this.redisDriver.usingRedisDriver<void>(expireCommands, 'PurgePage', 'run');
    }

    private async fetchElementsFromRedis(redisKeys: string[], startInclusiveRank: number, endExclusiveRank: number, maxElementsPerGroup: number): Promise<TimeseriesSample[]> {
        const commands = redisKeys
            .map(redisKey => [RedisKeywords.ZRANGE, redisKey, startInclusiveRank.toString(), endExclusiveRank.toString(), RedisKeywords.BYSCORE,
            ...(maxElementsPerGroup <= 0 ? [] : [RedisKeywords.LIMIT, "0", maxElementsPerGroup.toString()])]);

        const responses = await this.redisDriver.usingRedisDriver<string[][]>(commands, 'FetchElementsForPagesInRange', "pipeline");

        const rankedResults = new Map<string, Map<number, TimeseriesSample>>();
        for (let i = 0; i < responses.length; i++) {
            const elements = responses[i] ?? [];
            for (const stringifiedElement of elements) {
                const element = JSON.parse(stringifiedElement) as TimeseriesSample;
                const existingGroupedElements = rankedResults.get(element.dim.tag) ?? new Map<number, TimeseriesSample>();
                const existingElement = existingGroupedElements.get(element.dim.time);
                const newElementAddition = existingElement === undefined && (existingGroupedElements.size < maxElementsPerGroup || maxElementsPerGroup <= 0); //New element addition, we can add if we have not reached the max elements per group limit.
                const existingElementWithinPageUpdate = existingElement !== undefined && existingElement.mvccId < element.mvccId; //Update within same page, This may also mean we may have less samples as they were updated of the same timestamp.

                if (newElementAddition || existingElementWithinPageUpdate) {
                    existingGroupedElements.set(element.dim.time, element);
                    rankedResults.set(element.dim.tag, existingGroupedElements);
                    continue;
                }
            }
        }

        return Array.from(rankedResults.values())
            .flatMap(group => Array.from(group.values()));
    }

    private async groupsInPage(): Promise<string[]> {
        const groupListKey = this.keyBuilder.pageDimensionsDict(this._pageInfo.pageKey, this.dimensionNameForGrouping);
        const groups = await this.redisDriver.usingRedisDriver<string[]>([[RedisKeywords.SMEMBERS, groupListKey]], 'FetchGroupsForPage', 'run');
        return groups;
    }

}