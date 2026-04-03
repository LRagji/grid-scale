import { IDimensionalElement } from "../../src/interfaces/i-dimensional-element.js";
import { IDimensionalQuery, ConditionGroup } from "../../src/interfaces/i-dimensional-query.js";
import { IKeyBuilder } from "../../src/interfaces/i-key-builder.js";
import { IPageInfo } from "../../src/interfaces/i-page-info.js";
import { IPage } from "../../src/interfaces/i-page.js";
import { IRDriver, RedisKeywords } from "../../src/interfaces/i-r-driver.js";
import { RKeyBuilder } from "../../src/utilities/r-key-builder.js";
import { ConvenienceMethods } from "../../src/utilities/convenience-methods.js";

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

//The minimal we save in redis the better.
interface IStoredSample {
    p: any;
    mvccId: number;
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
        const tagSet = new Set<string>();
        let elementCounter = sequenceStart;

        for (const element of elements) {
            const elementGroupKey = this.keyBuilder.dimensionKey(this._pageInfo.pageKey, element.dim.tag);
            const existingCommands = pageUpsertCommands.get(elementGroupKey) || [RedisKeywords.ZADD, elementGroupKey];
            // Store a serializable object that includes tag and time
            const storableElement: IStoredSample = {
                p: element.pld,
                mvccId: elementCounter
            };
            existingCommands.push(element.dim.time.toString(), JSON.stringify(storableElement));
            pageUpsertCommands.set(elementGroupKey, existingCommands);
            tagSet.add(element.dim.tag);
            elementCounter++;
        }
        const groupListKey = this.keyBuilder.pageDimensionsDict(this._pageInfo.pageKey, this.dimensionNameForGrouping);
        const commands = [...pageUpsertCommands.values()];
        commands.push([RedisKeywords.SADD, groupListKey, ...[...tagSet]]);//Add all tags to group list for the page, this will help us in fetching and purging data for the page.
        commands.push([RedisKeywords.PEXPIRE, groupListKey, `${this.pageDimensionsDicTTLms}`]);

        await this.redisDriver.usingRedisDriver<void>(commands, 'DumpDataToPage', 'pipeline')
    }

    public async queryElementsByDimensions(query: IDimensionalQuery, maxElementsCount: number): Promise<TimeseriesSample[]> {
        const allTags = await this.groupsInPage();
        if (allTags.length === 0) return [];

        // Narrow the tag set and score range from AND-resolvable conditions so Redis
        // does the heavy lifting instead of loading the entire page into memory.
        const candidateTags = this.extractCandidateTags(query, allTags);
        if (candidateTags.length === 0) return [];

        const { start, end } = this.extractScoreRange(query);
        const results = await this.fetchElementsFromRedis(candidateTags, start, end, maxElementsCount);
        return results;
        //return filterByDimensionalQuery(rawResults, query, maxElementsCount) as TimeseriesSample[];
    }

    // Walks AND groups to narrow the working tag set using tag-dimension conditions.
    // OR groups are skipped — they cannot safely narrow the set without union logic.
    private extractCandidateTags(query: IDimensionalQuery, allTags: string[]): string[] {
        const tagSet = new Set(allTags);
        this.applyTagConstraints(query.query, tagSet);
        return Array.from(tagSet);
    }

    private applyTagConstraints(group: ConditionGroup, tagSet: Set<string>): void {
        if (group.operator !== "AND") return;
        for (const condition of group.conditions) {
            if ("conditions" in condition) {
                this.applyTagConstraints(condition, tagSet);
                continue;
            }
            if (condition.dimension !== this.dimensionNameForGrouping) continue;
            if (condition.operator === "eq" && typeof condition.value === "string") {
                for (const t of tagSet) if (t !== condition.value) tagSet.delete(t);
            } else if (condition.operator === "in" && Array.isArray(condition.value)) {
                const allowed = new Set(condition.value as string[]);
                for (const t of tagSet) if (!allowed.has(t)) tagSet.delete(t);
            } else if (condition.operator === "noteq" && typeof condition.value === "string") {
                tagSet.delete(condition.value);
            } else if (condition.operator === "notin" && Array.isArray(condition.value)) {
                for (const v of condition.value as string[]) tagSet.delete(v);
            }
        }
    }

    // Walks AND groups to derive the tightest BYSCORE window from time-dimension conditions.
    // OR groups are skipped — they cannot safely narrow the range without union logic.
    private extractScoreRange(query: IDimensionalQuery): { start: number | "-inf", end: number | "+inf" } {
        const range = { start: -Infinity, end: Infinity };
        this.applyTimeConstraints(query.query, range);
        return {
            start: range.start === -Infinity ? "-inf" : range.start,
            end: range.end === Infinity ? "+inf" : range.end
        };
    }

    private applyTimeConstraints(group: ConditionGroup, range: { start: number, end: number }): void {
        if (group.operator !== "AND") return;
        for (const condition of group.conditions) {
            if ("conditions" in condition) {
                this.applyTimeConstraints(condition, range);
                continue;
            }
            if (condition.dimension !== "time") continue;
            if (condition.operator === "eq" && typeof condition.value === "number") {
                range.start = Math.max(range.start, condition.value);
                range.end = Math.min(range.end, condition.value);
            } else if (condition.operator === "gt" && typeof condition.value === "number") {
                range.start = Math.max(range.start, condition.value + 1);
            } else if (condition.operator === "lt" && typeof condition.value === "number") {
                range.end = Math.min(range.end, condition.value - 1);
            } else if (condition.operator === "between" && Array.isArray(condition.value)) {
                range.start = Math.max(range.start, (condition.value as [number, number])[0]);
                range.end = Math.min(range.end, (condition.value as [number, number])[1]);
            }
        }
    }

    public async dumpPage(): Promise<TimeseriesSample[]> {
        const tags = await this.groupsInPage();
        return this.fetchElementsFromRedis(tags, "-inf", "+inf", -1);
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

        if (startInclusiveRank >= ConvenienceMethods.u48In3 || endExclusiveRank > ConvenienceMethods.u48In3) {
            throw new Error("Rank values must be less than " + ConvenienceMethods.u48In3.toString() + ". Currently, start rank is " + startInclusiveRank.toString() + " and end rank is " + endExclusiveRank.toString() + ".");
        }

        return await this.fetchElementsFromRedis(groupKeys, startInclusiveRank, endExclusiveRank, maxElementsPerGroup);
    }

    public async purgePage(expireAfterInMilliseconds: number = 60 * 1000): Promise<void> {
        const expireCommands: string[][] = [];
        const tags = await this.groupsInPage();
        for (const tag of tags) {
            expireCommands.push([RedisKeywords.PEXPIRE, this.keyBuilder.dimensionKey(this._pageInfo.pageKey, tag), expireAfterInMilliseconds.toString()]);//Expire in specified time, this is to avoid blocking calls to redis and also give some buffer time for any ongoing fetches to complete.
        }
        expireCommands.push([RedisKeywords.PEXPIRE, this.keyBuilder.pageDimensionsDict(this._pageInfo.pageKey, this.dimensionNameForGrouping), expireAfterInMilliseconds.toString()]);//Expire group list as well.
        await this.redisDriver.usingRedisDriver<void>(expireCommands, 'PurgePage', 'pipeline');
    }

    private async fetchElementsFromRedis(groupKeys: string[], startInclusiveRank: number | "-inf", endExclusiveRank: number | "+inf", maxElementsPerGroup: number): Promise<TimeseriesSample[]> {

        const redisKeys = groupKeys.map(gk => this.keyBuilder.dimensionKey(this._pageInfo.pageKey, gk));

        const commands = redisKeys
            .map(redisKey => [RedisKeywords.ZRANGE, redisKey, startInclusiveRank.toString(), endExclusiveRank.toString(), RedisKeywords.BYSCORE,
            ...(maxElementsPerGroup <= 0 ? [] : [RedisKeywords.LIMIT, "0", maxElementsPerGroup.toString()]), RedisKeywords.WITHSCORES]);

        const responses = await this.redisDriver.usingRedisDriver<string[][]>(commands, 'FetchElementsForPagesInRange', "pipeline");

        const rankedResults = new Map<string, Map<number, TimeseriesSample>>();
        for (let i = 0; i < responses.length; i++) {
            const elementsWithScores = responses[i] ?? [];
            const tagName = groupKeys[i];
            for (let j = 0; j < elementsWithScores.length; j++) {
                const stringifiedElement = elementsWithScores[j];
                const time = parseInt(elementsWithScores[++j], 10);//Time is the score of the sorted set entry
                const stored = JSON.parse(stringifiedElement) as IStoredSample;
                const element = new TimeseriesSample(tagName, time, stored.p, stored.mvccId);
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