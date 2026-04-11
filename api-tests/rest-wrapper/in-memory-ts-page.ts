import { IDimensionalQuery, ConditionGroup } from "../../src/interfaces/i-dimensional-query.js";
import { IPageInfo } from "../../src/interfaces/i-page-info.js";
import { IPage } from "../../src/interfaces/i-page.js";
import { ConvenienceMethods } from "../../src/utilities/convenience-methods.js";
import { TimeseriesSample } from "./redis-ts-page.js";

export class InMemoryTsPage implements IPage {

    public get info(): IPageInfo {
        return this._pageInfo;
    }

    public get pageType(): string {
        return InMemoryTsPage.pageType;
    }

    public static get pageType(): string {
        return "IN_MEMORY_TIMESERIES_PAGE";
    }

    // tag -> (time -> sample)
    private readonly store = new Map<string, Map<number, TimeseriesSample>>();
    private purgeTimer: ReturnType<typeof setTimeout> | null = null;

    constructor(
        private readonly _pageInfo: IPageInfo,
        private readonly dimensionNameForGrouping: string = "tag"
    ) { }

    public async upsertElements(elements: TimeseriesSample[], sequenceStart: number): Promise<void> {
        let elementCounter = sequenceStart;
        for (const element of elements) {
            const { tag, time } = element.dim;
            const tagMap = this.store.get(tag) ?? new Map<number, TimeseriesSample>();
            const existing = tagMap.get(time);
            const incoming = new TimeseriesSample(tag, time, element.pld, elementCounter);
            if (existing === undefined || existing.mvccId < incoming.mvccId) {
                tagMap.set(time, incoming);
                this.store.set(tag, tagMap);
            }
            elementCounter++;
        }
    }

    public async queryElementsByDimensions(query: IDimensionalQuery, maxElementsCount: number): Promise<TimeseriesSample[]> {
        const allTags = Array.from(this.store.keys());
        if (allTags.length === 0) return [];

        const candidateTags = this.extractCandidateTags(query, allTags);
        if (candidateTags.length === 0) return [];

        const { start, end } = this.extractScoreRange(query);
        return this.fetchElements(candidateTags, start, end, maxElementsCount);
    }

    public async dumpPage(): Promise<TimeseriesSample[]> {
        const allTags = Array.from(this.store.keys());
        return this.fetchElements(allTags, -Infinity, Infinity, -1);
    }

    public async fetchElementsByRange(groupKeys: string[], startInclusiveRank: number, endExclusiveRank: number, maxElementsPerGroup: number): Promise<TimeseriesSample[]> {
        if (groupKeys.length === 0 || startInclusiveRank === endExclusiveRank) {
            return [];
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
        return this.fetchElements(groupKeys, startInclusiveRank, endExclusiveRank, maxElementsPerGroup);
    }

    public async purgePage(expireAfterInMilliseconds: number = 60 * 1000): Promise<void> {
        if (this.purgeTimer !== null) clearTimeout(this.purgeTimer);
        this.purgeTimer = setTimeout(() => {
            this.store.clear();
            this.purgeTimer = null;
        }, expireAfterInMilliseconds);
    }

    // Walks AND groups to narrow the working tag set.
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

    // Walks AND groups to derive the tightest time window.
    private extractScoreRange(query: IDimensionalQuery): { start: number, end: number } {
        const range = { start: -Infinity, end: Infinity };
        this.applyTimeConstraints(query.query, range);
        return range;
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

    private fetchElements(groupKeys: string[], start: number, end: number, maxPerGroup: number): TimeseriesSample[] {
        const results: TimeseriesSample[] = [];
        for (const tag of groupKeys) {
            const tagMap = this.store.get(tag);
            if (tagMap === undefined) continue;
            let count = 0;
            // Iterate times in ascending order
            const sortedTimes = Array.from(tagMap.keys()).sort((a, b) => a - b);
            for (const time of sortedTimes) {
                if (time < start || time > end) continue;
                if (maxPerGroup > 0 && count >= maxPerGroup) break;
                results.push(tagMap.get(time)!);
                count++;
            }
        }
        return results;
    }
}
