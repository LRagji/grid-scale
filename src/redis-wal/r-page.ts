import { IRDriver, RedisKeywords } from "../interfaces/i-r-driver";
import { ISample, ISampleSet, IScoredSample } from "../interfaces/i-sample";
import { Utilities } from "../utilities";
import { IKeyBuilder, RedisKeyBuilder } from "./redis-key-builder";

export class RPage {

    constructor(
        public readonly timeKeyPart: number,
        public readonly sizeKeyPart: number,
        public readonly writesKeyPart: number,
        public readonly newPage: boolean,
        private readonly redisDriver: IRDriver,
        //Defaults.
        private readonly timeWindowInMs: number = 24 * 60 * 60 * 1000, // 24 hours
        private readonly sizeWindowInBytes: number = Utilities.u48In3,
        private readonly writeWindow: number = Utilities.u48In3,
        private readonly maxPagesInBook: number = 100,
        private readonly keyBuilder: IKeyBuilder = new RedisKeyBuilder()
    ) { }

    public get pageKey(): string {
        //TODO: Why are we not using times.
        const modSize = Utilities.modMinus(this.sizeKeyPart, this.sizeWindowInBytes);
        const modWrites = Utilities.modMinus(this.writesKeyPart, this.writeWindow);
        const pageKey = this.keyBuilder.pageKey(this.timeKeyPart.toString(), modSize.toString(), modWrites.toString());
        return pageKey;
    }

    //-------------------------------------------------- Upsert Data to Pages --------------------------------------------
    public async dumpDataToPage(pageKey: string, samples: ISample[], insertTime: number, currentWriteCount: number, currentByteCount: number): Promise<void> {
        const updateBookCommands = this.generateBookUpdateCommand(pageKey, insertTime);
        const pageUpsertCommands = new Map<string, string[]>();[RedisKeywords.ZADD, pageKey];
        for (const sample of samples) {
            const tagKey = this.keyBuilder.tagKey(pageKey, sample.tag);
            const existingCommands = pageUpsertCommands.get(tagKey) || [RedisKeywords.ZADD, tagKey];
            const clonedSample = structuredClone(sample);
            delete clonedSample.tag;
            existingCommands.push(`${sample.ts.toString()}.${currentWriteCount.toString()}`, JSON.stringify(clonedSample));
            pageUpsertCommands.set(tagKey, existingCommands);
        }

        await this.redisDriver.usingRedisDriver<void>([...pageUpsertCommands.values(), ...updateBookCommands], 'DumpDataToPage', 'pipeline')
    }

    private generateBookUpdateCommand(pageKey: string, insertTime: number): string[][] {
        return [
            [RedisKeywords.ZADD, this.keyBuilder.bookKey(), insertTime.toString(), pageKey],
            [RedisKeywords.ZREMRANGEBYRANK, this.keyBuilder.bookKey(), "0", `-${this.maxPagesInBook + 1}`]
        ];
    }

    //-------------------------------------------------- Query Data from Pages --------------------------------------------
    public async queryRange(tags: string[], startTime: number, endTime: number, pageSize = 100): Promise<ISampleSet[]> {
        const deDuplicatedTags = this.validateQueryRangeParams(tags, startTime, endTime);
        const rankedPages = await this.fetchAllPagesWithRanks();
        return await this.fetchDataForPagesInRange(deDuplicatedTags, rankedPages, startTime, endTime, pageSize);
    }

    private async fetchAllPagesWithRanks(): Promise<Map<string, number>> {
        const bookKey = this.keyBuilder.bookKey();
        const pageKeys = await this.redisDriver.usingRedisDriver<string[]>([[RedisKeywords.ZRANGE, bookKey, "0", "-1", RedisKeywords.WITHSCORES]], 'FetchAllPagesWithRanks', "run");
        const pageKeyMap = new Map<string, number>();
        for (let i = 0; i < pageKeys.length; i += 2) {
            const pageKey = pageKeys[i];
            const score = parseInt(pageKeys[i + 1], 10);
            pageKeyMap.set(pageKey, score);
        }
        return pageKeyMap;
    }

    private validateQueryRangeParams(tags: string[], startTime: number, endTime: number): string[] {

        if (tags.length === 0) {
            throw new Error("At least one tag must be specified for querying.");
        }
        if (startTime < 0 || endTime < 0) {
            throw new Error("Start time and end time must be non-negative.");
        }
        if (endTime < startTime) {
            throw new Error("End time must be greater than or equal to start time.");
        }
        if ((endTime - startTime) === 0) {
            throw new Error(`The difference between end time and start time must be greater than 0. Currently, it is ${endTime - startTime} ms.`);
        }
        if (tags.length > 10) {
            throw new Error("A maximum of 10 tags can be specified for querying to prevent excessive load. Currently, " + tags.length + " tags were provided.");
        }

        return [...(new Set(tags)).values()]
    }

    private async fetchDataForPagesInRange(tagNames: string[], rankedPages: Map<string, number>, startTime: number, endTime: number, pageSize: number): Promise<ISampleSet[]> {

        const commands: string[][] = [];
        const indexedResponseContext = new Array<{ pageRank: number, tagName: string }>();
        for (const [pageKey, pageRank] of rankedPages) {
            for (const tagName of tagNames) {
                const finalTagKey = this.keyBuilder.tagKey(pageKey, tagName);
                commands.push([RedisKeywords.ZRANGE, finalTagKey, startTime.toString(), endTime.toString(), RedisKeywords.BYSCORE, RedisKeywords.WITHSCORES, RedisKeywords.LIMIT, "0", pageSize.toString()]);
                indexedResponseContext.push({ pageRank, tagName });
            }
        }

        const responses = await this.redisDriver.usingRedisDriver<string[][]>(commands, 'FetchDataForPagesInRange', "pipeline");

        const result = new Map<string, Map<number, IScoredSample>>();
        for (let i = 0; i < responses.length; i++) {
            const response = responses[i] ?? [];
            const { pageRank, tagName } = indexedResponseContext[i];
            for (let i = 0; i < response.length; i++) {
                const sample = JSON.parse(response[i]) as ISample;
                const writeScore = parseFloat(response[++i]);
                sample.tag = tagName;
                const existingTagSamples = result.get(sample.tag) ?? new Map<number, IScoredSample>();
                let existingSample = existingTagSamples.get(sample.ts) ?? { ...sample, pageRank, writeScore };
                //This is a scenario where updates are spread across multiple pages. 
                //We need to ensure that we take the latest update for the same timestamp based on the score (which is the insert time of the page).
                if ((existingSample.pageRank < pageRank) || (existingSample.pageRank === pageRank && existingSample.writeScore < writeScore)) {
                    existingSample = { ...sample, pageRank, writeScore };
                }
                existingTagSamples.set(sample.ts, existingSample);
                result.set(sample.tag, existingTagSamples);
            }
        }
        return this.transformScoresToOriginalTimestamps(result);
    }

    private transformScoresToOriginalTimestamps(data: Map<string, Map<number, IScoredSample>>): ISampleSet[] {
        const result = new Map<string, ISampleSet>();
        for (const [tagName, samplesByTimestamp] of data) {
            for (const [_, sample] of samplesByTimestamp) {
                const { pageRank: score, ...originalSample } = sample;
                originalSample.tag = tagName;
                const existingSampleSet = result.get(tagName) ?? { tag: tagName, samples: [], minTs: Number.MAX_SAFE_INTEGER, maxTs: Number.MIN_SAFE_INTEGER, count: 0 };
                existingSampleSet.samples.push(originalSample);
                existingSampleSet.minTs = Math.min(existingSampleSet.minTs, originalSample.ts);
                existingSampleSet.maxTs = Math.max(existingSampleSet.maxTs, originalSample.ts);
                existingSampleSet.count += 1;
                result.set(tagName, existingSampleSet);
            }
        }
        return Array.from(result.values());
    }
}