import { IRedisClientPool } from "redis-abstraction";
import { IKeyBuilder, RedisKeyBuilder } from "./redis-key-builder.js";
import { ISample, IScoredSample } from "../interfaces/i-sample.js";
import { Utilities } from "../utilities.js";

class RedisKeywords {
    static BITFIELD = "bitfield";
    static INCRBY = "incrby";
    static OVERFLOW = "overflow";
    static FAIL = "fail";
    static PEXPIRE = "pexpire";
    static ZADD = "zadd";
    static ZREMRANGEBYRANK = "zremrangebyrank";
    static TIME = "time";
    static ZRANGE = "zrange";
    static BYSCORE = "byscore";
    static LIMIT = "limit";
    static WITHSCORES = "withscores";
    static SET_ONLY_IF_NO_EXPIRY = "nx";
    static SET = "set";
    static GET = "get";
}

export class RedisWAL {

    constructor(
        private readonly redisDriver: IRedisClientPool,
        //Defaults.
        private readonly timeToleranceInMs: bigint = 1n * 60n * 1000n, // 1 minute
        private readonly timeWindowInMs: bigint = 24n * 60n * 60n * 1000n, // 24 hours
        private readonly sizeWindowInBytes: bigint = Utilities.u63Max,
        private readonly writeWindow: bigint = Utilities.u63Max,
        private readonly keyBuilder: IKeyBuilder = new RedisKeyBuilder(),
        private readonly sizeEstimator: (samples: ISample[]) => bigint = Utilities.roughSizeEstimator,
        private readonly maxPagesInBook: number = 100
    ) { }

    public async initialize(): Promise<void> {

        await this.redisDriver.initialize();

        const timeAligned = await this.checkTimeTolerance();
        if (!timeAligned) {
            throw new Error("Time tolerance check failed. Host and Redis server times are not aligned, Cannot proceed with operations.");
        }
    }

    private async checkTimeTolerance(): Promise<boolean> {
        const hostTime = BigInt(Date.now());
        let redisTime = 0n;
        const token = this.redisDriver.generateUniqueToken('TimeToleranceCheck');
        try {
            await this.redisDriver.acquire(token);
            const redisTimeArray = await this.redisDriver.run(token, [RedisKeywords.TIME]) as string[];
            const redisSeconds = BigInt(redisTimeArray[0]);
            const redisMicroseconds = BigInt(redisTimeArray[1]);
            redisTime = (redisSeconds * 1000n) + (redisMicroseconds / 1000n);
        }
        finally {
            await this.redisDriver.release(token);
        }
        return Utilities.modMinus(hostTime, this.timeToleranceInMs) == Utilities.modMinus(redisTime, this.timeToleranceInMs);
    }

    public async upsertBulkSamples(samples: ISample[]): Promise<void> {
        const currentTimeWithTolerance = Utilities.modMinus(BigInt(Date.now()), this.timeToleranceInMs);
        const sizeInBytes = this.sizeEstimator(samples);
        const writes = 1n;
        const actualPageCounters = await this.incrementCounter(currentTimeWithTolerance, sizeInBytes, writes);

        const modSize = Utilities.modMinus(actualPageCounters.sizeInBytes, this.sizeWindowInBytes);
        const modWrites = Utilities.modMinus(actualPageCounters.writes, this.writeWindow);
        const pageKey = this.keyBuilder.pageKey(actualPageCounters.timeKey, modSize.toString(), modWrites.toString());

        await this.dumpDataToPage(pageKey, samples, currentTimeWithTolerance, actualPageCounters.writes);
    }

    private async incrementCounter(timeWithTolerance: bigint, sizeInBytes: bigint, writes: bigint): Promise<{ timeKey: string, sizeInBytes: bigint, writes: bigint }> {
        const counterKey = this.keyBuilder.counterKey();
        const returnObject = { timeKey: "", sizeInBytes: 0n, writes: 0n };
        //This is the number of bytes we need to reserve for encoding the time part in the key. 
        //This allows us to have a fixed length binary representation for the time part, 
        //This is max length of 64bit Uint in string length, which is 20 characters, plus some extra padding to be safe.
        // Also 21 bytes are a multiple of 3 get operations in redis using u56 encoding.
        const headerBytes = 21;
        const binaryTimeValue = timeWithTolerance.toString()
            .padStart(headerBytes, "0")
            .substring(0, headerBytes);
        const sizeCounterBitLocation = headerBytes * 8; // Each character is 8 bits.
        const writeCounterBitLocation = sizeCounterBitLocation + 63; // Size counter takes 63 bits, we start the write counter right after that.

        const commands = [
            [RedisKeywords.SET, counterKey, binaryTimeValue, RedisKeywords.SET_ONLY_IF_NO_EXPIRY],
            [
                RedisKeywords.BITFIELD, counterKey, RedisKeywords.OVERFLOW, RedisKeywords.FAIL,
                RedisKeywords.GET, "u56", "#0", RedisKeywords.GET, "u56", "#1", RedisKeywords.GET, "u56", "#2",
                RedisKeywords.INCRBY, "u63", `${sizeCounterBitLocation}`, `${sizeInBytes}`,
                RedisKeywords.INCRBY, "u63", `${writeCounterBitLocation}`, `${writes}`
            ],
            [RedisKeywords.PEXPIRE, counterKey, `${this.timeWindowInMs}`, RedisKeywords.SET_ONLY_IF_NO_EXPIRY]
        ];
        const token = this.redisDriver.generateUniqueToken('IncrementCounter');
        try {
            await this.redisDriver.acquire(token);
            const response = await this.redisDriver.pipeline(token, commands, false) as string[][];
            const buff = Buffer.alloc(headerBytes + 1);
            //Start writing and overwriting from end cause we just have 7 bytes not 8 bytes
            buff.writeBigUInt64BE(BigInt(response[1][2]), (buff.length - (8 - 0)));
            buff.writeBigUInt64BE(BigInt(response[1][1]), (buff.length - (16 - 1)));
            buff.writeBigUInt64BE(BigInt(response[1][0]), (buff.length - (24 - 2)));
            returnObject.timeKey = buff.toString("ascii", 1, buff.length);
            //returnObject.timeKey = `${response[1][0]}${response[1][1]}${response[1][2]}`;
            returnObject.sizeInBytes = BigInt(response[1][3]);
            returnObject.writes = BigInt(response[1][4]);
        }
        finally {
            await this.redisDriver.release(token);
        }
        return returnObject;
    }

    private async dumpDataToPage(pageKey: string, samples: ISample[], insertTime: bigint, currentWriteCount: bigint): Promise<void> {
        const updateBookCommands = this.generateBookUpdateCommand(pageKey, insertTime);
        const pageUpsertCommands = new Map<string, string[]>();[RedisKeywords.ZADD, pageKey];
        for (const sample of samples) {
            const tagKey = this.keyBuilder.tagKey(pageKey, sample.tag);
            const existingCommands = pageUpsertCommands.get(tagKey) || [RedisKeywords.ZADD, tagKey];
            delete sample.tag;
            existingCommands.push(`${sample.ts.toString()}.${currentWriteCount.toString()}`, JSON.stringify(sample));
            pageUpsertCommands.set(tagKey, existingCommands);
        }

        const token = this.redisDriver.generateUniqueToken('DumpDataToPage');
        try {
            await this.redisDriver.acquire(token);
            await this.redisDriver.pipeline(token, [...pageUpsertCommands.values(), ...updateBookCommands], false);
        }
        finally {
            await this.redisDriver.release(token);
        }
    }

    private generateBookUpdateCommand(pageKey: string, insertTime: bigint): string[][] {
        return [
            [RedisKeywords.ZADD, this.keyBuilder.bookKey(), insertTime.toString(), pageKey],
            [RedisKeywords.ZREMRANGEBYRANK, this.keyBuilder.bookKey(), "0", `-${this.maxPagesInBook + 1}`]
        ];
    }

    public async queryRange(tags: string[], startTime: bigint, endTime: bigint, pageSize = 100): Promise<ISample[]> {
        const deDuplicatedTags = this.validateQueryRangeParams(tags, startTime, endTime);
        const rankedPages = await this.fetchAllPagesWithRanks();
        return await this.fetchDataForPagesInRange(deDuplicatedTags, rankedPages, startTime, endTime, pageSize);
    }

    private async fetchAllPagesWithRanks(): Promise<Map<number, string>> {
        const bookKey = this.keyBuilder.bookKey();
        const token = this.redisDriver.generateUniqueToken('QueryRange');
        let pageKeys: string[] = [];
        try {
            await this.redisDriver.acquire(token);
            pageKeys = await this.redisDriver.run(token, [RedisKeywords.ZRANGE, bookKey, "0", "-1", RedisKeywords.WITHSCORES]) as string[];
            const pageKeyMap = new Map<number, string>();
            for (let i = 0; i < pageKeys.length; i += 2) {
                const pageKey = pageKeys[i];
                const score = parseInt(pageKeys[i + 1]);
                pageKeyMap.set(score, pageKey);
            }
            return pageKeyMap;
        }
        finally {
            await this.redisDriver.release(token);
        }
    }

    private validateQueryRangeParams(tags: string[], startTime: bigint, endTime: bigint): string[] {

        if (tags.length === 0) {
            throw new Error("At least one tag must be specified for querying.");
        }
        if (startTime < 0n || endTime < 0n) {
            throw new Error("Start time and end time must be non-negative.");
        }
        if (endTime < startTime) {
            throw new Error("End time must be greater than or equal to start time.");
        }
        if ((endTime - startTime) === 0n) {
            throw new Error(`The difference between end time and start time must be greater than 0. Currently, it is ${endTime - startTime} ms.`);
        }
        if (tags.length > 10) {
            throw new Error("A maximum of 10 tags can be specified for querying to prevent excessive load. Currently, " + tags.length + " tags were provided.");
        }

        return [...(new Set(tags)).values()]
    }

    private async fetchDataForPagesInRange(tagNames: string[], rankedPages: Map<number, string>, startTime: bigint, endTime: bigint, pageSize: number): Promise<ISample[]> {

        const commands: string[][] = [];
        const indexedResponseContext = new Array<{ score: number, tagName: string }>();
        for (const [score, pageKey] of rankedPages) {
            for (const tagName of tagNames) {
                const finalTagKey = this.keyBuilder.tagKey(pageKey, tagName);
                commands.push([RedisKeywords.ZRANGE, finalTagKey, startTime.toString(), endTime.toString(), RedisKeywords.BYSCORE, RedisKeywords.LIMIT, "0", pageSize.toString()]);
                indexedResponseContext.push({ score, tagName });
            }
        }

        const token = this.redisDriver.generateUniqueToken('FetchDataForPagesInRange');
        try {
            await this.redisDriver.acquire(token);
            const responses = await this.redisDriver.pipeline(token, commands, false) as string[][];
            const result = new Map<string, Map<number, IScoredSample>>();
            for (let i = 0; i < responses.length; i++) {
                const response = responses[i];
                const { score, tagName } = indexedResponseContext[i];
                for (let i = 0; i < response.length; i++) {
                    const sample = JSON.parse(response[i]) as ISample;
                    sample.tag = tagName;
                    const existingTagSamples = result.get(sample.tag) ?? new Map<number, IScoredSample>();
                    let existingSample = existingTagSamples.get(sample.ts) ?? { ...sample, score: BigInt(score) };
                    //This is a scenario where updates are spread across multiple pages. 
                    //We need to ensure that we take the latest update for the same timestamp based on the score (which is the insert time of the page).
                    if (existingSample.score < score) {
                        existingSample = { ...sample, score: BigInt(score) };
                    }
                    existingTagSamples.set(sample.ts, existingSample);
                    result.set(sample.tag, existingTagSamples);
                }
            }
            return this.transformScoresToOriginalTimestamps(result);
        }
        finally {
            await this.redisDriver.release(token);
        }
    }

    private transformScoresToOriginalTimestamps(data: Map<string, Map<number, IScoredSample>>): ISample[] {
        const result: ISample[] = [];
        for (const [tagName, samplesByTimestamp] of data) {
            for (const [_, sample] of samplesByTimestamp) {
                const { score, ...originalSample } = sample;
                originalSample.tag = tagName;
                result.push(originalSample);
            }
        }
        return result;
    }
}