// import { IRedisClientPool } from "redis-abstraction";
// import { IKeyBuilder, RedisKeyBuilder } from "./redis-key-builder.js";
// import { ISample, ISampleSet, IScoredSample } from "../interfaces/i-sample.js";
// import { Utilities } from "../utilities.js";

// class RedisKeywords {
//     static BITFIELD = "bitfield";
//     static INCRBY = "incrby";
//     static OVERFLOW = "overflow";
//     static FAIL = "fail";
//     static PEXPIRE = "pexpire";
//     static ZADD = "zadd";
//     static ZREMRANGEBYRANK = "zremrangebyrank";
//     static TIME = "time";
//     static ZRANGE = "zrange";
//     static BYSCORE = "byscore";
//     static LIMIT = "limit";
//     static WITHSCORES = "withscores";
//     static SET_ONLY_IF_NO_EXPIRY = "nx";
//     static SET = "set";
//     static GET = "get";
// }

// export class RedisWAL {

//     constructor(
//         private readonly redisDriver: IRedisClientPool,
//         //Defaults.
//         private readonly timeToleranceInMs: number = 1 * 60 * 1000, // 1 minute
//         private readonly timeWindowInMs: number = 24 * 60 * 60 * 1000, // 24 hours
//         private readonly sizeWindowInBytes: number = Utilities.u48In3,
//         private readonly writeWindow: number = Utilities.u48In3,
//         private readonly keyBuilder: IKeyBuilder = new RedisKeyBuilder(),
//         private readonly sizeEstimator: (samples: ISample[]) => number = Utilities.roughSizeEstimator,
//         private readonly maxPagesInBook: number = 100,
//         private readonly newPageCallback: (pageKey: string) => Promise<void> = async (_pageKey: string) => { }
//     ) {
//         if (this.timeToleranceInMs <= 1000 || this.timeToleranceInMs > Utilities.u48In3) {
//             throw new Error("Time tolerance must be between 1 second and " + Utilities.u48In3 + " ms. Currently, it is set to " + this.timeToleranceInMs.toString() + " ms.");
//         }
//         if (this.timeWindowInMs <= 1000 || this.timeWindowInMs > Utilities.u48In3) {
//             throw new Error("Time window must be between 1 second and " + Utilities.u48In3 + " ms. Currently, it is set to " + this.timeWindowInMs.toString() + " ms.");
//         }
//         if (this.sizeWindowInBytes <= 0 || this.sizeWindowInBytes > Utilities.u48In3) {
//             throw new Error("Size window must be between 0 and " + Utilities.u48In3 + ". Currently, it is set to " + this.sizeWindowInBytes.toString() + " bytes.");
//         }
//         if (this.writeWindow <= 0 || this.writeWindow > Utilities.u48In3) {
//             throw new Error("Write window must be between 0 and " + Utilities.u48In3 + ". Currently, it is set to " + this.writeWindow.toString() + " writes.");
//         }
//         if (this.maxPagesInBook <= 0 || this.maxPagesInBook > 1000) {
//             throw new Error("Max pages in book must be between 1 and 1000. Currently, it is set to " + this.maxPagesInBook + " pages.");
//         }
//         if (this.timeToleranceInMs >= this.timeWindowInMs) {
//             throw new Error("Time tolerance must be less than time window to ensure proper functioning of the system. Currently, time tolerance is " + this.timeToleranceInMs.toString() + " ms and time window is " + this.timeWindowInMs.toString() + " ms.");
//         }
//     }

//     public async initialize(): Promise<void> {

//         await this.redisDriver.initialize();

//         const timeAligned = await this.checkTimeTolerance();
//         if (!timeAligned) {
//             throw new Error("Time tolerance check failed. Host and Redis server times are not aligned, Cannot proceed with operations.");
//         }
//     }

//     private async checkTimeTolerance(): Promise<boolean> {
//         const hostTime = Date.now();
//         let redisTime = 0;
//         const redisTimeArray = await this.usingRedisDriver<string[]>([[RedisKeywords.TIME]], 'TimeToleranceCheck', 'run');
//         const redisSeconds = parseInt(redisTimeArray[0], 10);
//         const redisMicroseconds = parseInt(redisTimeArray[1], 10);
//         redisTime = (redisSeconds * 1000) + (redisMicroseconds / 1000);

//         return Utilities.modMinus(hostTime, this.timeToleranceInMs) == Utilities.modMinus(redisTime, this.timeToleranceInMs);
//     }

//     public async upsertBulkSamples(samples: ISample[]): Promise<void> {
//         const currentTimeWithTolerance = Utilities.modMinus(Date.now(), this.timeToleranceInMs);
//         const sizeInBytes = this.sizeEstimator(samples);
//         const writes = 1;
//         const actualPageCounters = await this.incrementCounter(currentTimeWithTolerance, sizeInBytes, writes);

//         const modSize = Utilities.modMinus(actualPageCounters.sizeInBytes, this.sizeWindowInBytes);
//         const modWrites = Utilities.modMinus(actualPageCounters.writes, this.writeWindow);
//         const pageKey = this.keyBuilder.pageKey(actualPageCounters.timeKey, modSize.toString(), modWrites.toString());

//         await this.dumpDataToPage(pageKey, samples, currentTimeWithTolerance, actualPageCounters.writes, actualPageCounters.sizeInBytes);
//         if (actualPageCounters.newPage) {
//             await this.newPageCallback(pageKey);
//         }
//     }

//     private async incrementCounter(timeWithTolerance: number, sizeInBytes: number, writes: number): Promise<{ timeKey: string, sizeInBytes: number, writes: number, newPage: boolean }> {
//         const counterKey = this.keyBuilder.counterKey();
//         const returnObject = { timeKey: "", sizeInBytes: 0, writes: 0, newPage: false };
//         // Current js engine v8 only guarantees 53 bit precision for integers, so we use 48 bits for the time header.
//         // We use the same 48 bits for counter sizes etc.
//         const headerBytes = 6;
//         const timeHeaderBuffer = Buffer.alloc(headerBytes);
//         timeHeaderBuffer.writeUintBE(Number(timeWithTolerance), 0, headerBytes);

//         const commands = [
//             [RedisKeywords.SET, counterKey, timeHeaderBuffer as any, RedisKeywords.SET_ONLY_IF_NO_EXPIRY],
//             [
//                 RedisKeywords.BITFIELD, counterKey, RedisKeywords.OVERFLOW, RedisKeywords.FAIL,
//                 RedisKeywords.GET, "u48", "#0",
//                 RedisKeywords.INCRBY, "u48", "#1", `${sizeInBytes}`,
//                 RedisKeywords.INCRBY, "u48", "#2", `${writes}`
//             ],
//             [RedisKeywords.PEXPIRE, counterKey, `${this.timeWindowInMs}`, RedisKeywords.SET_ONLY_IF_NO_EXPIRY]
//         ];

//         const response = await this.usingRedisDriver<string[][]>(commands, 'IncrementCounter');
//         returnObject.timeKey = response[1][0].toString();
//         returnObject.sizeInBytes = parseInt(response[1][1], 10);
//         returnObject.writes = parseInt(response[1][2], 10);
//         returnObject.newPage = (response[0] ?? "").toString().toLowerCase() === "ok";
//         if (returnObject.newPage && returnObject.timeKey !== Number(timeWithTolerance).toString()) {
//             throw new Error(`System Error:Time key mismatch when creating new page. Expected: ${Number(timeWithTolerance).toString()}, Actual: ${returnObject.timeKey}. This indicates a potential issue with time alignment between host and Redis server.`);
//         }
//         return returnObject;
//     }

//     private async usingRedisDriver<T>(commands: any[][], tokenName: string, type: "run" | "pipeline" = "pipeline"): Promise<T> {
//         const token = this.redisDriver.generateUniqueToken(tokenName);
//         try {
//             await this.redisDriver.acquire(token);
//             if (type === "pipeline") {
//                 return await this.redisDriver.pipeline(token, commands, false) as unknown as T;
//             } else {
//                 return await this.redisDriver.run(token, commands[0]) as unknown as T;
//             }
//         }
//         finally {
//             await this.redisDriver.release(token);
//         }
//     }

//     private async dumpDataToPage(pageKey: string, samples: ISample[], insertTime: number, currentWriteCount: number, currentByteCount: number): Promise<void> {
//         const updateBookCommands = this.generateBookUpdateCommand(pageKey, insertTime);
//         const pageUpsertCommands = new Map<string, string[]>();[RedisKeywords.ZADD, pageKey];
//         for (const sample of samples) {
//             const tagKey = this.keyBuilder.groupKey(pageKey, sample.tag);
//             const existingCommands = pageUpsertCommands.get(tagKey) || [RedisKeywords.ZADD, tagKey];
//             const clonedSample = structuredClone(sample);
//             delete clonedSample.tag;
//             existingCommands.push(`${sample.ts.toString()}.${currentWriteCount.toString()}`, JSON.stringify(clonedSample));
//             pageUpsertCommands.set(tagKey, existingCommands);
//         }

//         await this.usingRedisDriver<void>([...pageUpsertCommands.values(), ...updateBookCommands], 'DumpDataToPage')
//     }

//     private generateBookUpdateCommand(pageKey: string, insertTime: number): string[][] {
//         return [
//             [RedisKeywords.ZADD, this.keyBuilder.bookKey(), insertTime.toString(), pageKey],
//             [RedisKeywords.ZREMRANGEBYRANK, this.keyBuilder.bookKey(), "0", `-${this.maxPagesInBook + 1}`]
//         ];
//     }

//     // private generateUniqueSortablePageScore(currentTime: number, sizeInBytes: number, writes: number): number {
//     //     //Current time is absolute, we need to convert it to a relative time based on the time window to ensure that scores are always increasing and we don't run into issues with redis sorted set score limits.
//     //     const relativeTime = currentTime - Utilities.modMinus(currentTime, this.timeToleranceInMs);
//     //     // We combine time, size and writes into a single score to ensure total ordering of pages. Time is the most significant factor, followed by size and then writes.
//     //     return (relativeTime * Utilities.u48In3 * Utilities.u48In3) + (sizeInBytes * Utilities.u48In3) + writes;
//     //     //Why all this?
//     //     // 1. We need to ensure that the score generated within the page is always unique and sortable.
//     //     // 2. Redis only supports upto 53 bits of score precision, we use 48 bits space and divide it into 3 parts for each counter for the page.
//     //     // 3. Since the upper bound is capped by 48/3 space, with validations in constructor, we simply use math to construct the correct score.
//     // }

//     public async queryRange(tags: string[], startTime: number, endTime: number, pageSize = 100): Promise<ISampleSet[]> {
//         const deDuplicatedTags = this.validateQueryRangeParams(tags, startTime, endTime);
//         const rankedPages = await this.fetchAllPagesWithRanks();
//         return await this.fetchDataForPagesInRange(deDuplicatedTags, rankedPages, startTime, endTime, pageSize);
//     }

//     private async fetchAllPagesWithRanks(): Promise<Map<string, number>> {
//         const bookKey = this.keyBuilder.bookKey();
//         const pageKeys = await this.usingRedisDriver<string[]>([[RedisKeywords.ZRANGE, bookKey, "0", "-1", RedisKeywords.WITHSCORES]], 'FetchAllPagesWithRanks', "run");
//         const pageKeyMap = new Map<string, number>();
//         for (let i = 0; i < pageKeys.length; i += 2) {
//             const pageKey = pageKeys[i];
//             const score = parseInt(pageKeys[i + 1], 10);
//             pageKeyMap.set(pageKey, score);
//         }
//         return pageKeyMap;
//     }

//     private validateQueryRangeParams(tags: string[], startTime: number, endTime: number): string[] {

//         if (tags.length === 0) {
//             throw new Error("At least one tag must be specified for querying.");
//         }
//         if (startTime < 0 || endTime < 0) {
//             throw new Error("Start time and end time must be non-negative.");
//         }
//         if (endTime < startTime) {
//             throw new Error("End time must be greater than or equal to start time.");
//         }
//         if ((endTime - startTime) === 0) {
//             throw new Error(`The difference between end time and start time must be greater than 0. Currently, it is ${endTime - startTime} ms.`);
//         }
//         if (tags.length > 10) {
//             throw new Error("A maximum of 10 tags can be specified for querying to prevent excessive load. Currently, " + tags.length + " tags were provided.");
//         }

//         return [...(new Set(tags)).values()]
//     }

//     private async fetchDataForPagesInRange(tagNames: string[], rankedPages: Map<string, number>, startTime: number, endTime: number, pageSize: number): Promise<ISampleSet[]> {

//         const commands: string[][] = [];
//         const indexedResponseContext = new Array<{ pageRank: number, tagName: string }>();
//         for (const [pageKey, pageRank] of rankedPages) {
//             for (const tagName of tagNames) {
//                 const finalTagKey = this.keyBuilder.groupKey(pageKey, tagName);
//                 commands.push([RedisKeywords.ZRANGE, finalTagKey, startTime.toString(), endTime.toString(), RedisKeywords.BYSCORE, RedisKeywords.WITHSCORES, RedisKeywords.LIMIT, "0", pageSize.toString()]);
//                 indexedResponseContext.push({ pageRank, tagName });
//             }
//         }

//         const responses = await this.usingRedisDriver<string[][]>(commands, 'FetchDataForPagesInRange');

//         const result = new Map<string, Map<number, IScoredSample>>();
//         for (let i = 0; i < responses.length; i++) {
//             const response = responses[i] ?? [];
//             const { pageRank, tagName } = indexedResponseContext[i];
//             for (let i = 0; i < response.length; i++) {
//                 const sample = JSON.parse(response[i]) as ISample;
//                 const writeScore = parseFloat(response[++i]);
//                 sample.tag = tagName;
//                 const existingTagSamples = result.get(sample.tag) ?? new Map<number, IScoredSample>();
//                 let existingSample = existingTagSamples.get(sample.ts) ?? { ...sample, pageRank, writeScore };
//                 //This is a scenario where updates are spread across multiple pages. 
//                 //We need to ensure that we take the latest update for the same timestamp based on the score (which is the insert time of the page).
//                 if ((existingSample.pageRank < pageRank) || (existingSample.pageRank === pageRank && existingSample.writeScore < writeScore)) {
//                     existingSample = { ...sample, pageRank, writeScore };
//                 }
//                 existingTagSamples.set(sample.ts, existingSample);
//                 result.set(sample.tag, existingTagSamples);
//             }
//         }
//         return this.transformScoresToOriginalTimestamps(result);
//     }

//     private transformScoresToOriginalTimestamps(data: Map<string, Map<number, IScoredSample>>): ISampleSet[] {
//         const result = new Map<string, ISampleSet>();
//         for (const [tagName, samplesByTimestamp] of data) {
//             for (const [_, sample] of samplesByTimestamp) {
//                 const { pageRank: score, ...originalSample } = sample;
//                 originalSample.tag = tagName;
//                 const existingSampleSet = result.get(tagName) ?? { tag: tagName, samples: [], minTs: Number.MAX_SAFE_INTEGER, maxTs: Number.MIN_SAFE_INTEGER, count: 0 };
//                 existingSampleSet.samples.push(originalSample);
//                 existingSampleSet.minTs = Math.min(existingSampleSet.minTs, originalSample.ts);
//                 existingSampleSet.maxTs = Math.max(existingSampleSet.maxTs, originalSample.ts);
//                 existingSampleSet.count += 1;
//                 result.set(tagName, existingSampleSet);
//             }
//         }
//         return Array.from(result.values());
//     }
// }