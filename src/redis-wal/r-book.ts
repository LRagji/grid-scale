import { IRDriver, RedisKeywords } from "../interfaces/i-r-driver.js";
import { Utilities } from "../utilities.js";
import { RPage } from "./r-page.js";
import { IKeyBuilder, RKeyBuilder } from "./r-key-builder.js";

interface IPageInfo {
    pageKey: string;
    modSize: number;
    modWrites: number;
    modTime: number;
}

export class RBook {
    private readonly counterBytes = 6;//Only u48 so its 6 bytes
    private timeCounterBuffer = Buffer.alloc(this.counterBytes);

    constructor(
        public readonly redisDriver: IRDriver,
        //Defaults.
        private readonly timeWindowInMs: number = 24 * 60 * 60 * 1000, // 24 hours
        private readonly sizeWindowInBytes: number = Utilities.u48In3,
        private readonly writeWindow: number = Utilities.u48In3,
        private readonly maxPagesInBook: number = 100,
        public readonly keyBuilder: IKeyBuilder = new RKeyBuilder(),
        private readonly newPageCallback: (pageKey: string) => Promise<void> = async (_pageKey: string) => { }
    ) {
        if (this.timeWindowInMs <= 1000 || this.timeWindowInMs > Utilities.u48In3) {
            throw new Error("Time window must be between 1 second and " + Utilities.u48In3 + " ms. Currently, it is set to " + this.timeWindowInMs.toString() + " ms.");
        }
        if (this.sizeWindowInBytes <= 0 || this.sizeWindowInBytes > Utilities.u48In3) {
            throw new Error("Size window must be between 0 and " + Utilities.u48In3 + ". Currently, it is set to " + this.sizeWindowInBytes.toString() + " bytes.");
        }
        if (this.writeWindow <= 0 || this.writeWindow > Utilities.u48In3) {
            throw new Error("Write window must be between 0 and " + Utilities.u48In3 + ". Currently, it is set to " + this.writeWindow.toString() + " writes.");
        }
        if (this.maxPagesInBook <= 0 || this.maxPagesInBook > 1000) {
            throw new Error("Max pages in book must be between 1 and 1000. Currently, it is set to " + this.maxPagesInBook + " pages.");
        }
        if (this.redisDriver.timeToleranceInMs >= this.timeWindowInMs) {
            throw new Error("Time tolerance must be less than time window to ensure proper functioning of the system. Currently, time tolerance is " + this.redisDriver.timeToleranceInMs.toString() + " ms and time window is " + this.timeWindowInMs.toString() + " ms.");
        }
    }

    public async fetchWriteablePage(timeInMs: number, sizeInBytes: number, count: number): Promise<{ page: RPage, sequenceStartNumber: number }> {

        const insertTimeWithTolerance = Utilities.modMinus(timeInMs, this.redisDriver.timeToleranceInMs);

        const { pageKey, modeTime, modSize, modWrites, sequenceStartNumber } = await this.incrementAndGenerateKey(insertTimeWithTolerance, sizeInBytes, count);

        const newPage = await this.upsertPageInfo(pageKey, insertTimeWithTolerance, modeTime, modSize, modWrites);

        if (newPage === true) {
            await this.newPageCallback(pageKey);
        }

        const page = new RPage(this.redisDriver, pageKey, this.keyBuilder);

        return { page, sequenceStartNumber };
    }

    private async incrementAndGenerateKey(insertTimeWithTolerance: number, sizeInBytes: number, count: number): Promise<{ pageKey: string, modeTime: number, modSize: number, modWrites: number, sequenceStartNumber: number }> {
        const counterKey = this.keyBuilder.counterKey();
        // Current js engine v8 only guarantees 53 bit precision for integers, so we use 48 bits for the time header.
        // We use the same 48 bits for counter sizes etc.
        // Redis is the ultimate decider of when page turns cause of time by using redis server time via key expiry.
        // Rest 2 counters are expected no to overflow within this time window and will be use to determine the page name.
        this.timeCounterBuffer.writeUintBE(insertTimeWithTolerance, 0, this.counterBytes);

        const commands = [
            [RedisKeywords.SET, counterKey, this.timeCounterBuffer as any, RedisKeywords.SET_ONLY_IF_NO_EXPIRY],
            [
                RedisKeywords.BITFIELD, counterKey, RedisKeywords.OVERFLOW, RedisKeywords.FAIL,
                RedisKeywords.GET, "u48", "#0",
                RedisKeywords.INCRBY, "u48", "#1", `${sizeInBytes}`,
                RedisKeywords.INCRBY, "u48", "#2", `${count}`
            ],
            [RedisKeywords.PEXPIRE, counterKey, `${this.timeWindowInMs}`, RedisKeywords.SET_ONLY_IF_NO_EXPIRY]//This is how redis mod-minus time for us, using its own clock
        ];

        const response = await this.redisDriver.usingRedisDriver<string[][]>(commands, 'IncrementCounter', 'pipeline');

        const receivedTime = parseInt(response[1][0].toString(), 10); // Time counter is modded by redis server time via key expiry, so no need to mode it.
        const sizeCounter = parseInt(response[1][1].toString(), 10); // This counter is always increasing so needs to be modded to determine page key.
        const writeCounter = parseInt(response[1][2].toString(), 10); // This counter is always increasing so needs to be modded to determine page key.

        const modSize = Utilities.modMinus(sizeCounter, this.sizeWindowInBytes);
        const modWrites = Utilities.modMinus(writeCounter, this.writeWindow);
        const pageKey = this.keyBuilder.pageKey(receivedTime.toString(), modSize.toString(), modWrites.toString());
        const sequenceStartNumber = writeCounter - count;

        return { pageKey, modeTime: receivedTime, modSize, modWrites, sequenceStartNumber };
    }

    private async upsertPageInfo(pageKey: string, insertTime: number, modTime: number, modSize: number, modWrites: number): Promise<boolean> {
        const upsertTrimCommands = [
            [RedisKeywords.ZADD, this.keyBuilder.bookKey(), insertTime.toString(), JSON.stringify({ pageKey, modSize, modWrites, modTime } as IPageInfo)],
            [RedisKeywords.ZREMRANGEBYRANK, this.keyBuilder.bookKey(), "0", `-${this.maxPagesInBook + 1}`]
        ];
        const response = await this.redisDriver.usingRedisDriver<void>(upsertTrimCommands, 'UpdateBookForNewPage', 'pipeline');
        return response[0] === "OK" || response[0] === 1; // This means a new page was added.
    }

    public async fetchAvailablePagesWithRanks(): Promise<Map<RPage, number>> {
        const bookKey = this.keyBuilder.bookKey();
        const pageKeys = await this.redisDriver.usingRedisDriver<string[]>([[RedisKeywords.ZRANGE, bookKey, "0", "-1"]], 'FetchAllPagesWithRanks', "run");
        const sortedPageInfo = pageKeys
            .map(serializedPageInfo => JSON.parse(serializedPageInfo) as IPageInfo)
            .sort((aObj: IPageInfo, bObj: IPageInfo) => {
                if (aObj.modTime !== bObj.modTime) {
                    return aObj.modTime - bObj.modTime;
                }
                if (aObj.modSize !== bObj.modSize) {
                    return aObj.modSize - bObj.modSize;
                }
                return aObj.modWrites - bObj.modWrites;
            })
            .map((pageInfo, index) => ([new RPage(this.redisDriver, pageInfo.pageKey, this.keyBuilder), index] as [RPage, number]));
        return new Map<RPage, number>(sortedPageInfo);
    }
}