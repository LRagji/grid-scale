import { IRDriver, RedisKeywords } from "../interfaces/i-r-driver";
import { Utilities } from "../utilities";
import { RPage } from "./r-page";
import { IKeyBuilder, RedisKeyBuilder } from "./redis-key-builder";



export class RBook {

    constructor(
        private readonly redisDriver: IRDriver,
        //Defaults.
        private readonly timeWindowInMs: number = 24 * 60 * 60 * 1000, // 24 hours
        private readonly sizeWindowInBytes: number = Utilities.u48In3,
        private readonly writeWindow: number = Utilities.u48In3,
        private readonly maxPagesInBook: number = 100,
        private readonly keyBuilder: IKeyBuilder = new RedisKeyBuilder(),
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

    public async openPageForWrites(estimatedSize: number): Promise<void> {
        if (estimatedSize <= 0 || estimatedSize > Utilities.u48In3) {
            throw new Error("Estimated size must be between 0 and " + Utilities.u48In3 + ". Currently, it is set to " + estimatedSize.toString() + " bytes.");
        }

        const currentTimeWithTolerance = Utilities.modMinus(Date.now(), this.redisDriver.timeToleranceInMs);
        const writeablePage = await this.incrementCounter(currentTimeWithTolerance, estimatedSize, 1);

        if (writeablePage.newPage) {
            await this.newPageCallback(writeablePage.pageKey);
        }
    }

    private async incrementCounter(timeWithTolerance: number, sizeInBytes: number, writes: number): Promise<RPage> {
        const counterKey = this.keyBuilder.counterKey();
        // const returnObject = { timeKey: "", sizeInBytes: 0, writes: 0, newPage: false };
        // Current js engine v8 only guarantees 53 bit precision for integers, so we use 48 bits for the time header.
        // We use the same 48 bits for counter sizes etc.
        const headerBytes = 6;
        const timeHeaderBuffer = Buffer.alloc(headerBytes);
        timeHeaderBuffer.writeUintBE(Number(timeWithTolerance), 0, headerBytes);

        const commands = [
            [RedisKeywords.SET, counterKey, timeHeaderBuffer as any, RedisKeywords.SET_ONLY_IF_NO_EXPIRY],
            [
                RedisKeywords.BITFIELD, counterKey, RedisKeywords.OVERFLOW, RedisKeywords.FAIL,
                RedisKeywords.GET, "u48", "#0",
                RedisKeywords.INCRBY, "u48", "#1", `${sizeInBytes}`,
                RedisKeywords.INCRBY, "u48", "#2", `${writes}`
            ],
            [RedisKeywords.PEXPIRE, counterKey, `${this.timeWindowInMs}`, RedisKeywords.SET_ONLY_IF_NO_EXPIRY]
        ];

        const response = await this.redisDriver.usingRedisDriver<string[][]>(commands, 'IncrementCounter', 'pipeline');
        const returnPage = new RPage(
            parseInt(response[1][0].toString(), 10),
            parseInt(response[1][1], 10),
            parseInt(response[1][2], 10),
            (response[0] ?? "").toString().toLowerCase() === "ok",
            this.redisDriver,
            this.timeWindowInMs,
            this.sizeWindowInBytes,
            this.writeWindow,
            this.maxPagesInBook,
            this.keyBuilder
        );
        if (returnPage.newPage && returnPage.timeKeyPart !== timeWithTolerance) {
            throw new Error(`System Error:Time key mismatch when creating new page. Expected: ${Number(timeWithTolerance).toString()}, Actual: ${returnPage.timeKeyPart}. This indicates a potential issue with time alignment between host and Redis server.`);
        }
        return returnPage;
    }

}