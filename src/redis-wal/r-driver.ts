import { IRedisClientPool } from "redis-abstraction";
import { IRDriver, RedisKeywords } from "../interfaces/i-r-driver";
import { Utilities } from "../utilities";


export class RDriver implements IRDriver {

    constructor(
        private readonly redisDriver: IRedisClientPool,
        //Defaults.
        public readonly timeToleranceInMs: number = 1 * 60 * 1000, // 1 minute
    ) {
        if (this.timeToleranceInMs <= 1000 || this.timeToleranceInMs > Utilities.u48In3) {
            throw new Error("Time tolerance must be between 1 second and " + Utilities.u48In3 + " ms. Currently, it is set to " + this.timeToleranceInMs.toString() + " ms.");
        }

    }

    public async initialize(): Promise<void> {
        await this.redisDriver.initialize();

        const timeAligned = await this.checkTimeTolerance();
        if (!timeAligned) {
            throw new Error("Time tolerance check failed. Host and Redis server times are not aligned, Cannot proceed with operations.");
        }
    }

    private async checkTimeTolerance(hostTime = Date.now()): Promise<boolean> {
        let redisTime = 0;
        const redisTimeArray = await this.usingRedisDriver<string[]>([[RedisKeywords.TIME]], 'TimeToleranceCheck', 'run');
        const redisSeconds = parseInt(redisTimeArray[0], 10);
        const redisMicroseconds = parseInt(redisTimeArray[1], 10);
        redisTime = (redisSeconds * 1000) + (redisMicroseconds / 1000);

        return Utilities.modMinus(hostTime, this.timeToleranceInMs) == Utilities.modMinus(redisTime, this.timeToleranceInMs);
    }

    public async usingRedisDriver<T>(commands: any[][], tokenName: string, type: "run" | "pipeline" = "pipeline"): Promise<T> {
        const token = this.redisDriver.generateUniqueToken(tokenName);
        try {
            await this.redisDriver.acquire(token);
            if (type === "pipeline") {
                return await this.redisDriver.pipeline(token, commands, false) as unknown as T;
            } else {
                return await this.redisDriver.run(token, commands[0]) as unknown as T;
            }
        }
        finally {
            await this.redisDriver.release(token);
        }
    }

}