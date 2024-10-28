import { createClient, RedisClientType } from "redis";
import { INonVolatileHashMap } from "./i-non-volatile-hash-map.js";

export class RedisHashMap implements INonVolatileHashMap {

    private readonly redisClient: RedisClientType;

    constructor(clientConnectionInfo: string) {
        this.redisClient = createClient({ url: clientConnectionInfo });
    }

    public async initialize(): Promise<void> {
        await this.redisClient
            .on('error', err => console.log('Redis Client Error', err))
            .connect();
    }

    public async set(key: string, fieldValues: string[]): Promise<void> {
        await this.redisClient.HSET(key, fieldValues);
    }

    public async getFields(key: string): Promise<Array<string>> {
        return await this.redisClient.HKEYS(key) || [];
    }

    public async del(key: string[]): Promise<void> {
        throw new Error("Method not implemented.");
    }

    async [Symbol.asyncDispose]() {
        this.redisClient.removeAllListeners();
        await this.redisClient.quit();
    }
}
