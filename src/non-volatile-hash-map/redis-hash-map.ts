import { createClient, RedisClientType } from "redis";
import { INonVolatileHashMap } from "./i-non-volatile-hash-map.js";

export class RedisHashMap implements INonVolatileHashMap {

    private readonly redisClient: RedisClientType;

    constructor(clientConnectionInfo: string, private readonly prefix: string = "") {
        this.redisClient = createClient({ url: clientConnectionInfo });
    }

    public async initialize(): Promise<void> {
        await this.redisClient
            .on('error', err => console.log('Redis Client Error', err))
            .connect();
    }

    public async set(key: string, fieldValues: string[]): Promise<void> {
        await this.redisClient.HSET(this.prefix + key, fieldValues);
    }

    public async getFields(key: string): Promise<Array<string>> {
        return await this.redisClient.HKEYS(this.prefix + key) || [];
    }

    public async getFieldValues(key: string, fields: string[]): Promise<Map<string, string>> {
        const result = await this.redisClient.HMGET(this.prefix + key, fields);
        const returnValues = new Map<string, string>();
        for (const [idx, value] of result.entries()) {
            if (value !== null) {
                returnValues.set(fields[idx], value);
            }
        }
        return returnValues;
    }

    public async rename(key: string, newKey: string): Promise<void> {
        await this.redisClient.RENAME(this.prefix + key, this.prefix + newKey);
    }

    public async del(keys: string[]): Promise<void> {
        await this.redisClient.DEL(keys.map(key => this.prefix + key));
    }

    async [Symbol.asyncDispose]() {
        this.redisClient.removeAllListeners();
        await this.redisClient.quit();
    }
}
