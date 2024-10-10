import { createClient, RedisClientType } from "redis";
import { TConfig } from "./t-config.js";
import { logicalChunkId } from "./grid-scale.js";

class RedisHashMap {

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

    public async get(key: string): Promise<Record<string, string>> {
        return await this.redisClient.HGETALL(key) || {};
    }

    public async del(key: string[]): Promise<void> {
        throw new Error("Method not implemented.");
    }

    async [Symbol.asyncDispose]() {
        this.redisClient.removeAllListeners();
        await this.redisClient.quit();
    }
}


export class ChunkLinker {

    private readonly redis: RedisHashMap;
    private isInitialized = false;
    constructor(private readonly config: TConfig) {
        this.redis = new RedisHashMap(config.redisConnection);
    }

    public async link(indexed: logicalChunkId, unIndexed: logicalChunkId[], unIndexedBucketedTime: number, selfId: string): Promise<void> {
        if (this.isInitialized === false) {
            await this.redis.initialize();
            this.isInitialized = true;
        }

        const values = [];
        for (const item of unIndexed) {
            values.push(item);
            values.push(JSON.stringify([unIndexedBucketedTime, selfId]));
        }

        await this.redis.set(indexed, values);
    }

    public async getRelated(indexed: logicalChunkId): Promise<Record<string, { selfId: string, bucketedTime: number }>> {
        if (this.isInitialized === false) {
            await this.redis.initialize();
            this.isInitialized = true;
        }

        const values = await this.redis.get(indexed);
        const returnValues: Record<string, { selfId: string, bucketedTime: number }> = {}
        Object.keys(values).forEach((key) => {
            const [bucketedTime, selfId] = JSON.parse(values[key]);
            returnValues[key] = { selfId, bucketedTime };
        });
        return returnValues;
    }

    public async [Symbol.asyncDispose]() {
        if (this.isInitialized === true) {
            await this.redis[Symbol.asyncDispose]();
        }
    }

}