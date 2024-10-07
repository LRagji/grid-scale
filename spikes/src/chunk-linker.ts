import { createClient, RedisClientType } from "redis";
import { ChunkId } from "./chunk.js";
import { TConfig } from "./t-config.js";

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

    public async get(key: string, fieldsOnly = true): Promise<string[]> {
        if (fieldsOnly === true) {
            return await this.redisClient.HKEYS(key) || [];
        }
        else {
            return [];
        }
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

    public async link(indexed: ChunkId, unIndexed: ChunkId[], selfId: string): Promise<void> {
        if (this.isInitialized === false) {
            await this.redis.initialize();
            this.isInitialized = true;
        }
        const indexedKey = indexed.logicalChunkId;
        const values = [];
        for (const item of unIndexed) {
            values.push(item);
            values.push(selfId);
        }

        await this.redis.set(indexedKey, values);
    }

    public async getRelated(b: ChunkId): Promise<string[]> {
        if (this.isInitialized === false) {
            await this.redis.initialize();
            this.isInitialized = true;
        }
        const key = b.logicalChunkId;
        const values = await this.redis.get(key, true);
        return values;
    }

    public async [Symbol.asyncDispose]() {
        if (this.isInitialized === true) {
            await this.redis[Symbol.asyncDispose]();
        }
    }

}