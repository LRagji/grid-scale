import { IChunkMetadata } from "../../chunk-metadata/i-chunk-metadata.js";
import { RedisHashMap } from "../../non-volatile-hash-map/redis-hash-map.js";

export class ChunkMetaRegistry implements IChunkMetadata {

    private readonly redisRedisHashMap: RedisHashMap;

    public constructor(connectionString: string, prefix: string) {
        this.redisRedisHashMap = new RedisHashMap(connectionString, prefix);
    }

    async metadataGet(connectionPaths: string[], key: string, defaultValue: string): Promise<Map<string, string[]>> {
        const result = new Map<string, string[]>();
        for (const connectionPath of connectionPaths) {
            const redisResult = await this.redisRedisHashMap.getFieldValues(connectionPath, [key]);
            const value = redisResult.get(key) ?? defaultValue;
            result.set(connectionPath, [value]);
        }
        return result;
    }

    async metadataSet(connectionPaths: string[], key: string, value: string): Promise<void> {

        for (const connectionPath of connectionPaths) {
            await this.redisRedisHashMap.set(connectionPath, [key, value]);
        }
    }

    async initialize(): Promise<void> {
        await this.redisRedisHashMap.initialize();
    }

    async [Symbol.asyncDispose]() {
        await this.redisRedisHashMap[Symbol.asyncDispose]();
    }

}