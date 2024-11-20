import { IChunkMetadata } from "../../chunk-metadata/i-chunk-metadata.js";

export class ChunkMetaRegistry implements IChunkMetadata {

    private readonly metadata = new Map<string, Map<string, string>>();

    async metadataGet(connectionPaths: string[], key: string, defaultValue: string): Promise<Map<string, string[]>> {
        const result = new Map<string, string[]>();
        for (const connectionPath of connectionPaths) {
            const propMap = this.metadata.get(connectionPath) ?? new Map<string, string>();
            const value = propMap.get(key) ?? defaultValue;
            result.set(connectionPath, [value]);
        }
        return result;
    }

    async metadataSet(connectionPaths: string[], key: string, value: string): Promise<void> {
        for (const connectionPath of connectionPaths) {
            const propMap = this.metadata.get(connectionPath) ?? new Map<string, string>();
            propMap.set(key, value);
            this.metadata.set(connectionPath, propMap);
        }
    }

}