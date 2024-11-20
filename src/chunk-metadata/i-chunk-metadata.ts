export interface IChunkMetadata {
    metadataGet(connectionPaths: string[], key: string, defaultValue: string): Promise<Map<string, string[]>>;
    metadataSet(connectionPaths: string[], key: string, value: string): Promise<void>;
}