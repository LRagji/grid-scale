export interface INonVolatileHashMap {
    set(key: string, fieldValues: string[]): Promise<void>;
    getFields(key: string): Promise<string[]>;
}
