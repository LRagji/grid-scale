export interface INonVolatileHashMap {
    set(key: string, fieldValues: string[]): Promise<void>;
    getFields(key: string): Promise<string[]>;
    getFieldValues(key: string, fields: string[]): Promise<Map<string, string>>;
    rename(key: string, newKey: string): Promise<void>;
    del(keys: string[]): Promise<void>;
}
