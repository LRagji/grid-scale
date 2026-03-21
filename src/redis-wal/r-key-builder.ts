export interface IKeyBuilder {
    counterKey(): string;
    pageKey(timeKeyPart: string, sizeKeyPart: string, writeKeyPart: string): string;
    bookKey(): string;
    groupKey(pageKey: string, tagName: string): string;
    groupListKey(pageKey: string): string;
}

export class RKeyBuilder implements IKeyBuilder {
    constructor(
        private readonly keyPrefix: string = "wal",
        private readonly keySeparator: string = ":",
        private readonly counterKeyName: string = "counter",
        private readonly pageKeyName: string = "page",
        private readonly bookKeyName: string = "book",
        private readonly groupKeyName: string = "group"
    ) { }


    public counterKey(): string {
        return `${this.keyPrefix}${this.keySeparator}${this.counterKeyName}`;
    }

    public pageKey(timeKeyPart: string, sizeKeyPart: string, writeKeyPart: string): string {
        return `${this.keyPrefix}${this.keySeparator}${this.pageKeyName}${this.keySeparator}${timeKeyPart}${this.keySeparator}${sizeKeyPart}${this.keySeparator}${writeKeyPart}`;
    }

    public bookKey(): string {
        return `${this.keyPrefix}${this.keySeparator}${this.bookKeyName}`;
    }

    public groupKey(pageKey: string, tagName: string): string {
        return `${pageKey}${this.keySeparator}${tagName}`;
    }

    public groupListKey(pageKey: string): string {
        return `${pageKey}${this.keySeparator}${this.groupKeyName}`;
    }
}