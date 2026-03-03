export class RedisKeyBuilder {
    constructor(
        private readonly keyPrefix: string = "wal",
        private readonly keySeparator: string = ":"
    ) { }


    public counterKey(timeKeyPart: string): string {
        return `${this.keyPrefix}${this.keySeparator}counter${this.keySeparator}${timeKeyPart}`;
    }

    public pageKey(timeKeyPart: string, sizeKeyPart: string, writeKeyPart: string): string {
        return `${this.keyPrefix}${this.keySeparator}page${this.keySeparator}${timeKeyPart}${this.keySeparator}${sizeKeyPart}${this.keySeparator}${writeKeyPart}`;
    }

    public bookKey(): string {
        return `${this.keyPrefix}${this.keySeparator}book`;
    }

    public tagKey(pageKey: string, tagName: string): string {
        return `${this.keyPrefix}${this.keySeparator}tag${this.keySeparator}${pageKey}${this.keySeparator}${tagName}`;
    }
}