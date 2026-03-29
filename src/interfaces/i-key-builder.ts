export interface IKeyBuilder {
    counterKey(): string;
    pageKey(timeKeyPart: string, sizeKeyPart: string, writeKeyPart: string): string;
    bookKey(): string;
    dimensionKey(pageKey: string, tagName: string): string;
    pageDimensionsDict(pageKey: string, dimensionName: string): string;
}