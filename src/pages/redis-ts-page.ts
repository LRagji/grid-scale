import { IDimensionalElement } from "../interfaces/i-dimensional-element.js";
import { IDimensionalQuery } from "../interfaces/i-dimensional-query.js";
import { IPageInfo } from "../interfaces/i-page-info.js";
import { IPage } from "../interfaces/i-page.js";
import { IRDriver, RedisKeywords } from "../interfaces/i-r-driver.js";
import { IKeyBuilder, RKeyBuilder } from "../redis-wal/r-key-builder.js";

export class RedisTsPage implements IPage {

    public get info(): IPageInfo {
        return this._pageInfo;
    }
    public get pageType(): string {
        return "REDIS_TS_PAGE";
    }

    constructor(private readonly _pageInfo: IPageInfo,
        private readonly redisDriver: IRDriver,
        private readonly keyBuilder: IKeyBuilder = new RKeyBuilder(),
        private readonly pageDimensionsDicTTLms: number = 24 * 60 * 60 * 1000 // 24 hours,
    ) { }

    public async upsertElements(elements: IDimensionalElement[], sequenceStart: number)
    public async upsertElements(elements: IDimensionalElement[], sequenceStart: number, stringSearchDimSet: Set<string> = new Set(), numericSearchDimSet: Set<string> = new Set()): Promise<void> {
        // const pageUpsertCommands = new Map<string, string[]>();
        // let elementCounter = sequenceStart;

        // for (const element of elements) {
        //     const elementGroupKey = this.keyBuilder.dimensionKey(this._pageInfo.pageKey, element.dim as string);
        //     const existingCommands = pageUpsertCommands.get(elementGroupKey) || [RedisKeywords.ZADD, elementGroupKey];
        //     element.mvccId = elementCounter;// We can do a structure clone here, but to avoid perf overhead we will mutate the original element.
        //     existingCommands.push(element.elementRank.toString(), JSON.stringify(element));
        //     pageUpsertCommands.set(elementGroupKey, existingCommands);
        //     elementCounter++;
        // }
        // const groupListKey = this.keyBuilder.pageDimensionsDict(this._pageInfo.pageKey);
        // const commands = [...pageUpsertCommands.values()];
        // commands.push([RedisKeywords.SADD, groupListKey, ...[...pageUpsertCommands.keys()]]);//Add all groups to group list for the page, this will help us in fetching and purging data for the page.
        // commands.push([RedisKeywords.PEXPIRE, groupListKey, `${this.pageDimensionsDicTTLms}`]);

        // await this.redisDriver.usingRedisDriver<void>(commands, 'DumpDataToPage', 'pipeline')
    }

    public queryElementsByDimensions(query: IDimensionalQuery): Promise<IDimensionalElement[]> {
        throw new Error("Method not implemented.");
    }
    public dumpPage(): Promise<IDimensionalElement[]> {
        throw new Error("Method not implemented.");
    }
    public fetchElementsByRange(groupKeys: string[], startInclusiveRank: number, endExclusiveRank: number, maxElementsPerGroup: number): Promise<IDimensionalElement[]> {
        throw new Error("Method not implemented.");
    }

}