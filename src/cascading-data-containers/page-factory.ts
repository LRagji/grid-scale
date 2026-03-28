import { IPage } from "../interfaces/i-page";
import { IPageInfo } from "../interfaces/i-page-info";
import { IRDriver } from "../interfaces/i-r-driver";
import { RedisTsPage } from "../pages/redis-ts-page";
import { IKeyBuilder, RKeyBuilder } from "../redis-wal/r-key-builder";

export class PageFactory {

    constructor(private readonly _pageInfo: IPageInfo,
        private readonly redisDriver: IRDriver,
        private readonly keyBuilder: IKeyBuilder = new RKeyBuilder(),
    ) { }

    public makePage(pageInfo: IPageInfo, pageType: string): Promise<IPage> {
        switch (pageType) {
            case "REDIS_TS_PAGE":
                return Promise.resolve(new RedisTsPage(pageInfo, this.redisDriver, this.keyBuilder));
            default:
                throw new Error(`Unsupported page type: ${pageType}`);
        }
    }


}