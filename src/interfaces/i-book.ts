import { IDimensionalElement } from "./i-dimensional-element";
import { IDimensionalQuery } from "./i-dimensional-query.js";
import { IPage } from "./i-page.js";
import { IPageInfo } from "./i-page-info.js";

export interface IBook {

    readonly totalPageCapacity: number;
    readonly pageSizeLimitInBytes: number;
    readonly pageActiveTimeLimitInMs: number;
    readonly pageFactory: (pageInfo: IPageInfo, pageType: string) => Promise<IPage>;
    readonly pagesReconcileCallback: (newPageInfo: IPageInfo | undefined, evictedPageInfo: IPageInfo[]) => Promise<void>;

    listPagesSorted(): Promise<IPageInfo[]>;

    fetchPageByKey(pageKey: IPageInfo): Promise<IPage | null>;

    removePage(pageKey: IPageInfo): Promise<void>;

    upsertElements(elements: IDimensionalElement[]): Promise<void>;

    queryElementsByDimensions(query: IDimensionalQuery, maxElementsCount: number): Promise<IDimensionalElement[]>;
}