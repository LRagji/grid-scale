import { IDimensionalElement } from "./i-dimensional-element";
import { IDimensionalQuery } from "./i-dimensional-query";
import { IPage } from "./i-page";
import { IPageInfo } from "./i-page-info";

export interface IBook<PT extends IPage> {

    readonly totalPageCapacity: number;
    readonly pageSizeLimitInBytes: number;
    readonly pageActiveTimeLimitInMs: number;
    readonly pageFactory: (pageInfo: IPageInfo, pageType: string) => Promise<PT>;
    readonly pagesReconcileCallback: (newPageInfo: IPageInfo | undefined, evictedPageInfo: IPageInfo[]) => Promise<void>;

    //navigateToWritablePage(contentSizeInBytes: number, contentCount: number): Promise<PT>;

    listPages(): Promise<IPageInfo[]>;

    fetchPageByKey(pageKey: IPageInfo): Promise<PT | null>;

    removePage(pageKey: IPageInfo): Promise<void>;

    upsertElements(elements: IDimensionalElement[]): Promise<void>;

    queryElementsByDimensions(query: IDimensionalQuery, maxElementsCount: number): Promise<IDimensionalElement[]>;

    //TODO:This should be deleted and replaced by queryElementsByDimensions once the latter is implemented, but we keep it for now to ensure that functionality is not broken so a shortcut until full query parser is implemented.
    queryByRank(groupKeys: string[], startInclusiveRank: number, endExclusiveRank: number, maxElementsPerGroup: number): Promise<IDimensionalElement[]>

}