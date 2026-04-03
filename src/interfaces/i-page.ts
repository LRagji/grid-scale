import { IDimensionalElement } from "./i-dimensional-element";
import { IDimensionalQuery } from "./i-dimensional-query";
import { IPageInfo } from "./i-page-info";


export interface IPage {

    readonly info: IPageInfo;
    readonly pageType: string;

    upsertElements(elements: IDimensionalElement[], sequenceStart: number): Promise<void>;

    queryElementsByDimensions(query: IDimensionalQuery, maxElementsCount: number): Promise<IDimensionalElement[]>;

    dumpPage(): Promise<IDimensionalElement[]>;

    purgePage(expireAfterInMilliseconds: number): Promise<void>
}