import { IDimensionalElement } from "./i-dimensional-element.js";
import { IDimensionalQuery } from "./i-dimensional-query.js";
import { IPageInfo } from "./i-page-info.js";


export interface IPage {

    readonly info: IPageInfo;
    readonly pageType: string;

    upsertElements(elements: IDimensionalElement[], sequenceStart: number): Promise<void>;

    queryElementsByDimensions(query: IDimensionalQuery, maxElementsCount: number): Promise<IDimensionalElement[]>;

    dumpPage(): Promise<IDimensionalElement[]>;

    purgePage(expireAfterInMilliseconds: number): Promise<void>
}