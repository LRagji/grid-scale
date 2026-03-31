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

    //TODO: Remove this and replace with queryElementsByDimensions once the latter is implemented, but we keep it for now to ensure that functionality is not broken so a shortcut until full query parser is implemented.
    fetchElementsByRange(groupKeys: string[], startInclusiveRank: number, endExclusiveRank: number, maxElementsPerGroup: number): Promise<IDimensionalElement[]>;

}