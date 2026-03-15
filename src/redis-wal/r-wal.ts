import { ISortedElement } from "../interfaces/i-sorted-element";
import { Utilities } from "../utilities";
import { RBook } from "./r-book";
import { PageRankedElement, RPage } from "./r-page";

export class RWal {

    constructor(
        private readonly book: RBook,
        //Defaults.
        private readonly sizeEstimator: (samples: ISortedElement[]) => number = Utilities.roughSizeEstimator,
    ) { }

    public async append(mutableElements: ISortedElement[], insertTimestamp: number = Date.now()): Promise<void> {
        const numberOfElements = mutableElements.length;
        if (numberOfElements <= 0 || numberOfElements > Utilities.u48In3) {
            throw new Error("Number of elements must be between 1 and " + Utilities.u48In3 + ". Currently, it is set to " + numberOfElements.toString() + ".");
        }

        const totalElementSizeInBytes = this.sizeEstimator(mutableElements);
        if (totalElementSizeInBytes <= 0 || totalElementSizeInBytes > Utilities.u48In3) {
            throw new Error("Estimated size must be between 1 and " + Utilities.u48In3 + ". Currently, it is set to " + totalElementSizeInBytes.toString() + " bytes.");
        }

        const pagInfo = await this.book.fetchWriteablePage(insertTimestamp, totalElementSizeInBytes, numberOfElements);
        await pagInfo.page.dumpDataToPage(mutableElements, pagInfo.sequenceStartNumber);
    }

    public async queryByRank(groupKeys: string[], startInclusiveRank: number, endExclusiveRank: number, maxElementsPerGroup = 100): Promise<ISortedElement[]> {

        const deDuplicatedGroupKeys = this.validateQueryRangeParams(groupKeys, startInclusiveRank, endExclusiveRank, maxElementsPerGroup);
        const rankedPages = await this.book.fetchAvailablePagesWithRanks();
        const pageResults = await this.parallelQueryPages(rankedPages, deDuplicatedGroupKeys, startInclusiveRank, endExclusiveRank, maxElementsPerGroup);
        const result: ISortedElement[] = this.aggregateRankedElements(deDuplicatedGroupKeys, pageResults, maxElementsPerGroup);

        return result;
    }

    private async parallelQueryPages(rankedPages: Map<RPage, number>, deDuplicatedGroupKeys: string[], startInclusiveRank: number, endExclusiveRank: number, maxElementsPerGroup: number) {
        const pageQueriesHandles = new Array<Promise<Map<string, Map<number, PageRankedElement>>>>();
        for (const [page, pageRank] of rankedPages) {
            pageQueriesHandles.push(page.fetchElementsByRange(deDuplicatedGroupKeys, pageRank, startInclusiveRank, endExclusiveRank, maxElementsPerGroup));
        }
        const pageResults = await Promise.all(pageQueriesHandles);
        return pageResults;
    }

    private aggregateRankedElements(deDuplicatedGroupKeys: string[], pageResults: Map<string, Map<number, PageRankedElement>>[], maxElementsPerGroup: number) {
        const result: ISortedElement[] = [];
        for (const groupKey of deDuplicatedGroupKeys) {
            let resultsIterator = 0;
            const currentGroupKeyElements = new Array<ISortedElement>();
            do {
                for (const rank of pageResults[resultsIterator].get(groupKey)?.keys() ?? []) {
                    let existingElement: PageRankedElement | undefined = undefined;
                    for (const results of pageResults) {
                        const element = results.get(groupKey)?.get(rank);
                        if (element == undefined) {
                            continue;
                        }
                        if (existingElement === undefined) {
                            existingElement = element;
                            continue;
                        }
                        const pageRankUpdate = existingElement.pageRank < element.pageRank;
                        const serialNumberUpdate = existingElement.pageRank === element.pageRank && existingElement.sn < element.sn;
                        if (pageRankUpdate || serialNumberUpdate) {
                            existingElement = element;
                        }
                    }
                    if (existingElement !== undefined && currentGroupKeyElements.length < maxElementsPerGroup) {
                        delete existingElement.pageRank;
                        currentGroupKeyElements.push(existingElement);
                    }
                }
                resultsIterator++;
            } while (resultsIterator < pageResults.length && currentGroupKeyElements.length < maxElementsPerGroup);
            currentGroupKeyElements.sort((a, b) => a.elementRank - b.elementRank); //TODO: This sorting can be optimized by using a priority queue while iterating instead of sorting at the end.
            result.push(...currentGroupKeyElements);
        }
        return result;
    }

    private validateQueryRangeParams(groupKeys: string[], startInclusiveRank: number, endExclusiveRank: number, maxElementsPerGroup: number): string[] {

        if (groupKeys.length === 0) {
            throw new Error("At least one group key must be specified for querying.");
        }
        if (startInclusiveRank < 0 || endExclusiveRank < 0) {
            throw new Error("Start rank and end rank must be non-negative.");
        }
        if (endExclusiveRank < startInclusiveRank) {
            throw new Error("End rank must be greater than or equal to start rank.");
        }
        if ((endExclusiveRank - startInclusiveRank) === 0) {
            throw new Error(`The difference between end rank and start rank must be greater than 0. Currently, it is ${endExclusiveRank - startInclusiveRank}.`);
        }
        if (groupKeys.length > 10) {
            throw new Error("A maximum of 10 group keys can be specified for querying to prevent excessive load. Currently, " + groupKeys.length + " group keys were provided.");
        }
        if (maxElementsPerGroup <= 0 || maxElementsPerGroup > 10000) {
            throw new Error("Max elements must be between 1 and 10000. Currently, it is set to " + maxElementsPerGroup.toString() + ".");
        }

        return [...(new Set(groupKeys)).values()]
    }
}