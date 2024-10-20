import { kWayMerge } from "./k-way-merge.js";

function frameMerge<T>(elements: T[]): { yieldIndex: number, purgeIndexes: number[] } {
    let purgeIndexes = [];
    let yieldIndex = -1;
    for (let elementIndex = 0; elementIndex < elements.length; elementIndex++) {
        const element = elements[elementIndex];
        if (element == null || Array.isArray(element) === false || (Array.isArray(element) && element.length === 0)) {
            purgeIndexes.push(elementIndex);
            continue;
        }

        if (yieldIndex === -1) {
            yieldIndex = elementIndex;
        }
        else {
            //TagName need to be compared
            if (element[this.tagIndex] === elements[yieldIndex][this.tagIndex] && element[this.timeIndex] < elements[yieldIndex][this.timeIndex]) {
                yieldIndex = elementIndex;
            }
            else if (element[this.tagIndex] === elements[yieldIndex][this.tagIndex] && element[this.timeIndex] === elements[yieldIndex][this.timeIndex]) {
                //Compare Insert time in descending order MVCC
                if (elements[1] > elements[yieldIndex][1]) {
                    purgeIndexes.push(yieldIndex);
                    yieldIndex = elementIndex;
                }
                else {
                    purgeIndexes.push(elementIndex);
                }
            }
        }
    };
    return { yieldIndex, purgeIndexes };
};


export function gridKWayMerge(tagIndex: number, timeIndex: number) {
    const mergeFunction = frameMerge.bind({ tagIndex, timeIndex });
    return <T>(iterators: IterableIterator<T>[]): IterableIterator<T> => {
        return kWayMerge<T>(iterators, mergeFunction);
    }
}