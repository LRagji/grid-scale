import { kWayMerge } from "node-apparatus";

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
            //Promote first element as the yield element
            yieldIndex = elementIndex;
            continue;
        }

        // Compare TagName
        if (element[this.tagIndex] === elements[yieldIndex][this.tagIndex]) {
            //Compare sample time
            if (element[this.timeIndex] < elements[yieldIndex][this.timeIndex]) {
                //Promote element as the yield element when its time is less than the yield element(Ascending order by sample time.)
                yieldIndex = elementIndex;
            }
            else if (element[this.timeIndex] === elements[yieldIndex][this.timeIndex]) {

                if (element[this.insertTimeIndex] > elements[yieldIndex][this.insertTimeIndex]) {
                    //Purge yield element when insert time is less than the yield element.(This row needs to be vacuumed from MVCC standpoint.)
                    purgeIndexes.push(yieldIndex);
                    //Promote element as the yield element when its insert time is greater than the yield element.(Descending order by insert time.)
                    yieldIndex = elementIndex;
                }
                else {
                    //Purge element when insert time is less than the yield element.(This row needs to be vacuumed from MVCC standpoint.)
                    purgeIndexes.push(elementIndex);
                }

            }
        }
    };
    return { yieldIndex, purgeIndexes };
};


export function gridKWayMerge(tagIndex: number, timeIndex: number, insertTimeIndex: number) {
    const mergeFunction = frameMerge.bind({ tagIndex, timeIndex, insertTimeIndex });
    return <T>(iterators: IterableIterator<T>[]): IterableIterator<T> => {
        return kWayMerge<T>(iterators, mergeFunction);
    }
}