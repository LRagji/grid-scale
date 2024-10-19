export function* kWayMerge<T>(iterators: IterableIterator<T>[], frameProcessFunction: (elements: (T | null)[]) => { yieldIndex: number | null, purgeIndexes: number[] }): IterableIterator<T> {
    const compareFrame = iterators.map(_ => _.next().value);
    let doneCounter = compareFrame.length;
    const endReachedIndexes = new Set<number>();
    while (doneCounter > 0) {
        const result = frameProcessFunction(compareFrame);
        const yieldIndex = result.yieldIndex;
        const purgeIndexes = result.purgeIndexes || [];
        let exitLoop = false;

        if (yieldIndex !== null && !Number.isNaN(yieldIndex) && yieldIndex >= 0 && yieldIndex < compareFrame.length && !endReachedIndexes.has(yieldIndex)) {
            yield compareFrame[yieldIndex];
            const nextCursor = iterators[yieldIndex].next();
            compareFrame[yieldIndex] = nextCursor.value;
            if (nextCursor.done === true) {
                doneCounter--;
                endReachedIndexes.add(yieldIndex);
            }
        }
        else {
            exitLoop = true;
        }

        if (purgeIndexes.length > 0) {
            for (const purgeIndex of purgeIndexes) {
                if (purgeIndex !== -1 && !Number.isNaN(purgeIndex) && purgeIndex >= 0 && purgeIndex < compareFrame.length && !endReachedIndexes.has(purgeIndex)) {
                    const nextCursor = iterators[purgeIndex].next();
                    compareFrame[purgeIndex] = nextCursor.value;
                    if (nextCursor.done === true) {
                        doneCounter--;
                        endReachedIndexes.add(purgeIndex);
                    }
                }
            }
        }
        else if (exitLoop === true) {
            //This means the iteration was in-decisive, so we exit the loop, to prevent infinite loop.
            throw new Error("Frame processing was in-decisive, either it is pointing to empty iterator or incorrect value.");
            break;
        }
    }
}