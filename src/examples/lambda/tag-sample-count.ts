export default function countPerTagFunction(first: boolean, last: boolean, page: any[][], acc: Map<string, number>) {
    const returnObject = {
        yield: false,
        yieldValue: null,
        accumulator: acc
    };

    if (first === true) {
        return returnObject;
    }

    if (last === true) {
        returnObject.yield = true;
        returnObject.yieldValue = Array.from(acc.entries());
        returnObject.accumulator.clear();
        return returnObject;
    }

    for (const row of page) {
        const tagId = row[4];
        const count = acc.get(tagId) ?? 0;
        acc.set(tagId, count + 1);
    }

    return returnObject;
};
