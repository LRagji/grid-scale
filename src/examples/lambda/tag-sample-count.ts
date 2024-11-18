export default function tagSampleCount(page: any[][], acc: Map<string, number> = new Map<string, number>()) {
    const result = new Array<any[]>();
    for (const row of page) {
        const tagId = row[4];
        const count = acc.get(tagId) ?? 0;
        acc.set(tagId, count + 1);
    }

    for (const [tagId, count] of acc.entries()) {
        if (count === 86400) {
            acc.delete(tagId);
        }
        else {
            result.push([tagId, count]);
        }
    }

    return result;
}