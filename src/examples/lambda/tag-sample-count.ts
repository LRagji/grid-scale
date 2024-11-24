export default function tagSampleCount(page: any[][]) {
    const acc: Map<string, number> = new Map<string, number>()

    for (const row of page) {
        const tagId = row[4];
        const count = acc.get(tagId) ?? 0;
        acc.set(tagId, count + 1);
    }

    return Array.from(acc.entries());
}