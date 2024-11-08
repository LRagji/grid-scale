//This file should only be used for testing.

function generateRandomString(length: number, index: number): string {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_+[]{}|;:,.<>?';
    let result = '';
    const charactersLength = characters.length;
    for (let i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
    return result;
}

function generateIndexedString(length: number, index: number) {
    return `Tag${index}`;
}

export function generateTagNames(totalTags: number, increment: number = 1): Array<bigint> {
    const tagIds = new Array<bigint>();
    for (let tagIndex = 0; (tagIndex / increment) < totalTags; tagIndex += increment) {
        //const tagName = generateRandomString(32, tagIndex);
        tagIds.push(BigInt(tagIndex));
    }
    return tagIds;
}

export function generateRandomSamples(totalTags: number, totalSamplesPerTag: number, columns: ((time: number, tagId: bigint) => number | string | null)[]): Map<bigint, number[]> {
    const generatedData = new Map<bigint, any[]>();
    const samples = new Array<number | string | null>();
    const tagIds = generateTagNames(totalTags);
    tagIds.forEach(tagId => {
        if (samples.length === 0) {
            for (let time = 0; time < totalSamplesPerTag; time++) {
                samples.push(time * 1000);
                columns.forEach(column => samples.push(column(time, tagId)));
            }
        }
        generatedData.set(tagId, samples);
    });
    return generatedData;
}

export function formatKB(B: number): number {
    return (B / 1024);
}
export function formatMB(KB: number): number {
    return (KB / 1024);
}
export function formatGB(MB: number): number {
    return (MB / 1024);
}

export function trackMemory() {
    const memoryUsage = process.memoryUsage();
    this.heapPeakMemory = Math.max(this.heapPeakMemory, memoryUsage.heapUsed);
    this.rssPeakMemory = Math.max(this.rssPeakMemory, memoryUsage.rss);
}
