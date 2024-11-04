//This file should only be used for testing.

function generateRandomString(length: number): string {
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

export function generateTagNames(totalTags: number, increment: number = 1): Array<string> {
    const tagNames = new Array<string>();
    for (let tagIndex = 0; (tagIndex / increment) < totalTags; tagIndex += increment) {
        const tagName = generateIndexedString(255, tagIndex);
        tagNames.push(tagName);
    }
    return tagNames;
}

export function generateRandomSamples(totalTags: number, totalSamplesPerTag: number, columns: ((time: number, tag: string) => number | string | null)[]): Map<string, number[]> {
    const generatedData = new Map<string, any[]>();
    const samples = new Array<number | string | null>();
    const tagNames = generateTagNames(totalTags);
    tagNames.forEach(tagName => {
        if (samples.length === 0) {
            for (let time = 0; time < totalSamplesPerTag; time++) {
                samples.push(time * 1000);
                columns.forEach(column => samples.push(column(time, tagName)));
            }
        }
        generatedData.set(tagName, samples);
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
