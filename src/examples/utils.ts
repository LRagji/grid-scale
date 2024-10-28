//This file should only be used for testing.

import { TConfig } from "./t-config";

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

export function CommonConfig(): TConfig {
    return {
        setPaths: new Map<string, string[]>([["../data/high-speed-1", ["disk1", "disk2", "disk3", "disk4", "disk5"]]]),
        activePath: "../data/high-speed-1",
        tagBucketWidth: 500,
        timeBucketWidth: 86400000,
        fileNamePre: "ts",
        fileNamePost: ".db",
        timeBucketTolerance: 1,
        activeCalculatorIndex: 2,
        maxDBOpen: 100,
        logicalChunkPrefix: "D",
        logicalChunkSeperator: "|",
        redisConnection: 'redis://localhost:6379'
    } as TConfig;
}