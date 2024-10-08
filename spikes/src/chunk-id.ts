import { createHash } from 'node:crypto';
import { TConfig } from './t-config.js';

// export interface IChunkId {
//     tagHash: Buffer;
//     tagPart: string;
//     timePart: number;
//     limitIndex: (limits: number) => number;
//     logicalChunkId: (prefix: string, sep: string) => string;
// }

function MD5(input: string): number[] {
    const hashAlgorithm = 'md5';
    const hashBuffer = createHash(hashAlgorithm)
        .update(input)
        .digest();
    return [hashBuffer.readInt32LE(0), hashBuffer.readInt32LE(4), hashBuffer.readInt32LE(8), hashBuffer.readInt32LE(12)];
}

function DJB2(input: string): number[] {
    let hash = 5381;
    for (let i = 0; i < input.length; i++) {
        hash = ((hash << 5) + hash) + input.charCodeAt(i); // hash * 33 + c
    }
    return [hash >>> 0]; // Ensure the hash is a positive integer
}

export const chunkAlgos = [MD5, DJB2];

export function generateChunkId(tagName: string, time: number, config: TConfig): ChunkId {
    const tagHash = chunkAlgos[config.activeCalculatorIndex](tagName);
    return new ChunkId(tagHash, [time], config);
}

export class ChunkId {
    public readonly logicalChunkId: string;

    public static from(tagName: string, time: number, config: TConfig): ChunkId {
        const tagHash = chunkAlgos[config.activeCalculatorIndex](tagName);//Just need to change string to number so only tagName is hashed with algo.
        return new ChunkId(tagHash, [time], config);
    }

    public tagNameMod(limit: number): number {
        return Math.abs(this.tagHash.reduce((acc, val) => acc + val, 0)) % limit;
    }

    public timeMod(limit: number): number {
        return Math.abs(this.timeHash.reduce((acc, val) => acc + val, 0)) % limit;
    }

    constructor(private readonly tagHash: number[], private readonly timeHash: number[], config: TConfig) {
        const tagNameMod = this.tagHash.map(val => val - (val % config.tagBucketWidth));
        const timeMod = this.timeHash.map(val => val - (val % config.timeBucketWidth));
        this.logicalChunkId = `${config.logicalChunkPrefix}${config.logicalChunkSeperator}${tagNameMod.join("")}${config.logicalChunkSeperator}${timeMod.join("")}`;
    }
}