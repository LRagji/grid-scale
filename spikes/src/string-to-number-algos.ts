import { createHash } from 'node:crypto';

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

function StringSplit(input: string): number[] {
    if (input.substring(0, 3) === 'Tag') {
        return [parseInt(input.substring(3), 10)]; //Tag1 Tag[x]
    }
    else {
        return DJB2(input);
    }
}

export const StringToNumberAlgos = [MD5, DJB2, StringSplit];