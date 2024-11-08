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

function encodeUnicodeToUint32Array(input: string): number[] {
    //Note: Do not change this to classic for loop, as it will break for code points > 0xFFFF
    //https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/codePointAt
    return Array.from(input)
        .map(char => char.codePointAt(0))
        .filter(codePoint => codePoint !== undefined) as number[];
}

function encodeStringToBigint(input: string): bigint {
    //The maximum possible Bigint for v8 implementation is 1 million bits or 125,000 bytes which should easy cover 256 characters of input string, even if each character is 4 bytes(UTF32) ie:1024bytes.
    //https://v8.dev/blog/bigint
    const limit = 2000;
    if (input.length > limit) {
        throw new Error(`Input string is too long(${input.length}) to be converted to a valid numeric sequence, length should be less than ${limit}.`);
    }
    let result = BigInt(0);
    //Note: Do not change this to classic for loop, as it will break for code points > 0xFFFF
    //https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/codePointAt
    const maximumCodePoint = BigInt(0x110000);//Highest possible Unicode code: 1,114,112 in Dec or 0x10FFFF in Hex
    for (const char of input) {
        const codePoint = char.codePointAt(0);
        result = result * maximumCodePoint + BigInt(codePoint);
    }
    return result;
}

export const StringToNumberAlgos = [MD5, DJB2, StringSplit, encodeUnicodeToUint32Array];
export const StringToBigIntAlgos = [encodeStringToBigint];