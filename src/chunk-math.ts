export function logicalChunkId(dimensions: bigint[], logicalChunkPrefix: string, logicalChunkSeparator: string): string {
    return `${logicalChunkPrefix}${logicalChunkSeparator}${dimensions.join(logicalChunkSeparator)}`;
}

export function bucketInt(input: number, width: number): number {
    return input - (input % width);
}

export function bucket(input: bigint, width: bigint): bigint {
    return input - (input % width);
}

export function bigIntMin(a: bigint, b: bigint): bigint {
    return a < b ? a : b;
}

export function bigIntMax(a: bigint, b: bigint): bigint {
    return a > b ? a : b;
}

export function DJB2StringToNumber(input: string): number {
    let hash = 5381;
    for (let i = 0; i < input.length; i++) {
        hash = ((hash << 5) + hash) + input.charCodeAt(i); // hash * 33 + c
    }
    return hash >>> 0; // Ensure the hash is a positive integer
}