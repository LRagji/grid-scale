export function logicalChunkId(dimensions: bigint[], logicalChunkPrefix: string, logicalChunkSeparator: string): string {
    return `${logicalChunkPrefix}${logicalChunkSeparator}${dimensions.join(logicalChunkSeparator)}`;
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