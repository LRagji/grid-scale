import crypto from 'node:crypto';

export function bucketString(inputString: string, bucketWidth: number, sep = "|"): string {
    // Generate MD5 hash 16Bytes long
    const hashBuffer = crypto.createHash('md5')
        .update(inputString)
        .digest()

    // Bucket it
    const bucket1 = hashBuffer.readInt32LE(0) - (hashBuffer.readInt32LE(0) % bucketWidth);
    const bucket2 = hashBuffer.readInt32LE(4) - (hashBuffer.readInt32LE(4) % bucketWidth);
    const bucket3 = hashBuffer.readInt32LE(8) - (hashBuffer.readInt32LE(8) % bucketWidth);
    const bucket4 = hashBuffer.readInt32LE(12) - (hashBuffer.readInt32LE(12) % bucketWidth);

    return `${bucket1}${sep}${bucket2}${sep}${bucket3}${sep}${bucket4}`;
}

export function bucketTime(time: number, bucketWidth: number): string {
    return `${time - (time % bucketWidth)}`;
}

export function generateOrFetchIdentity(processMemory: Map<string, any>, variableName: string = "identity"): string {
    if (processMemory.has(variableName)) {
        return processMemory.get(variableName);
    }
    else {
        const identity = crypto.randomUUID();
        processMemory.set(variableName, identity);
        return identity;
    }
}

export function generateFileName(dateTime: number, tagName: string, tagBucketingWidth: number, timeBucketingWidth: number, processMemory: Map<string, any>, prefix = "D", sep = "|"): string {
    return `${prefix}${sep}${bucketString(tagName, tagBucketingWidth)}${sep}${bucketTime(dateTime, timeBucketingWidth)}${sep}${generateOrFetchIdentity(processMemory)}`;
}