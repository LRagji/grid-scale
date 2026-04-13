import { Condition, ConditionGroup, IDimensionalQuery } from "../interfaces/i-dimensional-query.js";
import { IDimensionalElement } from "../interfaces/i-dimensional-element.js";
import { IMetadata, IQContainer } from "../interfaces/i-q-acc.js";

interface IBufferedRecord {
    offset: number;
    length: number;
}

/**
 * Fixed-size in-memory container for time-series shaped dimensional elements.
 * Records are serialized into a circular buffer and oldest records are evicted
 * when capacity is exhausted.
 */
export class InMemoryTimeseriesQContainer implements IQContainer {

    public readonly id: string;

    private static readonly recordLengthHeaderBytes = 4;
    private static readonly bufferSizeMetadataKey = "bufferSizeInBytes";

    private initialized = false;
    private capacityInBytes = 0;
    private storageBuffer = Buffer.alloc(0);
    private usedBytes = 0;
    private writeOffset = 0;
    private records: IBufferedRecord[] = [];

    constructor(id: string) {
        if (!id || id.trim().length === 0) {
            throw new Error("Container id cannot be empty.");
        }
        this.id = id;
    }

    public async initialize(containerMetadata: IMetadata): Promise<void> {
        const configuredSize = containerMetadata[InMemoryTimeseriesQContainer.bufferSizeMetadataKey];
        const capacity = Number(configuredSize);
        if (!Number.isFinite(capacity) || Number.isNaN(capacity) || capacity <= InMemoryTimeseriesQContainer.recordLengthHeaderBytes) {
            throw new Error("containerMetadata.bufferSizeInBytes must be a finite number greater than 4 bytes.");
        }

        this.capacityInBytes = Math.floor(capacity);
        this.storageBuffer = Buffer.alloc(this.capacityInBytes);
        this.usedBytes = 0;
        this.writeOffset = 0;
        this.records = [];
        this.initialized = true;
    }

    public async accumulate(elements: IDimensionalElement[]): Promise<void> {
        this.ensureInitialized();
        if (!Array.isArray(elements) || elements.length === 0) {
            return;
        }

        for (const element of elements) {
            const payload = Buffer.from(JSON.stringify(element), "utf8");
            const totalRecordLength = InMemoryTimeseriesQContainer.recordLengthHeaderBytes + payload.length;

            if (totalRecordLength > this.capacityInBytes) {
                throw new Error(`Element is too large for configured buffer. Needed ${totalRecordLength} bytes, capacity is ${this.capacityInBytes} bytes.`);
            }

            while (this.usedBytes + totalRecordLength > this.capacityInBytes) {
                this.evictOldestRecord();
            }

            const recordOffset = this.writeOffset;
            const lengthHeader = Buffer.allocUnsafe(InMemoryTimeseriesQContainer.recordLengthHeaderBytes);
            lengthHeader.writeUInt32BE(payload.length, 0);

            this.writeBytesCircular(this.writeOffset, lengthHeader);
            this.writeOffset = (this.writeOffset + InMemoryTimeseriesQContainer.recordLengthHeaderBytes) % this.capacityInBytes;
            this.writeBytesCircular(this.writeOffset, payload);
            this.writeOffset = (this.writeOffset + payload.length) % this.capacityInBytes;

            this.usedBytes += totalRecordLength;
            this.records.push({
                offset: recordOffset,
                length: totalRecordLength
            });
        }
    }

    public async query(query: IDimensionalQuery, maxElementsCount: number): Promise<IDimensionalElement[]> {
        this.ensureInitialized();
        if (maxElementsCount <= 0) {
            throw new Error("maxElementsCount must be greater than zero.");
        }

        const hashed = new Map<string, IDimensionalElement>();
        const anonymous: IDimensionalElement[] = [];

        for (const record of this.records) {
            const element = this.deserializeRecord(record);
            if (!this.matchesQuery(element, query.query)) {
                continue;
            }

            if (element.globalIdentityHash == null) {
                anonymous.push(element);
            } else {
                hashed.set(element.globalIdentityHash, element);
            }
        }

        const merged = [...hashed.values(), ...anonymous];
        return merged.slice(0, maxElementsCount);
    }

    private ensureInitialized(): void {
        if (this.initialized === false) {
            throw new Error("Container must be initialized before use.");
        }
    }

    private evictOldestRecord(): void {
        const oldest = this.records.shift();
        if (oldest === undefined) {
            throw new Error("Cannot evict from an empty record buffer.");
        }
        this.usedBytes -= oldest.length;
    }

    private writeBytesCircular(offset: number, payload: Buffer): void {
        if (payload.length === 0) {
            return;
        }
        const bytesUntilEnd = this.capacityInBytes - offset;
        if (payload.length <= bytesUntilEnd) {
            payload.copy(this.storageBuffer, offset, 0, payload.length);
            return;
        }

        payload.copy(this.storageBuffer, offset, 0, bytesUntilEnd);
        payload.copy(this.storageBuffer, 0, bytesUntilEnd, payload.length);
    }

    private readBytesCircular(offset: number, length: number): Buffer {
        if (length <= 0) {
            return Buffer.alloc(0);
        }
        const bytesUntilEnd = this.capacityInBytes - offset;
        if (length <= bytesUntilEnd) {
            return this.storageBuffer.subarray(offset, offset + length);
        }

        const first = this.storageBuffer.subarray(offset, offset + bytesUntilEnd);
        const secondLength = length - bytesUntilEnd;
        const second = this.storageBuffer.subarray(0, secondLength);
        return Buffer.concat([first, second], length);
    }

    private deserializeRecord(record: IBufferedRecord): IDimensionalElement {
        const headerBytes = this.readBytesCircular(record.offset, InMemoryTimeseriesQContainer.recordLengthHeaderBytes);
        const payloadLength = headerBytes.readUInt32BE(0);
        const payloadOffset = (record.offset + InMemoryTimeseriesQContainer.recordLengthHeaderBytes) % this.capacityInBytes;
        const payload = this.readBytesCircular(payloadOffset, payloadLength);
        return JSON.parse(payload.toString("utf8")) as IDimensionalElement;
    }

    private matchesQuery(element: IDimensionalElement, group: ConditionGroup): boolean {
        if (group.operator === "AND") {
            for (const condition of group.conditions) {
                const isMatch = "conditions" in condition
                    ? this.matchesQuery(element, condition)
                    : this.matchesCondition(element, condition);
                if (!isMatch) {
                    return false;
                }
            }
            return true;
        }

        for (const condition of group.conditions) {
            const isMatch = "conditions" in condition
                ? this.matchesQuery(element, condition)
                : this.matchesCondition(element, condition);
            if (isMatch) {
                return true;
            }
        }
        return false;
    }

    private matchesCondition(element: IDimensionalElement, condition: Condition): boolean {
        const currentValue = element.dim?.[condition.dimension];

        if (typeof currentValue === "string") {
            if (condition.operator === "eq") {
                return typeof condition.value === "string" && currentValue === condition.value;
            }
            if (condition.operator === "noteq") {
                return typeof condition.value === "string" && currentValue !== condition.value;
            }
            if (condition.operator === "in") {
                return Array.isArray(condition.value) && (condition.value as string[]).includes(currentValue);
            }
            if (condition.operator === "notin") {
                return Array.isArray(condition.value) && !(condition.value as string[]).includes(currentValue);
            }
            return false;
        }

        if (typeof currentValue === "number") {
            if (condition.operator === "eq") {
                return typeof condition.value === "number" && currentValue === condition.value;
            }
            if (condition.operator === "noteq") {
                return typeof condition.value === "number" && currentValue !== condition.value;
            }
            if (condition.operator === "lt") {
                return typeof condition.value === "number" && currentValue < condition.value;
            }
            if (condition.operator === "gt") {
                return typeof condition.value === "number" && currentValue > condition.value;
            }
            if (condition.operator === "between") {
                return Array.isArray(condition.value)
                    && condition.value.length === 2
                    && typeof condition.value[0] === "number"
                    && typeof condition.value[1] === "number"
                    && currentValue >= condition.value[0]
                    && currentValue <= condition.value[1];
            }
            return false;
        }

        return false;
    }
}