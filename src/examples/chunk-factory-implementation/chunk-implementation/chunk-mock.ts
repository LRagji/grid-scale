import { InjectableConstructor } from "node-apparatus";
import { IChunk } from "../../../chunk/i-chunk.js";
import { ShardAccessMode } from "../../../types/shard-access-mode.js";

export class ChunkGenerator implements IChunk {

    constructor(
        private readonly connectionPath: string,
        private readonly mode: ShardAccessMode,
        private readonly mergeFunction: <T>(cursors: IterableIterator<T>[]) => IterableIterator<T>,
        private readonly callerSignature: string,
        private readonly injectableConstructor: InjectableConstructor = new InjectableConstructor()) {
    }

    bulkSet(records: Map<string, any[][]>): void {

    }

    *bulkIterator(tags: string[], startTimeInclusive: number, endTimeExclusive: number): IterableIterator<any[]> {
        for (let i = 0; i < (86400 * 5); i++) {
            yield [i, i, i, null, `Tag${i - (i % 86400)}-${this.connectionPath}`];
        }
    }

    canBeDisposed(): boolean {
        return true;
    }

    [Symbol.asyncDispose](): Promise<void> {
        return Promise.resolve();
    }

}

