import { InjectableConstructor } from "node-apparatus";
import { IChunk } from "../../../types/i-chunk.js";
import { ShardAccessMode } from "../../../types/shard-access-mode.js";

export class ChunkGenerator implements IChunk {

    constructor(
        private readonly connectionPath: string,
        private readonly mode: ShardAccessMode,
        private readonly mergeFunction: <T>(cursors: IterableIterator<T>[]) => IterableIterator<T>,
        private readonly callerSignature: string,
        private readonly injectableConstructor: InjectableConstructor = new InjectableConstructor()) {
    }

    public async bulkSet(records: Map<string, any[][]>): Promise<void> {

    }

    public async  * bulkIterator(tags: string[], startTimeInclusive: number, endTimeExclusive: number): AsyncIterableIterator<any[]> {
        for (let i = 0; i < (86400 * 5); i++) {
            yield [i, i, i, null, `Tag${i - (i % 86400)}-${this.connectionPath}`];
        }
    }

    public canBeDisposed(): boolean {
        return true;
    }

    public [Symbol.asyncDispose](): Promise<void> {
        return Promise.resolve();
    }

}

