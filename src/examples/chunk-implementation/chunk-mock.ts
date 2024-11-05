import { InjectableConstructor } from "node-apparatus";
import { ChunkBase } from "../../chunk/chunk-base.js";
import { ShardAccessMode } from "../../types/shard-access-mode.js";

export default class ChunkGenerator extends ChunkBase {

    public static override readonly columnCount: number = 5;
    public static override readonly timeColumnIndex: number = 0;
    public static override readonly insertTimeColumnIndex: number = 1;
    public static override readonly tagColumnIndex: number = 4;


    constructor(
        private readonly connectionPath: string,
        private readonly mode: ShardAccessMode,
        private readonly mergeFunction: <T>(cursors: IterableIterator<T>[]) => IterableIterator<T>,
        private readonly callerSignature: string,
        private readonly injectableConstructor: InjectableConstructor = new InjectableConstructor()) {
        super(connectionPath, mode, mergeFunction, callerSignature, injectableConstructor);
        //console.log(`ChunkGenerator.constructor(${connectionPath}, ${mode},  ${callerSignature})`);
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

