import { IThreadCommunication } from "./i-thread-communication.js";

export interface ISyncRunner<InitializeArgs> {
    name: string
    initialize(identity: string, args: InitializeArgs): void;
    work(input: IThreadCommunication<unknown>): IThreadCommunication<unknown>;
    [Symbol.dispose](): void;
}