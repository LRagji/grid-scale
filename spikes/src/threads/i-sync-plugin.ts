import { IThreadCommunication } from "./i-thread-communication.js";

export interface ISyncPlugin<InitializeArgs> {
    name: string
    initialize(identity: string, args: InitializeArgs): void;
    work(input: IThreadCommunication<unknown>): IThreadCommunication<unknown>;
    [Symbol.dispose](): void;
}