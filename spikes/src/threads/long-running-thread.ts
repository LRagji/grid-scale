import { parentPort, MessagePort, threadId, workerData, isMainThread } from "node:worker_threads";
import { BackgroundThread } from "../background-thread.js";
import { ISyncRunner } from "./i-sync-runner.js";
import { deserialize, IThreadCommunication, serialize } from "./i-thread-communication.js";

export class LongRunningThread {
    private readonly runners: Map<string, ISyncRunner<any>> = new Map<string, ISyncRunner<any>>();

    constructor(private readonly messagePort: MessagePort, private readonly identity: string, syncWorkItems: { runner: ISyncRunner<any>, args: any }[]) {
        syncWorkItems.forEach((item) => {
            item.runner.initialize(identity, item.args);
            this.runners.set(item.runner.name, item.runner);
        });
    }

    public async start(): Promise<void> {
        this.messagePort.on('message', this.work.bind(this));
        this.messagePort.on('error', (error) => {
            console.error(`Error in worker: ${error.message}`);
            this.stop();
        });
    }

    public async stop(): Promise<void> {
        this.messagePort.removeAllListeners();
        this.runners.forEach((runner) => runner[Symbol.dispose]());
        this.runners.clear();
        this.messagePort.close();
    }

    public async work(message: Buffer): Promise<void> {
        try {
            const parsedPayload = deserialize(message);
            switch (parsedPayload.header) {
                case "Shutdown":
                    await this.stop();
                    break;
                default:
                    const runner = this.runners.get(parsedPayload.header);
                    if (runner != undefined) {
                        const response = runner.work(parsedPayload);
                        this.messagePort.postMessage(serialize(response));
                    }
                    else {
                        throw new Error(`Unknown header: ${parsedPayload.header}`);
                    }
                    break;
            }

        } catch (error) {
            console.error(`Error in worker[${this.identity}]: ${error.message}`);
            const payload: IThreadCommunication<String> = {
                header: "Error",
                subHeader: "Error",
                payload: error.message
            };
            this.messagePort.postMessage(serialize(payload));
        }
    }

    public async [Symbol.asyncDispose]() {
        await this.stop();
    }

    public [Symbol.dispose]() {
        this.stop();
    }
}

if (!isMainThread) {
    const longRunning = new LongRunningThread(parentPort, `${process.pid}-${threadId}`, [{ runner: new BackgroundThread(), args: { config: workerData, cacheLimit: 100, bulkDropLimit: 10 } }]);
    longRunning.start();
}