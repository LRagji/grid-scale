import { parentPort, MessagePort, threadId, workerData, isMainThread } from "node:worker_threads";
import { BackgroundPlugin } from "../background-plugin.js";
import { ISyncPlugin } from "./i-sync-plugin.js";
import { deserialize, HeaderPayload, IThreadCommunication, serialize } from "./i-thread-communication.js";

export class LongRunningThread {

    private readonly plugins: Map<string, ISyncPlugin<any>> = new Map<string, ISyncPlugin<any>>();

    constructor(private readonly messagePort: MessagePort, private readonly identity: string, plugins: ISyncPlugin<any>[], pluginInitializeArgs: any[]) {
        let index = 0;
        plugins.forEach((plugin) => {
            plugin.initialize(identity, pluginInitializeArgs[index]);
            this.plugins.set(plugin.name, plugin);
            index++;
        });
    }

    public start() {
        this.messagePort.on('message', this.work.bind(this));
        this.messagePort.on('error', this.stop.bind(this));
    }

    public stop() {
        this.messagePort.removeAllListeners();
        this.plugins.forEach((runner) => runner[Symbol.dispose]());
        this.plugins.clear();
        this.messagePort.close();
    }

    public work(workMessage: Buffer) {
        try {
            const parsedPayload = deserialize(workMessage) as IThreadCommunication<any>;
            if (parsedPayload.header === HeaderPayload.Shutdown) {
                this.stop();
                return;
            }

            const plugin = this.plugins.get(parsedPayload.header);
            if (plugin != undefined) {
                const response = plugin.work(parsedPayload);
                this.messagePort.postMessage(serialize(response));
            }
            else {
                throw new Error(`Unknown header: ${parsedPayload.header}`);
            }

        } catch (error) {
            const payload: IThreadCommunication<String> = {
                header: HeaderPayload.Error,
                subHeader: `Error in worker[${this.identity}]`,
                payload: error.message
            };
            this.messagePort.postMessage(serialize(payload));
        }
    }

    public [Symbol.dispose]() {
        this.stop();
    }
}

if (isMainThread === false) {
    const longRunning = new LongRunningThread(parentPort, `${process.pid}-${threadId}`, [new BackgroundPlugin()], workerData as any[]);
    longRunning.start();
}