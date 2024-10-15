import { cpus } from "node:os";
import { Worker } from "node:worker_threads";
import { BootstrapConstructor } from "express-service-bootstrap";
import { ISyncPlugin } from "./i-sync-plugin.js";
import { deserialize, HeaderPayload, IThreadCommunication, serialize } from "./i-thread-communication.js";

export class WorkerManager<pluginType extends ISyncPlugin<any>> {

    private readonly externalWorkers = new Array<Worker>();
    private readonly selfWorker: pluginType;

    constructor(public readonly workerCount: number, workerFilePath: string, initializeArgs: any, pluginClass: new () => pluginType, identity = `${process.pid}-${Date.now()}`, bootstrap: BootstrapConstructor = new BootstrapConstructor) {
        this.workerCount = Math.min(cpus().length, Math.max(0, workerCount));
        for (let index = 0; index < this.workerCount; index++) {
            const newWorker = bootstrap.createInstance<Worker>(Worker, [workerFilePath, { workerData: initializeArgs }]);
            this.externalWorkers.push(newWorker);
        }
        if (this.externalWorkers.length === 0) {
            this.selfWorker = bootstrap.createInstance<pluginType>(pluginClass);
            this.selfWorker.initialize(identity, initializeArgs);
        }
    }

    public async execute<inputType, outputType>(inputs: IThreadCommunication<inputType>[]): Promise<IThreadCommunication<outputType>[]> {
        if (this.externalWorkers.length === 0) {
            const results = new Array<IThreadCommunication<outputType>>();
            for (const input of inputs) {
                if (input == undefined) {
                    continue;
                }
                results.push(this.selfWorker.work(input) as IThreadCommunication<outputType>);
            }
            return results;
        }
        else {
            const workerHandles = new Array<Promise<IThreadCommunication<outputType>>>();
            for (let workerIndex = 0; workerIndex < this.externalWorkers.length; workerIndex++) {
                if (inputs[workerIndex] == undefined) {
                    continue;
                }
                workerHandles.push(new Promise<IThreadCommunication<outputType>>((resolve, reject) => {
                    const worker = this.externalWorkers[workerIndex];

                    const workerErrorHandler = (error: Error) => {
                        worker.off('message', workerMessageHandler);
                        reject(error);
                    };

                    const workerMessageHandler = (message: any) => {
                        worker.off('error', workerErrorHandler);
                        const comMessage = deserialize<outputType>(message);
                        if (comMessage.header === HeaderPayload.Error) {
                            reject(new Error(comMessage.subHeader + comMessage.payload));
                        }
                        if (comMessage.header === HeaderPayload.Response) {
                            resolve(comMessage);
                        }
                    };

                    worker.once('error', workerErrorHandler);
                    worker.once('message', workerMessageHandler);
                    worker.postMessage(serialize(inputs[workerIndex]));
                }));
            }
            return Promise.all(workerHandles);
        }
    }

    public async[Symbol.asyncDispose]() {
        if (this.externalWorkers.length === 0) {
            this.selfWorker[Symbol.dispose]();
        }
        else {
            const payload = {
                header: HeaderPayload.Shutdown,
                subHeader: "Shutdown called from Dispose",
                payload: ''
            } as IThreadCommunication<string>;
            const closeQueryPayloads = new Array<IThreadCommunication<string>>(this.externalWorkers.length).fill(payload);
            await this.execute<string, string>(closeQueryPayloads);
        }
    }
}