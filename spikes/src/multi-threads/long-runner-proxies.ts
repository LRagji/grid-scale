import { cpus } from "node:os";
import { deserialize, DisposeMethodPayload, IProxyMethod, serialize } from "./i-proxy-method.js";
import { Worker } from "node:worker_threads";

export class LongRunnerProxies {

    private workersExistingWork = new Array<Promise<any>>();
    private readonly workers: Array<Worker> = new Array<Worker>()
    public WorkerCount: number;
    private selfWorker: any;

    constructor(private readonly workerCount: number, private readonly workerFilePath: string) { }

    public async initialize() {
        const parsedWorkerCount = Math.min(cpus().length, Math.max(0, this.workerCount));
        for (let index = 0; index < parsedWorkerCount; index++) {
            //TODO:Have Handshake with threads that they are ready to accept commands.
            this.workers.push(new Worker(this.workerFilePath, { workerData: null }));
        }
        if (this.workers.length === 0) {
            const module = await import(this.workerFilePath);
            this.selfWorker = module.default;
            this.WorkerCount = 1;
        }
        else {
            this.WorkerCount = this.workers.length;
        }
        this.workersExistingWork = new Array<Promise<any>>(this.workers.length);
    }

    public async invokeMethod<T>(methodName: string, methodArguments: any[], workerIndex = 0, methodInvocationId = Number.NaN): Promise<T> {
        const workerIdx = Math.max(0, Math.min(workerIndex, this.workers.length - 1));

        if (workerIdx === 0 && this.selfWorker !== undefined) {
            return this.selfWorker[methodName](...methodArguments);
        }
        else {
            return this.invokeRemoteMethod(methodName, methodArguments, workerIdx, methodInvocationId);
        }
    }

    private async invokeRemoteMethod<T>(methodName: string, methodArguments: any[], workerIndex = 0, methodInvocationId = Number.NaN): Promise<T> {

        if (this.workersExistingWork[workerIndex] !== undefined) {
            await this.workersExistingWork[workerIndex];
        }
        this.workersExistingWork[workerIndex] = new Promise<T>((resolve, reject) => {
            const worker = this.workers[workerIndex];
            const workerErrorHandler = (error: Error) => {
                worker.off('message', workerMessageHandler);
                reject(error);
            };

            const workerMessageHandler = (message: any) => {
                worker.off('error', workerErrorHandler);
                const returnValue = deserialize(message);
                if (Number.isNaN(returnValue.workerId) === false) {
                    const workerIdx = Math.max(0, Math.min(returnValue.workerId, this.workers.length - 1));
                    this.workersExistingWork[workerIdx] = undefined;
                }
                if (returnValue.error !== undefined) {
                    reject(new Error(returnValue.error));
                    return
                }
                resolve(returnValue.returnValue);
            };

            worker.once('error', workerErrorHandler);
            worker.once('message', workerMessageHandler);

            const methodInvocationPayload: IProxyMethod = { workerId: workerIndex, invocationId: methodInvocationId, methodName, methodArguments, returnValue: null };
            worker.postMessage(serialize(methodInvocationPayload));

        });
        return this.workersExistingWork[workerIndex]
    }

    public async[Symbol.asyncDispose]() {
        if (this.selfWorker !== undefined) {
            await this.selfWorker[Symbol.asyncDispose]();
        }
        await Promise.allSettled(this.workersExistingWork);
        for (const worker of this.workers) {
            worker.postMessage(serialize(DisposeMethodPayload));
        }
        this.workers.length = 0;
        this.workersExistingWork.length = 0;
    }
}