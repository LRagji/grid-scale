import { cpus } from "node:os";
import { deserialize, DisposeMethodPayload, IProxyMethod, serialize } from "./i-proxy-method.js";
import { Worker } from "node:worker_threads";

export class LongRunnerProxies {

    private readonly workersExistingWork = new Array<Promise<any>>();
    private readonly workers: Array<Worker> = new Array<Worker>()
    public readonly WorkerCount: number;

    constructor(workerCount: number, workerFilePath: string) {
        const parsedWorkerCount = Math.min(cpus().length, Math.max(0, workerCount));
        for (let index = 0; index < parsedWorkerCount; index++) {
            this.workers.push(new Worker(workerFilePath, { workerData: null }));
        }
        this.workersExistingWork = new Array<Promise<any>>(this.workers.length);
        this.WorkerCount = this.workers.length;
    }

    public async invokeRemoteMethod<T>(methodName: string, methodArguments: any[], workerIndex = 0, methodInvocationId = Number.NaN): Promise<T> {
        const workerIdx = Math.max(0, Math.min(workerIndex, this.workers.length - 1));
        if (this.workersExistingWork[workerIdx] !== undefined) {
            await this.workersExistingWork[workerIdx];
        }
        this.workersExistingWork[workerIdx] = new Promise<T>((resolve, reject) => {
            const worker = this.workers[workerIdx];
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

            const methodInvocationPayload: IProxyMethod = { workerId: workerIdx, invocationId: methodInvocationId, methodName, methodArguments, returnValue: null };
            worker.postMessage(serialize(methodInvocationPayload));

        });
        return this.workersExistingWork[workerIdx]
    }

    public async[Symbol.asyncDispose]() {
        await Promise.allSettled(this.workersExistingWork);
        for (const worker of this.workers) {
            worker.postMessage(serialize(DisposeMethodPayload));
        }
        this.workers.length = 0;
        this.workersExistingWork.length = 0;
    }
}