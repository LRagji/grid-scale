import { parentPort, MessagePort, isMainThread } from "node:worker_threads";
import { deserialize, DisposeMethodPayload, IProxyMethod, serialize } from "./i-proxy-method.js";

export class LongRunner {

    private readonly methodNameSet = new Set<string>();

    public constructor(
        shouldActivateMessagePort: boolean = !isMainThread,
        private readonly messagePort: MessagePort = parentPort) {

        // Get method names
        const methodNames = Object.getOwnPropertyNames(Object.getPrototypeOf(this))
            .filter(prop => typeof (this as any)[prop] === 'function' && prop !== 'constructor');
        this.methodNameSet = new Set<string>(methodNames);

        if (shouldActivateMessagePort === true) {
            this.messagePort.on('message', this.receiveCommands.bind(this));
            this.messagePort.on('error', this[Symbol.asyncDispose].bind(this));
        }
    }

    public receiveCommands(workMessage: Buffer) {
        let methodInvocation: IProxyMethod;
        try {
            methodInvocation = deserialize(workMessage);
            if (methodInvocation.methodName === DisposeMethodPayload.methodName) {
                this[Symbol.asyncDispose]();
                return;
            }

            if (this.methodNameSet.has(methodInvocation.methodName) === false) {
                throw new Error(`Unknown method: ${methodInvocation.methodName}`);
            }
            methodInvocation.returnValue = this[methodInvocation.methodName](...methodInvocation.methodArguments);
            this.messagePort.postMessage(serialize(methodInvocation));

        } catch (error) {
            const errorPayload: IProxyMethod = methodInvocation || { workerId: Number.NaN, invocationId: Number.NaN, methodName: "", methodArguments: [], returnValue: null };
            errorPayload.error = error.message
            this.messagePort.postMessage(serialize(errorPayload));
        }
    }

    public async [Symbol.asyncDispose]() {
        this.messagePort.removeAllListeners();
        this.messagePort.close();
    }
}