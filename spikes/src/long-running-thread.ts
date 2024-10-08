import { parentPort, MessagePort, threadId } from "node:worker_threads";

export interface IThreadCommunication {
    payload: string;
    header: "Error" | "Response" | "Execute" | "Shutdown";
}

export function deserialize(payload: string): IThreadCommunication {
    return JSON.parse(payload) as IThreadCommunication;
}

export function serialize(input: IThreadCommunication): string {
    return JSON.stringify(input);
}

export class LongRunningThread {
    private isRunning = false;

    constructor(private readonly messagePort: MessagePort, private readonly identity: string, private readonly executionCallback: <I extends IThreadCommunication>(input: I) => Promise<string>) { }

    public async start(): Promise<void> {
        this.isRunning = true;
        this.messagePort.on('message', this.work.bind(this));
        this.messagePort.on('error', (error) => {
            console.error(`Error in worker: ${error.message}`);
            this.stop();
        });
    }

    public async stop(): Promise<void> {
        this.messagePort.removeAllListeners();
        this.messagePort.close();
        this.isRunning = false;
    }

    public async work(message: string): Promise<void> {
        try {
            const parsedPayload = deserialize(message);
            switch (parsedPayload.header) {
                case "Execute":
                    const response = await this.executionCallback(parsedPayload);
                    const payload: IThreadCommunication = {
                        header: "Response",
                        payload: response
                    };
                    this.messagePort.postMessage(serialize(payload));
                    break;
                case "Shutdown":
                    await this.stop();
                    break;
                default:
                    throw new Error(`Unknown header: ${parsedPayload.header}`);
                    break;
            }

        } catch (error) {
            console.error(`Error in worker[${this.identity}]: ${error.message}`);
            const payload: IThreadCommunication = {
                header: "Error",
                payload: error.message
            };
            this.messagePort.postMessage(serialize(payload));
        }
    }

    public async [Symbol.asyncDispose]() {
        await this.stop();
    }
}


const longRunning = new LongRunningThread(parentPort, `${process.pid}-${threadId}`, async (input) => `${process.pid}-${threadId}` + input.payload);
longRunning.start();