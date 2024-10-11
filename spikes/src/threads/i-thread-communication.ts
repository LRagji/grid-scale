import v8 from "node:v8"

export interface IThreadCommunication<payloadType> {
    payload: payloadType;
    header: "Error" | "Response" | "Execute" | "Shutdown";
    subHeader: string;
}

export function deserialize(payload: Buffer): IThreadCommunication<any> {
    return v8.deserialize(payload) as IThreadCommunication<any>;
}

export function serialize(input: IThreadCommunication<any>): string {
    return JSON.stringify(input);
}