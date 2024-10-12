import v8 from "node:v8"

export interface IThreadCommunication<payloadType> {
    payload: payloadType;
    header: string | "Shutdown" | "Response" | "Error";
    subHeader: string;
}

export function deserialize<T>(payload: Buffer): IThreadCommunication<T> {
    return v8.deserialize(payload) as IThreadCommunication<T>;
}

export function serialize<T>(input: IThreadCommunication<T>): Buffer {
    return v8.serialize(input);
}