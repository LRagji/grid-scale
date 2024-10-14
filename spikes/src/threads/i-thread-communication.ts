import v8 from "node:v8"

export enum HeaderPayload { "Shutdown" = "Shutdown", "Response" = "Response", "Error" = "Error" }

export interface IThreadCommunication<payloadType> {
    payload: payloadType;
    header: string | HeaderPayload;
    subHeader: string;
}

export function deserialize<T>(payload: Buffer): IThreadCommunication<T> {
    return v8.deserialize(payload) as IThreadCommunication<T>;
}

export function serialize<T>(input: IThreadCommunication<T>): Buffer {
    return v8.serialize(input);
}