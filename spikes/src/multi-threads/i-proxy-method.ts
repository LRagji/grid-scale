import v8 from "node:v8"

export interface IProxyMethod {
    workerId: number;
    invocationId: number;
    methodName: string;
    methodArguments: any[];
    returnValue: any;
    error?: string
}

export const DisposeMethodPayload = { methodName: "Dispose", methodArguments: [], returnValue: null } as IProxyMethod;

export function deserialize(payload: Buffer): IProxyMethod {
    return v8.deserialize(payload) as IProxyMethod;
}

export function serialize(input: IProxyMethod): Buffer {
    return v8.serialize(input);
}