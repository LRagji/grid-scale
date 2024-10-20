export interface ISyncWork {
    setup(parameters: any[]): void;
    messageLoop(payload: any[], returnCallback: (returnValue) => void): void;
    [Symbol.dispose](): void;
}