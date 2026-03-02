import crypto from "node:crypto";
import { createClient } from "redis";
import { IRedisClientPool } from "redis-abstraction";

export class NodeRedisTestDriver implements IRedisClientPool {
    private activeClients = new Map<string, any>();

    public initialize(): Promise<void> {
        return Promise.resolve();
    }

    constructor(private readonly redisUrl: string) { }

    public async acquire(token: string): Promise<void> {
        if (this.activeClients.has(token)) {
            return;
        }
        const client = createClient({ url: this.redisUrl });
        await client.connect();
        this.activeClients.set(token, client);
    }

    public async release(token: string): Promise<void> {
        const client = this.activeClients.get(token);
        if (!client) {
            return;
        }
        this.activeClients.delete(token);
        await client.quit();
    }

    public async shutdown(): Promise<void> {
        const closeHandles = [...this.activeClients.values()].map((client) => client.quit());
        await Promise.allSettled(closeHandles);
        this.activeClients.clear();
    }

    public async run(token: string, commandArgs: string[]): Promise<any> {
        const client = this.getClient(token);
        return await client.sendCommand(commandArgs);
    }

    public async pipeline(token: string, commands: string[][], transaction: boolean): Promise<any> {
        const client = this.getClient(token);
        if (transaction) {
            const multi = client.multi();
            for (const command of commands) {
                multi.addCommand(command);
            }
            return await multi.exec();
        }

        const responses: any[] = [];
        for (const command of commands) {
            responses.push(await client.sendCommand(command));
        }
        return responses;
    }

    public async script(_token: string, _filePath: string, _keys: string[], _args: string[]): Promise<any> {
        throw new Error("Method not implemented.");
    }

    public generateUniqueToken(prefix: string): string {
        return `${prefix}-${crypto.randomUUID()}`;
    }

    private getClient(token: string): any {
        const client = this.activeClients.get(token);
        if (!client) {
            throw new Error("Please acquire a client with proper token");
        }
        return client;
    }
}