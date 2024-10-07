import Database from "better-sqlite3";
import { TConfig } from "./t-config.js";
import { mkdirSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { ChunkId } from "./chunk.js";

type TChunkAccessMode = "readwrite" | "readonly";

class ChunkShard {
    private db: Database.Database;
    private readonly preConditionedSectors = new Set<string>();
    private readonly tableSqlStatement = (tableName: string) => `CREATE TABLE IF NOT EXISTS [${tableName}] (sampleTime INTEGER PRIMARY KEY NOT NULL, insertTime INTEGER NOT NULL, nValue INTEGER, oValue TEXT);`;
    private readonly indexSqlStatement = (tableName: string) => `CREATE INDEX IF NOT EXISTS [timeIndex_${tableName}] ON [${tableName}] (sampleTime,insertTime,nValue);`;
    private readonly upsertSqlStatement = (tableName: string) => `Insert into [${tableName}] (sampleTime,insertTime,nValue) values (?,?,?)
            ON CONFLICT(sampleTime) DO UPDATE SET
            insertTime=excluded.insertTime,
            nValue=excluded.nValue;`;

    constructor(private readonly path: string, private readonly mode: TChunkAccessMode) {
        if (this.mode === "readwrite") {
            mkdirSync(dirname(path), { recursive: true });
            this.db = new Database(path);
            this.db.pragma('journal_mode = WAL');
        }
        else {
            this.db = new Database(path, { readonly: true, fileMustExist: true });
        }
    }

    public set<T>(sectorName: string, values: T[][]): void {
        if (this.mode === "readonly") {
            throw new Error(`ChunkShard ${this.path} is readonly, cannot set values`);
        }
        const tagNameInHex = Buffer.from(sectorName, "utf-8")
            .toString('hex');

        if (!this.preConditionedSectors.has(sectorName)) {
            this.db.exec(this.tableSqlStatement(tagNameInHex));
            this.db.exec(this.indexSqlStatement(tagNameInHex));
            this.preConditionedSectors.add(sectorName);
        }
        const preparedUpsert = this.db.prepare(this.upsertSqlStatement(tagNameInHex));

        this.db.transaction(() => {
            for (let i = 0; i < values.length; i++) {
                preparedUpsert.run(values[i]);
            }
        })();
    }

    public get<T>(sectorNames: string[], startInclusive: number, endExclusive: number): IterableIterator<T> {
        return this.db.prepare<unknown, T>(this.selectSqlStatement(sectorNames, startInclusive, endExclusive))
            .iterate(null);
    }

    private selectSqlStatement(tagNames: string[], startInclusive: number, endExclusive: number): string {
        return tagNames
            .map(tagName => {
                const tagNamesInHex = Buffer.from(tagName, "utf-8").toString('hex');
                return `SELECT * FROM [${tagNamesInHex}] WHERE sampleTime >= ${startInclusive} AND sampleTime < ${endExclusive} ORDER BY sampleTime ASC;`;
            })
            .join("\n UNION ALL \n");

    }

    public canBeClosed() {
        return this.db !== undefined && this.db.open && !this.db.inTransaction;
    }

    [Symbol.dispose]() {
        if (this.canBeClosed()) {
            this.db.close();
        }
        else {
            throw new Error(`ChunkShard ${this.path} is in use, was requested to close`);
        }
    }
}

export class ChunkContainer {

    private readonly chunkShardsCache = new Map<string, ChunkShard>();
    private readonly chunkNameRegex: RegExp;

    constructor(private readonly config: TConfig, private readonly id: ChunkId) {
        this.chunkNameRegex = new RegExp("^" + this.config.fileNamePre + "[a-z0-9]+\\" + this.config.fileNamePost + "$");//`^ts[a-z0-9]+\\.db$`;
    }

    private getChunkShard(chunkShardPath: string, mode: TChunkAccessMode): ChunkShard {
        //TODO: Implement a cache eviction policy
        const key = chunkShardPath + mode;
        if (this.chunkShardsCache.has(key)) {
            return this.chunkShardsCache.get(chunkShardPath + mode)
        }
        else {
            const chunkShard = new ChunkShard(chunkShardPath, mode);
            this.chunkShardsCache.set(key, chunkShard);
            return chunkShard;
        }
    }

    public set<T>(chunkId: ChunkId, sectorValues: Map<string, T[][]>, selfId: string,): void {
        sectorValues.forEach((values, sectorName) => {
            const diskPaths = this.config.setPaths.get(this.config.activePath) || ["./default-data/"];
            const diskPath = diskPaths[chunkId.tagNameMod(diskPaths.length)];
            const dbPath = join(diskPath, chunkId.logicalChunkId, `${this.config.fileNamePre}${selfId}${this.config.fileNamePost}`);
            const shard = this.getChunkShard(dbPath, "readwrite");
            shard.set<T>(sectorName, values);
        });
    }

    public get<T>(chunkId: ChunkId, sectorNames: string[], startInclusive: number, endExclusive: number): IterableIterator<T>[] {
        const returnIterators = new Array<IterableIterator<T>>();
        this.config.setPaths.forEach((diskPaths, activePath) => {
            const diskPath = diskPaths[chunkId.tagNameMod(diskPaths.length)];
            const directoryPath = join(diskPath, chunkId.logicalChunkId);
            let matchingChunkPaths = new Array<string>();
            try {
                matchingChunkPaths = readdirSync(directoryPath, { recursive: false, withFileTypes: true })
                    .filter(dirent => this.chunkNameRegex.test(dirent.name) && dirent.isFile())
                    .map(dirent => join(directoryPath, dirent.name));
            }
            catch (e) {
                if (e.code === 'ENOENT') {
                    return;//No Directory exists so no DB or Data
                }
                else {
                    throw e;
                }
            }
            matchingChunkPaths.forEach(chunkShardPath => {
                try {
                    const chunkShard = this.getChunkShard(chunkShardPath, "readonly");
                    returnIterators.push(chunkShard.get<T>(sectorNames, startInclusive, endExclusive));
                }
                catch (e) {
                    console.log(`DB query failed for ${chunkShardPath}`);
                    return;//No DB or Data
                }
            });
        });
        return returnIterators;
    }

    [Symbol.dispose]() {
        this.chunkShardsCache.forEach((value, key) => {
            if (value.canBeClosed()) {
                value[Symbol.dispose]();
                this.chunkShardsCache.delete(key);
            }
        });
    }

}