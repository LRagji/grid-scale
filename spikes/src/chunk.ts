import Database from "better-sqlite3";
import { TConfig } from "./t-config.js";
import { mkdirSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";

type TChunkAccessMode = "readwrite" | "readonly";

export class ChunkShard {
    private db: Database.Database;
    private readCursorOpen = false;
    private readonly preConditionedSectors = new Set<string>();
    private readonly tableSqlStatement = (tableName: string) => `CREATE TABLE IF NOT EXISTS [${tableName}] (sampleTime INTEGER PRIMARY KEY NOT NULL, insertTime INTEGER NOT NULL, nValue INTEGER, oValue TEXT);`;
    private readonly indexSqlStatement = (tableName: string) => `CREATE INDEX IF NOT EXISTS [timeIndex_${tableName}] ON [${tableName}] (sampleTime,insertTime,nValue);`;
    private readonly upsertSqlStatement = (tableName: string) => `INSERT INTO [${tableName}] (sampleTime,insertTime,nValue) values (?,?,?)
            ON CONFLICT(sampleTime) DO UPDATE SET
            insertTime=EXCLUDED.insertTime,
            nValue=EXCLUDED.nValue;`;

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

    public acquireAutoReleaseReadCursor() {
        this.readCursorOpen = true;
    }

    public releaseReadCursor() {
        this.readCursorOpen = false;
    }

    public *get<T>(sectorNames: string[], startInclusive: number, endExclusive: number): IterableIterator<T> {
        try {
            this.readCursorOpen = true;
            const cursor = this.db.prepare<unknown[], T>(this.selectSqlStatement(sectorNames, startInclusive, endExclusive))
                .raw()
                .iterate();
            yield* cursor;
        }
        finally {
            this.readCursorOpen = false;
        }
    }

    private selectSqlStatement(tagNames: string[], startInclusive: number, endExclusive: number): string {
        const unionStatement = tagNames
            .map(tagName => {
                const tagNameInHex = Buffer.from(tagName, "utf-8").toString('hex');
                return `SELECT *,cast( unhex('${tagNameInHex}') as TEXT) as tagName 
                FROM [${tagNameInHex}] 
                WHERE sampleTime >= ${startInclusive} AND sampleTime < ${endExclusive}`;
            })
            .join("\n UNION ALL \n");
        return `SELECT * 
        FROM (${unionStatement}) 
        ORDER BY tagName,sampleTime ASC;`;

    }

    public canBeDisposed() {
        return this.db !== undefined && this.db.open && !this.db.inTransaction && !this.readCursorOpen;
    }

    public [Symbol.dispose]() {
        if (this.canBeDisposed()) {
            this.db.close();
        }
        else {
            throw new Error(`ChunkShard ${this.path} is in use, was requested to close`);
        }
    }
}

export class Chunk {
    private readonly shardsCache = new Map<string, ChunkShard>();
    private readonly chunkNameRegex: RegExp;

    constructor(private readonly config: TConfig, private readonly logicalId: string) {
        this.chunkNameRegex = new RegExp("^" + this.config.fileNamePre + "[a-z0-9-]+\\" + this.config.fileNamePost + "$");//`^ts[a-z0-9]+\\.db$`;
    }

    private getChunkShard(chunkShardPath: string, mode: TChunkAccessMode): ChunkShard {
        //TODO: Implement a cache eviction policy
        const key = chunkShardPath + mode;
        if (this.shardsCache.has(key)) {
            return this.shardsCache.get(chunkShardPath + mode)
        }
        else {
            const chunkShard = new ChunkShard(chunkShardPath, mode);
            this.shardsCache.set(key, chunkShard);
            return chunkShard;
        }
    }

    public set<T extends Array<any>>(diskIndex: number, sectorValues: Map<string, T[]>, selfId: string,): void {
        sectorValues.forEach((values, sectorName) => {
            const parentDirectory = this.config.activePath;
            const diskPaths = this.config.setPaths.get(parentDirectory) || ["./default-data/"];
            const dbPath = join(parentDirectory, diskPaths[diskIndex], this.logicalId, `${this.config.fileNamePre}${selfId}${this.config.fileNamePost}`);
            const shard = this.getChunkShard(dbPath, "readwrite");
            shard.set<T>(sectorName, values);
        });
    }

    public get<T>(diskIndexes: Map<string, number>, sectorNames: string[], startInclusive: number, endExclusive: number): IterableIterator<T>[] {
        const returnIterators = new Array<IterableIterator<T>>();
        diskIndexes.forEach((diskIndex, setPath) => {
            if (this.config.setPaths.has(setPath) === false) {
                return;//No set exists so no DB or Data
            }
            const diskPath = this.config.setPaths.get(setPath)[diskIndex];
            const directoryPath = join(setPath, diskPath, this.logicalId);
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
                    chunkShard.acquireAutoReleaseReadCursor();
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

    public canBeDisposed(): boolean {
        for (const value of this.shardsCache.values()) {
            if (value.canBeDisposed() === false) {
                return false;//Return false on first non-disposable
            }
        }
        return true;
    }

    public [Symbol.dispose]() {
        this.shardsCache.forEach((value, key) => {
            if (value.canBeDisposed()) {
                value[Symbol.dispose]();
                this.shardsCache.delete(key);
            }
        });
    }
}