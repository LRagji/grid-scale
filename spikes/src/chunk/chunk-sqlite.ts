import Database from "better-sqlite3";
import { BootstrapConstructor } from "express-service-bootstrap";
import { join } from "node:path";
import { mkdirSync, readdirSync } from "node:fs";
import { ShardAccessMode } from "../types/shard-access-mode.js";
import { IChunk } from "./i-chunk.js";


export class ChunkSqlite implements IChunk {

    private readonly db: Database.Database;
    private readonly readonlyDBs = new Array<Database.Database>();
    private readCursorOpen = false;
    private readonly preConditionedSectors = new Set<string>();
    private readonly tableSqlStatement = (tableName: string) => `CREATE TABLE IF NOT EXISTS [${tableName}] (sampleTime INTEGER PRIMARY KEY NOT NULL, insertTime INTEGER NOT NULL, nValue INTEGER, oValue TEXT);`;
    private readonly indexSqlStatement = (tableName: string) => `CREATE INDEX IF NOT EXISTS [timeIndex_${tableName}] ON [${tableName}] (sampleTime,insertTime,nValue);`;
    private readonly upsertSqlStatement = (tableName: string) => `INSERT INTO [${tableName}] (sampleTime,insertTime,nValue) values (?,?,?)
            ON CONFLICT(sampleTime) DO UPDATE SET
            insertTime=EXCLUDED.insertTime,
            nValue=EXCLUDED.nValue;`;

    constructor(
        private readonly directoryPath: string,
        private readonly mode: ShardAccessMode,
        private readonly mergeFunction: <T>(cursors: IterableIterator<T>[]) => IterableIterator<T>,
        writeFileName: string,
        searchRegex: RegExp,
        injectableConstructor: BootstrapConstructor = new BootstrapConstructor()) {
        if (this.mode === "write") {
            const fullPath = join(directoryPath, writeFileName)
            mkdirSync(directoryPath, { recursive: true });
            this.db = injectableConstructor.createInstance<Database.Database>(Database, [fullPath]);
            this.db.pragma('journal_mode = WAL');
        }
        else {
            try {
                this.readonlyDBs = readdirSync(directoryPath, { recursive: false, withFileTypes: true })
                    .filter(dirent => searchRegex.test(dirent.name) && dirent.isFile())
                    .map(dirent => {
                        return injectableConstructor.createInstanceWithoutConstructor<Database.Database>((dbPath, dbOptions) => {
                            return new Database(dbPath, dbOptions);
                        }, [join(directoryPath, dirent.name), { readonly: true, fileMustExist: true }]);
                    });
            }
            catch (e) {
                if (e.code === 'ENOENT') {
                    return;//No Directory exists so no DB or Data
                }
                else {
                    throw e;
                }
            }
        }
    }

    public bulkSet(records: Map<string, any[][]>): void {
        if (this.mode === "read") {
            throw new Error(`ChunkShard ${this.directoryPath} is readonly, cannot set values`);
        }
        for (const [tag, rows] of records) {
            const tagNameInHex = Buffer.from(tag, "utf-8")
                .toString('hex');

            if (!this.preConditionedSectors.has(tag)) {
                this.db.exec(this.tableSqlStatement(tagNameInHex));
                this.db.exec(this.indexSqlStatement(tagNameInHex));
                this.preConditionedSectors.add(tag);
            }
            const preparedUpsert = this.db.prepare(this.upsertSqlStatement(tagNameInHex));

            this.db.transaction(() => {
                for (let i = 0; i < rows.length; i++) {
                    //TODO: Check if preparedUpsert.run(rows[i]) can be batched or bulk inserted.
                    preparedUpsert.run(rows[i]);
                }
            })();
        }
    }

    public *bulkIterator(tags: string[], startTimeInclusive: number, endTimeExclusive: number): IterableIterator<any[]> {
        try {
            this.readCursorOpen = true;
            const tagsInHex = tags.map(tag => Buffer.from(tag, "utf-8").toString('hex'));
            const cursors = new Array<IterableIterator<any[]>>();
            while (tagsInHex.length > 0) {
                //This limit is to prevent SQLITE_MAX_VARIABLE_NUMBER comes from https://www.sqlite.org/limits.html
                const chunkedTags = tagsInHex.splice(0, 3000);
                const chunkCursors = this.readonlyDBs.map(db => {
                    const existingTables = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name IN (${chunkedTags.map(_ => "?").join(",")});`)
                        .raw()
                        .all(chunkedTags) as string[][];
                    const sqlStatement = this.selectSqlStatement(existingTables.flat(), startTimeInclusive, endTimeExclusive);
                    return db.prepare<unknown[], any[]>(sqlStatement)
                        .raw()
                        .iterate()
                });
                cursors.push(...chunkCursors);
            }
            if (cursors.length === 1) {
                yield* cursors[0];
            }
            else if (cursors.length === 0) {
                return;
            }
            else {
                yield* this.mergeFunction(cursors);
            }
        }
        finally {
            this.readCursorOpen = false;
        }
    }

    public canBeDisposed(): boolean {
        return this.mode === "write" ? this.canWriterBeDisposed() : this.canReaderBeDisposed();
    }

    private selectSqlStatement(tagsInHex: string[], startInclusive: number, endExclusive: number): string {
        const unionStatement = tagsInHex
            .map(tagNameInHex => {
                return `SELECT *,cast( unhex('${tagNameInHex}') as TEXT) as tagName 
                FROM [${tagNameInHex}] 
                WHERE sampleTime >= ${startInclusive} AND sampleTime < ${endExclusive}`;
            })
            .join("\n UNION ALL \n");
        return `SELECT * 
        FROM (${unionStatement}) 
        ORDER BY tagName,sampleTime ASC;`;

    }

    private canWriterBeDisposed() {
        return this.db !== undefined && this.db.open && !this.db.inTransaction;
    }

    private canReaderBeDisposed() {
        return this.readonlyDBs.filter(db => db.open).every(db => !db.inTransaction) && this.readCursorOpen === false;
    }

    public [Symbol.dispose]() {
        if (this.mode === "write") {
            if (this.canWriterBeDisposed()) {
                this.db.close();
            }
            else {
                throw new Error(`Chunk ${this.directoryPath} is in write use, was requested to close`);
            }
        }
        else {
            if (this.canReaderBeDisposed()) {
                this.readonlyDBs.forEach(db => db.close());
                this.readonlyDBs.length = 0;
            }
            else {
                throw new Error(`Chunk ${this.directoryPath} is in read use, was requested to close`);
            }
        }
    }

    public [Symbol.asyncDispose](): Promise<void> {
        return Promise.resolve(this[Symbol.dispose]());
    }

}