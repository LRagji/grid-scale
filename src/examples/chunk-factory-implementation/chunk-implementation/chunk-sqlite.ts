import Database from "better-sqlite3";
import { InjectableConstructor } from "node-apparatus";
import { join } from "node:path";
import { mkdirSync, readdirSync, watch, WatchEventType, FSWatcher } from "node:fs";
import { ShardAccessMode } from "../../../types/shard-access-mode.js";
import { IChunk } from "../../../chunk/i-chunk.js";

/**
 * This is an example ChunkBase implementation using SQLite
 * This class uses better-sqlite3 to interact with SQLite database
 * It has a fixed schema with 5 columns,(you can write your own schema)
 * 1. sampleTime: The time of the sample
 * 2. insertTime: The time of the insertion
 * 3. nValue: The numeric value
 * 4. oValue: The other value
 * 5. tag: The tag name(Virtual column)
 */
export default class ChunkSqlite implements IChunk {

    private readonly filePrefix = "ts";
    private readonly filePostfix = ".gs";
    private readonly searchRegex = new RegExp("^" + this.filePrefix + "[a-z0-9-]+\\" + this.filePostfix + "$");//`^ts[a-z0-9]+\\.gs$`;

    private readonly db: Database.Database;
    private readonly readonlyDBs = new Array<Database.Database>();
    private readCursorOpen = false;
    private reconcileDirectory = false;
    private directoryWatcher: FSWatcher;
    private readonly reconcileHandler = this.reconcileDirectoryChanges.bind(this);
    private readonly readonlyDBActivePaths = new Set<string>();
    private readonly preConditionedSectors = new Set<string>();
    private preparedMetadataUpsert: Database.Statement = undefined;
    private readonly metadataTableSqlStatement = `CREATE TABLE IF NOT EXISTS [metadata] (key TEXT PRIMARY KEY NOT NULL, value TEXT);`;
    private readonly metadataUpsertSqlStatement = `INSERT INTO [metadata] (key,value) values (?,?) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value;`;
    private readonly tableSqlStatement = (tableName: string) => `CREATE TABLE IF NOT EXISTS [${tableName}] (sampleTime INTEGER PRIMARY KEY NOT NULL, insertTime INTEGER NOT NULL, nValue INTEGER, oValue TEXT);`;
    private readonly indexSqlStatement = (tableName: string) => `CREATE INDEX IF NOT EXISTS [timeIndex_${tableName}] ON [${tableName}] (sampleTime,insertTime,nValue);`;
    private readonly upsertSqlStatement = (tableName: string) => `INSERT INTO [${tableName}] (sampleTime,insertTime,nValue,oValue) values (?,?,?,?)
            ON CONFLICT(sampleTime) DO UPDATE SET
            insertTime=EXCLUDED.insertTime,
            nValue=EXCLUDED.nValue,
            oValue=EXCLUDED.oValue;`;

    constructor(
        private readonly directoryPath: string,
        private readonly mode: ShardAccessMode,
        private readonly mergeFunction: <T>(cursors: IterableIterator<T>[]) => IterableIterator<T>,
        callerSignature: string,
        private readonly injectableConstructor: InjectableConstructor = new InjectableConstructor()) {

        if (this.mode === "write") {
            const fullPath = join(directoryPath, (this.filePrefix + callerSignature + this.filePostfix));
            mkdirSync(directoryPath, { recursive: true });
            this.db = injectableConstructor.createInstance<Database.Database>(Database, [fullPath]);
            this.db.pragma('journal_mode = WAL');
        }
        else {
            this.searchAndOpenDatabases();
        }

    }

    // public metadataSet(key: string, value: string): void {
    //     if (this.mode === "write") {
    //         if (this.preparedMetadataUpsert === undefined) {
    //             this.db.exec(this.metadataTableSqlStatement);
    //             this.preparedMetadataUpsert = this.db.prepare(this.metadataUpsertSqlStatement);
    //         }
    //         this.db.transaction(() => {
    //             this.preparedMetadataUpsert.run(key, value);
    //         })();
    //     }
    // }

    // public metadataGet(key: string, defaultValue: string): string[] {
    //     if (this.mode === "write") {
    //         return [this.db.prepare(`SELECT value FROM [metadata] WHERE key = ?;`)
    //             .pluck()
    //             .get(key) as string ?? defaultValue];
    //     }
    //     else {
    //         return this.readonlyDBs.map(db => db.prepare(`SELECT value FROM [metadata] WHERE key = ?;`)
    //             .pluck()
    //             .get(key) as string ?? defaultValue)
    //     }
    // }

    private searchAndOpenDatabases() {
        try {
            const fileEntries = readdirSync(this.directoryPath, { recursive: false, withFileTypes: true });
            for (const fileEntry of fileEntries) {
                const fullPath = join(this.directoryPath, fileEntry.name);
                if (fileEntry.isFile() && this.searchRegex.test(fileEntry.name) && this.readonlyDBActivePaths.has(fullPath) === false) {
                    this.readonlyDBActivePaths.add(fullPath);
                    const openedDb = this.injectableConstructor.createInstanceWithoutConstructor<Database.Database>((dbPath, dbOptions) => {
                        return new Database(dbPath, dbOptions);
                    }, [fullPath, { readonly: true, fileMustExist: true }]);
                    this.readonlyDBs.push(openedDb);
                }
            }

            this.reconcileDirectory = false;
            this.directoryWatcher = watch(this.directoryPath, { recursive: false }, this.reconcileHandler);
        }
        catch (e) {
            this.reconcileDirectory = true; ``
            if (e.code === 'ENOENT') {
                return;//No Directory exists so no DB or Data
            }
            else {
                throw e;
            }
        }
    }

    private reconcileDirectoryChanges(eventType: WatchEventType, filename: string) {
        if (filename !== undefined && this.searchRegex.test(filename) && this.readonlyDBActivePaths.has(filename) === false) {
            this.reconcileDirectory = true;
            if (this.directoryWatcher !== undefined) {
                this.directoryWatcher.close();
                this.directoryWatcher = undefined;
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
            if (this.reconcileDirectory === true) {
                this.searchAndOpenDatabases();
            }
            const tagsInHex = tags.map(tag => Buffer.from(tag, "utf-8").toString('hex'));
            const cursors = new Array<IterableIterator<any[]>>();
            const SQLITE_MAX_VARIABLE_NUMBER = 3000;   //This limit is to prevent error from too many tag names in variable, SQLITE_MAX_VARIABLE_NUMBER comes from https://www.sqlite.org/limits.html
            while (tagsInHex.length > 0) {
                const chunkedTags = tagsInHex.splice(0, SQLITE_MAX_VARIABLE_NUMBER);
                for (const db of this.readonlyDBs) {
                    const existingTables = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name IN (${chunkedTags.map(_ => "?").join(",")});`)
                        .raw()
                        .all(chunkedTags) as string[][];
                    if (existingTables.length === 0) {
                        continue;
                    }
                    const sqlStatement = this.selectSqlStatement(existingTables.flat(), startTimeInclusive, endTimeExclusive);
                    const iterator = db.prepare<unknown[], any[]>(sqlStatement)
                        .raw()
                        .iterate()
                    cursors.push(iterator);
                }
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
                if (this.directoryWatcher !== undefined) {
                    this.directoryWatcher.close();
                    this.directoryWatcher = undefined;
                }
                this.readonlyDBs.forEach(db => db.close());
                this.readonlyDBs.length = 0;
                this.readonlyDBActivePaths.clear();
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
