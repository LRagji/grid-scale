
import { join } from "node:path";
import Database from 'better-sqlite3';
import { mkdirSync } from "node:fs";
import { ChunkCalculator, DJB2Calculator, IChunkId, MD5Calculator } from "./chunk-calculator.js";

export interface IChunkInfo {
    incorrectPlacements: number;
    absolutePath: string;
    upserts: number;
}

export interface IWriterOutput {
    preConditionedTables: Set<string>;
    chunks: Map<string, IChunkInfo>;
    dataDisplacements: Map<string, Set<string>>
}

export function writeData(diskPaths: Array<string>, sampleSet: Array<[string, number[]]>, tagBucketWidth: number, timeBucketWidth: number, preConditionedTables: Set<string>, fileName: string, prefix: string, seperatorChar: string, timeBucketTolerance = 1, systemTime = Date.now(), calculator: ChunkCalculator = DJB2Calculator): IWriterOutput {
    const chunks = new Map<string, IChunkInfo>();
    const dataDisplacements = new Map<string, Set<string>>();

    for (let payloadIndex = 0; payloadIndex < sampleSet.length; payloadIndex += 1) {
        const tagName = sampleSet[payloadIndex][0] as string;
        const samples = sampleSet[payloadIndex][1] as number[];

        const chunkId = calculator(tagName, systemTime, tagBucketWidth, timeBucketWidth);
        const diskPath = diskPaths[chunkId.limitIndex(diskPaths.length)];
        const logicalChunkId = chunkId.logicalChunkId(prefix, seperatorChar);

        const dbPath = join(diskPath, logicalChunkId, fileName);
        let db: Database.Database;

        const tagNameInHex = Buffer.from(tagName, "utf-8").toString('hex');
        const preConditionedTable = `${dbPath}:${tagName}`;
        if (!preConditionedTables.has(preConditionedTable)) {
            mkdirSync(join(diskPath, logicalChunkId), { recursive: true });
            db = new Database(dbPath);
            db.pragma('journal_mode = WAL');
            const table = (tableName: string) => `CREATE TABLE IF NOT EXISTS [${tableName}] (sampleTime INTEGER PRIMARY KEY NOT NULL, insertTime INTEGER NOT NULL, nValue INTEGER, oValue TEXT);`;
            const rangeSummaryIndex = (tableName: string) => `CREATE INDEX IF NOT EXISTS [timeIndex_${tableName}] ON [${tableName}] (sampleTime,insertTime,nValue);`;

            db.exec(table(tagNameInHex));
            db.exec(rangeSummaryIndex(tagNameInHex));
            preConditionedTables.add(preConditionedTable);
        }
        else {
            db = new Database(dbPath);
            db.pragma('journal_mode = WAL');
        }

        const preparedUpsert = db.prepare(`Insert into [${tagNameInHex}] (sampleTime,insertTime,nValue) values (?,?,?)
            ON CONFLICT(sampleTime) DO UPDATE SET
            insertTime=excluded.insertTime,
            nValue=excluded.nValue;`);

        const chunkInfo = chunks.get(logicalChunkId) || { incorrectPlacements: 0, absolutePath: dbPath, upserts: 0 } as IChunkInfo;

        db.transaction(() => {
            for (let i = 0; i < samples.length; i += 2) {
                const sampleTime = samples[i];
                const sampleBucketedTime = sampleTime - (sampleTime % timeBucketWidth);
                if (sampleBucketedTime < (chunkId.timePart - (timeBucketWidth * timeBucketTolerance)) || sampleBucketedTime > (chunkId.timePart + (timeBucketWidth * timeBucketTolerance))) {
                    const misplacedChunkId: IChunkId = { timePart: sampleBucketedTime, tagHash: chunkId.tagHash, tagPart: chunkId.tagPart, limitIndex: chunkId.limitIndex, logicalChunkId: chunkId.logicalChunkId };
                    const misplacedLogicalChunkId = misplacedChunkId.logicalChunkId(prefix, seperatorChar);
                    const dataPlacement = dataDisplacements.get(misplacedLogicalChunkId) || new Set<string>();
                    dataPlacement.add(logicalChunkId);
                    dataDisplacements.set(misplacedLogicalChunkId, dataPlacement);
                    chunkInfo.incorrectPlacements++;
                }
                preparedUpsert.run(sampleTime, systemTime, samples[i + 1]);
            }
        })();
        db.close();

        chunkInfo.absolutePath = dbPath;
        chunkInfo.upserts += samples.length / 2;
        chunks.set(logicalChunkId, chunkInfo);
    }
    return { preConditionedTables, dataDisplacements, chunks } as IWriterOutput;
}

export interface IRedisHashMap {
    set(key: string, fieldValues: string[]): Promise<void>;
    get(key: string, fieldsOnly: boolean): Promise<string[]>;
    del(keys: string[]): Promise<void>;
}

export async function updateDisplacedData(displacedData: Map<string, Set<string>>, writerIdentity: string, redisConnection: IRedisHashMap): Promise<void> {
    let handles = Array<Promise<void>>();
    displacedData.forEach(async (value, key) => {
        const values = [];
        for (const item of value) {
            values.push(item);
            values.push(writerIdentity);
        }
        handles.push(redisConnection.set(key, values));
        if (handles.length === 1000) {
            await Promise.all(handles);
            handles = [];
        }
    });
    await Promise.all(handles);
}