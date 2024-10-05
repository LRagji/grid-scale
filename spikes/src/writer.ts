import { createHash } from "node:crypto";
import { join } from "node:path";
import Database from 'better-sqlite3';
import { mkdirSync } from "node:fs";

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

export function writeData(writerIdentity: string, diskPaths: Array<string>, sampleSet: Array<[string, number[]]>, tagBucketWidth: number, timeBucketWidth: number, preConditionedTables: Set<string>, prefix = "D", sep = "|", timeBucketTolerance = 1): IWriterOutput {
    const hashAlgorithm = 'md5';
    const systemTime = Date.now();
    const systemTimeBucketed = systemTime - (systemTime % timeBucketWidth);
    const fileName = `${writerIdentity.toLowerCase()}.db`;
    const chunks = new Map<string, IChunkInfo>();
    const dataDisplacements = new Map<string, Set<string>>();

    for (let payloadIndex = 0; payloadIndex < sampleSet.length; payloadIndex += 1) {
        const tagName = sampleSet[payloadIndex][0] as string;
        const samples = sampleSet[payloadIndex][1] as number[];
        const hashBuffer = createHash(hashAlgorithm)
            .update(tagName)
            .digest()
        let bucket1 = (hashBuffer.readInt32LE(0) % diskPaths.length);
        let bucket2 = (hashBuffer.readInt32LE(4) % diskPaths.length);
        let bucket3 = (hashBuffer.readInt32LE(8) % diskPaths.length);
        let bucket4 = (hashBuffer.readInt32LE(12) % diskPaths.length);
        const diskIndex = Math.abs(bucket1 + bucket2 + bucket3 + bucket4) % diskPaths.length;
        const diskPath = diskPaths[diskIndex];

        bucket1 = hashBuffer.readInt32LE(0) - (hashBuffer.readInt32LE(0) % tagBucketWidth);
        bucket2 = hashBuffer.readInt32LE(4) - (hashBuffer.readInt32LE(4) % tagBucketWidth);
        bucket3 = hashBuffer.readInt32LE(8) - (hashBuffer.readInt32LE(8) % tagBucketWidth);
        bucket4 = hashBuffer.readInt32LE(12) - (hashBuffer.readInt32LE(12) % tagBucketWidth);
        const logicalChunkId = `${prefix}${sep}${bucket1}${sep}${bucket2}${sep}${bucket3}${sep}${bucket4}${sep}${systemTimeBucketed}`;

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
                if (sampleBucketedTime < (systemTimeBucketed - (timeBucketWidth * timeBucketTolerance)) || sampleBucketedTime > (systemTimeBucketed + (timeBucketWidth * timeBucketTolerance))) {
                    const misplacedLogicalChunkId = `${prefix}${sep}${bucket1}${sep}${bucket2}${sep}${bucket3}${sep}${bucket4}${sep}${sampleBucketedTime}`;
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