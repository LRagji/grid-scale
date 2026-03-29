import { ApplicationBuilder, ApplicationStartupStatus, ApplicationTypes, Convenience, DisposableSingletonContainer, EnvironmentVariables, IRouter, Request } from "express-service-bootstrap";
import { Job, Worker, WorkerOptions } from 'bullmq';
import { parseURL } from "ioredis/built/utils/index.js";
import { BullMQOtel } from "bullmq-otel";
import IORedis, { Cluster } from "ioredis";
import { mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { IPageInfo, RDriver } from "../../src/index.js";
import { RedisTsPage, TimeseriesSample } from "../../src/pages/redis-ts-page.js";
import { DIConstants, EnvironmentVariableConstants, PageWindowDefaults } from "./constants.js";
import { IORedisClientPool, IRedisClientPool } from "redis-abstraction";

class DisposableWorker implements AsyncDisposable {
    public readonly Worker: Worker<IPageInfo>;

    constructor(queueName: string, handler: (job: Job<IPageInfo>, token?: string, abortSignal?: AbortSignal) => Promise<void>, options: WorkerOptions) {
        this.Worker = new Worker<IPageInfo>(queueName, handler, options);
    }

    public async [Symbol.asyncDispose](): Promise<void> {
        this.Worker.cancelAllJobs('Disposed');
        await this.Worker.close();
    }
}

const applicationName = process.env.OTEL_SERVICE_NAME || "HDFS-SQLite-Checkpointer";
const app = new ApplicationBuilder(applicationName);
const utilities = new Convenience();
const defaultRedisConnectionString = "redis://localhost:6379";

async function initializeGridScale(DIContainer: DisposableSingletonContainer) {
    const env = DIContainer.createInstance<EnvironmentVariables>(DIConstants.EnvVars, EnvironmentVariables, []);
    const timeToleranceInMs = parseInt(env.getStringOrDefault(EnvironmentVariableConstants.TimeToleranceInMs, PageWindowDefaults.timeToleranceInMs), 10);
    const tempCheckPointingPath = env.getStringOrDefault(EnvironmentVariableConstants.TempCheckPointingPath, tmpdir());
    DIContainer.createInstanceWithoutConstructor(EnvironmentVariableConstants.TempCheckPointingPath, () => tempCheckPointingPath);
    DIContainer.createInstanceWithoutConstructor(EnvironmentVariableConstants.TimeToleranceInMs, () => timeToleranceInMs);
    const redisConnectionString = env.getStringOrDefault(EnvironmentVariableConstants.RedisConnectionString, defaultRedisConnectionString);
    const parseRedisConnectionString = (connectionString: string) => parseURL(connectionString);
    const connectionInjector = () => IORedisClientPool.IORedisClientClusterFactory([redisConnectionString], IORedis as any, Cluster as any, parseRedisConnectionString);
    const redisPoolDriver = DIContainer.createInstance<IRedisClientPool>(DIConstants.RedisClientPool, IORedisClientPool, [connectionInjector]);
    const redisDriver = DIContainer.createInstance<RDriver>(DIConstants.RDriver, RDriver, [redisPoolDriver, timeToleranceInMs]);
    await redisDriver.initialize();
    const queName = env.getStringOrDefault(EnvironmentVariableConstants.DistributionQueueName, "distribution_queue");
    const queConnectionParams = parseRedisConnectionString(redisConnectionString);
    const context = { DIContainer };
    DIContainer.createInstance<DisposableWorker>(DIConstants.CheckpointQueue, DisposableWorker, [queName, checkpointHandler.bind(context), {
        connection: queConnectionParams,
        concurrency: 1,
        telemetry: new BullMQOtel({
            tracerName: applicationName,
            meterName: `${applicationName}-BULLMQ`,
            enableMetrics: true
        })
    } as WorkerOptions]);
}

function setupRoutes(rootRouter: IRouter) {
    //NO API routes needed for this worker
}

function dumpPageElementsToSqlite(pageInfo: IPageInfo, pageKey: string, pageElements: TimeseriesSample[], tempDir = tmpdir()): string {
    const checkpointDirectory = join(tempDir, "grid-scale-checkpoints");
    mkdirSync(checkpointDirectory, { recursive: true });
    const fileName = Buffer.from(pageKey).toString("base64url");
    const databasePath = join(checkpointDirectory, `${fileName}-${Date.now()}.sqlite`);
    const database = new DatabaseSync(databasePath);

    try {
        database.exec(`
            CREATE TABLE IF NOT EXISTS page_elements (
                group_key TEXT NOT NULL,
                element_rank INTEGER NOT NULL,
                sequence_number INTEGER NOT NULL,
                element_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS metadata (
                page_key TEXT PRIMARY KEY,
                start_time INTEGER NOT NULL,
                start_size INTEGER NOT NULL,
                start_serial_number INTEGER NOT NULL,
                checkpointTime INTEGER NOT NULL
            );
        `);

        const insertMetadataStatement = database.prepare(`
            INSERT INTO metadata (
                page_key,
                start_time,
                start_size,
                start_serial_number,
                checkpointTime
            ) VALUES ( ?, ?, ?, ?, ?)
        `);

        const insertStatement = database.prepare(`
            INSERT INTO page_elements (
                group_key,
                element_rank,
                sequence_number,
                element_json
            ) VALUES ( ?, ?, ?, ?)
        `);

        database.exec("BEGIN TRANSACTION");

        insertMetadataStatement.run(
            pageKey,
            pageInfo.startTime,
            pageInfo.startSize,
            pageInfo.startSerialNumber,
            Date.now()
        );

        for (const element of pageElements) {
            insertStatement.run(
                element.dim.tag,
                element.dim.time,
                element.mvccId,
                JSON.stringify(element)
            );
        }

        database.exec("COMMIT");
        return databasePath;
    } catch (error) {
        database.exec("ROLLBACK");
        throw error;
    } finally {
        database.close();
    }
}

async function checkpointHandler(job: Job<IPageInfo>, token?: string, abortSignal?: AbortSignal): Promise<void> {

    const driver = (this.DIContainer as DisposableSingletonContainer).fetchInstance<RDriver>(DIConstants.RDriver);
    const timeToleranceInMs = (this.DIContainer as DisposableSingletonContainer).fetchInstance<number>(EnvironmentVariableConstants.TimeToleranceInMs);
    const tempCheckPointingPath = (this.DIContainer as DisposableSingletonContainer).fetchInstance<string>(EnvironmentVariableConstants.TempCheckPointingPath);
    const page = new RedisTsPage(job.data, driver, undefined, timeToleranceInMs * 2);
    const pageElements = await page.dumpPage();
    if (abortSignal?.aborted) {
        console.log(`Checkpoint for page ${job.data.pageKey} aborted stage:'Dump Redis' processing completed, reason: ${abortSignal.reason}.`);
        abortSignal.throwIfAborted();
        return;
    }

    dumpPageElementsToSqlite(job.data, job.data.pageKey, pageElements, tempCheckPointingPath);
    if (abortSignal?.aborted) {
        console.log(`Checkpoint for page ${job.data.pageKey} aborted stage:'Dump SQLite' processing completed, reason: ${abortSignal.reason}.`);
        abortSignal.throwIfAborted();
        return;
    }

    await page.purgePage();//Last thing to do if everything else is successful to ensure we don't lose data if process crashes midway
}

async function appStartUp(rootRouter: IRouter, DIContainer: DisposableSingletonContainer, applicationBuilder: ApplicationBuilder) {

    await initializeGridScale(DIContainer);

    setupRoutes(rootRouter);

    //Configure your application.
    applicationBuilder
        .overrideAppPort(8080)                                                                                                   //override the default port 8080(Default 3000)
        .overrideHealthPort(8081)                                                                                                //override the default health port 8081(Default 5678)
        .registerApplicationHandler(utilities.helmetMiddleware(), "*", 1, ApplicationTypes.Both)                                 //register helmet middleware for both application and health
        .registerApplicationHandler(utilities.bodyParserURLEncodingMiddleware(), "*", 2, ApplicationTypes.Main)                  //register body parser url middleware for application
        .registerApplicationHandler(utilities.bodyParserJSONEncodingMiddleware({ limit: '50mb' }), "*", 3, ApplicationTypes.Main) //register body parser json middleware for application
        //.registerApplicationHandler(apiDocsMiddleware.router, apiDocsMiddleware.hostingPath, 4, ApplicationTypes.Main)           //register api docs
        .registerApplicationHandler(utilities.injectInRequestMiddleware("DIProp", DIContainer), "*", 5, ApplicationTypes.Main)   //register DI container middleware
        .overrideCatchAllErrorResponseTransformer((req: Request, error: any) => ({                                                      //override the default catch all error response transformer
            path: req.path,
            status: 500,
            body: { message: error.message }
        }))


    return {
        status: ApplicationStartupStatus.UP,            // Indicates startup was successful
        data: { message: "Connected to database" }      // Additional data to be returned(Optional)
    };
}

app.overrideStartupHandler(appStartUp)
    .start()
    .then(() => console.log(`${applicationName} started successfully.`))
    .catch(console.error);