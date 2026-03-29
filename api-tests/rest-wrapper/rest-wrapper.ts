import { ApplicationBuilder, ApplicationStartupStatus, ApplicationTypes, Convenience, DisposableSingletonContainer, EnvironmentVariables, IRouter, Request, Response } from "express-service-bootstrap";
import { IORedisClientPool, type IRedisClientPool } from "redis-abstraction";
import IORedis, { Cluster } from "ioredis";
import { JobsOptions, Queue } from 'bullmq';
import { parseURL } from "ioredis/built/utils/index.js";
import { BullMQOtel } from "bullmq-otel";

import { IPageInfo, RDriver, RedisCascadingBook } from "../../src/index.js";
import { RedisTsPage, TimeseriesSample } from "./redis-ts-page.js";
import { RKeyBuilder } from "../../src/utilities/r-key-builder.js";
import { DIConstants, EnvironmentVariableConstants, PageWindowDefaults } from "./constants.js";
import { type IFetchRequest } from "./interfaces.js";
import { ConvenienceMethods } from "../../src/utilities/convenience-methods.js";

interface IApiSample {
    tag: string;
    ts: number;
    pld: {
        nV: number;
        [key: string]: any;
    };
}

class DisposableQue implements AsyncDisposable {
    public readonly queue: Queue<IPageInfo>;

    constructor(queueName: string, options: any) {
        this.queue = new Queue<IPageInfo>(queueName, options);
    }

    public async [Symbol.asyncDispose](): Promise<void> {
        await this.queue.close();
    }
}

const applicationName = process.env.OTEL_SERVICE_NAME || "RestWrapper";
const app = new ApplicationBuilder(applicationName);
const utilities = new Convenience();
const defaultRedisConnectionString = "redis://localhost:6379";


async function initializeGridScale(DIContainer: DisposableSingletonContainer) {
    const env = DIContainer.createInstance<EnvironmentVariables>(DIConstants.EnvVars, EnvironmentVariables, []);
    const fallbackRedisConnectionString = env.getStringOrDefault(EnvironmentVariableConstants.RedisConnectionString, defaultRedisConnectionString);
    const redisMetaConnectionString = env.getStringOrDefault(EnvironmentVariableConstants.RedisMetaConnectionString, fallbackRedisConnectionString);
    const redisDataConnectionString = env.getStringOrDefault(EnvironmentVariableConstants.RedisDataConnectionString, redisMetaConnectionString);
    const timeToleranceInMs = parseInt(env.getStringOrDefault(EnvironmentVariableConstants.TimeToleranceInMs, PageWindowDefaults.timeToleranceInMs), 10);
    const timeWindowInMs = parseInt(env.getStringOrDefault(EnvironmentVariableConstants.TimeWindowInMs, PageWindowDefaults.timeWindowInMs), 10);
    const sizeWindowInBytes = parseInt(env.getStringOrDefault(EnvironmentVariableConstants.SizeWindowInBytes, PageWindowDefaults.sizeWindowInBytes), 10);
    const maxPagesInBook = parseInt(env.getStringOrDefault(EnvironmentVariableConstants.MaxPagesInBook, PageWindowDefaults.maxPagesInBook), 10);
    const parseRedisConnectionString = (connectionString: string) => parseURL(connectionString);
    const metaConnectionInjector = () => IORedisClientPool.IORedisClientClusterFactory([redisMetaConnectionString], IORedis as any, Cluster as any, parseRedisConnectionString);
    const dataConnectionInjector = () => IORedisClientPool.IORedisClientClusterFactory([redisDataConnectionString], IORedis as any, Cluster as any, parseRedisConnectionString);
    const redisPoolDriver = DIContainer.createInstance<IRedisClientPool>(DIConstants.RedisClientPool, IORedisClientPool, [metaConnectionInjector, 100]);
    const redisDriver = DIContainer.createInstance<RDriver>(DIConstants.RDriver, RDriver, [redisPoolDriver, timeToleranceInMs]);
    await redisDriver.initialize();
    const dataRedisPoolDriver = DIContainer.createInstance<IRedisClientPool>(DIConstants.DataRedisClientPool, IORedisClientPool, [dataConnectionInjector, 100]);
    const dataRedisDriver = DIContainer.createInstance<RDriver>(DIConstants.DataRDriver, RDriver, [dataRedisPoolDriver, timeToleranceInMs]);
    await dataRedisDriver.initialize();
    const queName = env.getStringOrDefault(EnvironmentVariableConstants.DistributionQueueName, "distribution_queue");
    const queConnectionParams = parseRedisConnectionString(redisMetaConnectionString);
    const checkpointQueue = DIContainer.createInstance<DisposableQue>(DIConstants.CheckpointQueue, DisposableQue, [queName, {
        connection: queConnectionParams,
        telemetry: new BullMQOtel({
            tracerName: applicationName,
            meterName: `${applicationName}-BULLMQ`,
            enableMetrics: true
        })
    }]);
    const turnOverCallback = async (newPageInfo: IPageInfo | undefined, trimmedPages: IPageInfo[]) => {
        const jobsToPublish = trimmedPages.map((pageInfo) => ({
            name: pageInfo.pageKey,
            data: pageInfo,
            opts: {
                lifo: false,
                jobId: Buffer.from(pageInfo.pageKey, "utf8").toString("base64url"), // no colon,
                removeOnComplete: true,
                //delay: 10000 // Adding a delay to ensure that the page turnover process is completed before the job is picked up by any worker. This is to avoid potential race conditions.
            } as JobsOptions
        }));
        await checkpointQueue.queue.addBulk(jobsToPublish);
        console.log(`Turnover callback executed. New page: ${newPageInfo?.pageKey ?? "none"}, Trimmed pages[${trimmedPages.length}]: ${trimmedPages.map(p => p.pageKey).join(", ")}`);
    };
    const keyBuilder = new RKeyBuilder();
    const pageFactory = async (pageInfo: IPageInfo, pageType: string): Promise<RedisTsPage> => {
        return new RedisTsPage(pageInfo, dataRedisDriver, keyBuilder);
    };
    DIContainer.createInstance<RedisCascadingBook>(DIConstants.RedisCascadingBook, RedisCascadingBook, [
        maxPagesInBook,
        sizeWindowInBytes,
        timeWindowInMs,
        "redis-ts-page",
        pageFactory,
        turnOverCallback,
        redisDriver,
        ConvenienceMethods.roughSizeEstimator,
        keyBuilder
    ]);
}

function setupRoutes(rootRouter: IRouter) {
    //Upsert Samples API
    rootRouter.put("/v1/series/upsert", async (req: Request, res: Response) => {
        try {
            const diagnostics = new Map<string, any>();
            let startTime = Date.now();
            diagnostics.set("timestamp", startTime);
            const DIContainer = req["DIProp"] as DisposableSingletonContainer;
            const book = DIContainer.fetchInstance<RedisCascadingBook>(DIConstants.RedisCascadingBook) as RedisCascadingBook;
            //TODO: Validate only certain number of samples to come be allowed 10 tags and 1000 samples per request, configure through env vars if needed.
            const samples = req.body as IApiSample[];
            const dimensionalElements = samples.map((sample) => new TimeseriesSample(sample.tag, sample.ts, sample.pld));
            diagnostics.set("numberOfSamples", dimensionalElements.length);
            await book.upsertElements(dimensionalElements);
            let endTime = Date.now();
            diagnostics.set("durationMs", endTime - startTime);
            res.status(201) //Created
                .json(Object.fromEntries(diagnostics.entries()));
        } catch (error) {
            console.error("Error in upsert API:", error);
            res.status(500) //Internal Server Error
                .json({ message: (error as Error).message })
        }
    });

    //Read Samples API
    rootRouter.post("/v1/series/fetch", async (req: Request, res: Response) => {
        try {
            const diagnostics = new Map<string, any>();
            let startTime = Date.now();
            diagnostics.set("timestamp", startTime);
            const DIContainer = req["DIProp"] as DisposableSingletonContainer;
            const book = DIContainer.fetchInstance<RedisCascadingBook>(DIConstants.RedisCascadingBook) as RedisCascadingBook;
            const samplesPerPage = 1000; //TODO: Make this configurable through env vars if needed.
            const fetchRequest = req.body as IFetchRequest;
            const elements = await book.queryByRank(fetchRequest.tagsFilter.in, fetchRequest.timeFilter.startInclusiveTime, fetchRequest.timeFilter.endExclusiveTime, samplesPerPage + 1) as TimeseriesSample[];
            let morePages = false;
            const groupedElements = new Map<string, TimeseriesSample[]>();
            for (const element of elements) {
                const current = groupedElements.get(element.dim.tag) ?? [];
                current.push(element);
                groupedElements.set(element.dim.tag, current);
            }

            const allSamples = new Array<IApiSample>();
            for (const [groupKey, grouped] of groupedElements) {
                morePages = grouped.length > samplesPerPage || morePages;
                const minTs = grouped.reduce((acc, value) => Math.min(acc, value.dim.time), Number.MAX_SAFE_INTEGER);
                const maxTs = grouped.reduce((acc, value) => Math.max(acc, value.dim.time), Number.MIN_SAFE_INTEGER);

                diagnostics.set(`info_count${groupKey}`, grouped.length);
                diagnostics.set(`info_min_ts${groupKey}`, minTs);
                diagnostics.set(`info_max_ts${groupKey}`, maxTs);

                allSamples.push(...grouped.map((element) => ({
                    tag: element.dim.tag,
                    ts: element.dim.time,
                    pld: element.pld
                })));
            }
            diagnostics.set("numberOfSamples", allSamples.length);
            let endTime = Date.now();
            diagnostics.set("durationMs", endTime - startTime);
            if (morePages === true) {
                res.status(206) //Partial Content
                    .json({ "samples": allSamples, "diagnostics": Object.fromEntries(diagnostics.entries()) });
            }
            else {
                res.status(200) //OK
                    .json({ "samples": allSamples, "diagnostics": Object.fromEntries(diagnostics.entries()) });
            }
        } catch (error) {
            console.error("Error in fetch API:", error);
            res.status(500) //Internal Server Error
                .json({ message: (error as Error).message })
        }
    });
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