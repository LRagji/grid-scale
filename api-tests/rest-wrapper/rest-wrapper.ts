import { ApplicationBuilder, ApplicationStartupStatus, ApplicationTypes, Convenience, DisposableSingletonContainer, EnvironmentVariables, IRouter, Request, Response } from "express-service-bootstrap";
import { IORedisClientPool, type IRedisClientPool } from "redis-abstraction";
import IORedis, { Cluster } from "ioredis";
import { JobsOptions, Queue } from 'bullmq';
import { parseURL } from "ioredis/built/utils/index.js";
import { BullMQOtel } from "bullmq-otel";

import { IPageInfo, ISortedElement, RBook, RDriver, RWal } from "../../src/index.js";
import { DIConstants, EnvironmentVariableConstants, PageWindowDefaults } from "./constants.js";
import { type IFetchRequest } from "./interfaces.js";

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
    const redisConnectionString = env.getStringOrDefault(EnvironmentVariableConstants.RedisConnectionString, defaultRedisConnectionString);
    const timeToleranceInMs = parseInt(env.getStringOrDefault(EnvironmentVariableConstants.TimeToleranceInMs, PageWindowDefaults.timeToleranceInMs), 10);
    const timeWindowInMs = parseInt(env.getStringOrDefault(EnvironmentVariableConstants.TimeWindowInMs, PageWindowDefaults.timeWindowInMs), 10);
    const sizeWindowInBytes = parseInt(env.getStringOrDefault(EnvironmentVariableConstants.SizeWindowInBytes, PageWindowDefaults.sizeWindowInBytes), 10);
    const writeWindow = parseInt(env.getStringOrDefault(EnvironmentVariableConstants.WriteWindow, PageWindowDefaults.writeWindow), 10);
    const maxPagesInBook = parseInt(env.getStringOrDefault(EnvironmentVariableConstants.MaxPagesInBook, PageWindowDefaults.maxPagesInBook), 10);
    const parseRedisConnectionString = (connectionString: string) => parseURL(connectionString);
    const connectionInjector = () => IORedisClientPool.IORedisClientClusterFactory([redisConnectionString], IORedis as any, Cluster as any, parseRedisConnectionString);
    const redisPoolDriver = DIContainer.createInstance<IRedisClientPool>(DIConstants.RedisClientPool, IORedisClientPool, [connectionInjector]);
    const queName = env.getStringOrDefault(EnvironmentVariableConstants.DistributionQueueName, "distribution_queue");
    const queConnectionParams = parseRedisConnectionString(redisConnectionString);
    const checkpointQueue = DIContainer.createInstance<DisposableQue>(DIConstants.CheckpointQueue, DisposableQue, [queName, {
        connection: queConnectionParams,
        telemetry: new BullMQOtel({
            tracerName: applicationName,
            meterName: `${applicationName}-BULLMQ`,
            enableMetrics: true
        })
    }]);
    const turnOverCallback = async (newPageKey: string | undefined, trimmedPages: IPageInfo[]) => {
        const jobsToPublish = trimmedPages.map((pageInfo) => ({
            name: pageInfo.pageKey,
            data: pageInfo,
            opts: {
                lifo: false,
                jobId: Buffer.from(pageInfo.pageKey, "utf8").toString("base64url"), // no colon,
                removeOnComplete: true,
                delay: 10000 // Adding a delay to ensure that the page turnover process is completed before the job is picked up by any worker. This is to avoid potential race conditions.
            } as JobsOptions
        }));
        await checkpointQueue.queue.addBulk(jobsToPublish);
        console.log(`Turnover callback executed. New page: ${newPageKey}, Trimmed pages[${trimmedPages.length}]: ${trimmedPages.map(p => p.pageKey).join(", ")}`);
    };

    const redisDriver = new RDriver(redisPoolDriver, timeToleranceInMs);
    await redisDriver.initialize();
    const book = new RBook(redisDriver, timeWindowInMs, sizeWindowInBytes, writeWindow, maxPagesInBook, undefined, turnOverCallback);
    DIContainer.createInstance<RWal>(DIConstants.RWal, RWal, [book]);
}

function setupRoutes(rootRouter: IRouter) {
    //Upsert Samples API
    rootRouter.put("/v1/series/upsert", async (req: Request, res: Response) => {
        try {
            const diagnostics = new Map<string, any>();
            let startTime = Date.now();
            diagnostics.set("timestamp", startTime);
            const DIContainer = req["DIProp"] as DisposableSingletonContainer;
            const wal = DIContainer.fetchInstance<RWal>(DIConstants.RWal) as RWal;
            //TODO: Validate only certain number of samples to come be allowed 10 tags and 1000 samples per request, configure through env vars if needed.
            const samples = req.body as IApiSample[];
            const sortedElements = samples.map((sample) => ({
                gk: sample.tag,
                elementRank: sample.ts,
                sn: 0,
                pld: sample.pld
            } as ISortedElement));
            diagnostics.set("numberOfSamples", sortedElements.length);
            await wal.append(sortedElements);
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
            const wal = DIContainer.fetchInstance<RWal>(DIConstants.RWal) as RWal;
            const samplesPerPage = 1000; //TODO: Make this configurable through env vars if needed.
            const fetchRequest = req.body as IFetchRequest;
            const elements = await wal.queryByRank(fetchRequest.tagsFilter.in, fetchRequest.timeFilter.startInclusiveTime, fetchRequest.timeFilter.endExclusiveTime, samplesPerPage + 1);
            let morePages = false;
            const groupedElements = new Map<string, ISortedElement[]>();
            for (const element of elements) {
                const current = groupedElements.get(element.gk) ?? [];
                current.push(element);
                groupedElements.set(element.gk, current);
            }

            const allSamples = new Array<IApiSample>();
            for (const [groupKey, grouped] of groupedElements) {
                morePages = grouped.length > samplesPerPage || morePages;
                const minTs = grouped.reduce((acc, value) => Math.min(acc, value.elementRank), Number.MAX_SAFE_INTEGER);
                const maxTs = grouped.reduce((acc, value) => Math.max(acc, value.elementRank), Number.MIN_SAFE_INTEGER);

                diagnostics.set(`info_count${groupKey}`, grouped.length);
                diagnostics.set(`info_min_ts${groupKey}`, minTs);
                diagnostics.set(`info_max_ts${groupKey}`, maxTs);

                allSamples.push(...grouped.map((element) => ({
                    tag: element.gk,
                    ts: element.elementRank,
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