import { ApplicationBuilder, ApplicationStartupStatus, ApplicationTypes, Convenience, DisposableSingletonContainer, EnvironmentVariables, IRouter, Request, Response } from "express-service-bootstrap";
import { IORedisClientPool, type IRedisClientPool } from "redis-abstraction";
import Redis, { Cluster } from "ioredis";
import { parseURL } from "ioredis/built/utils/index.js";

import { type ISample, RedisWAL } from "../../src/index.js";
import { DIConstants, EnvironmentVariableConstants, PageWindowDefaults } from "./constants.js";
import { type IFetchRequest } from "./interfaces.js";

const applicationName = "RestWrapper";
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
    const connectionInjector = () => IORedisClientPool.IORedisClientClusterFactory([redisConnectionString], Redis as any, Cluster as any, parseRedisConnectionString);
    const redisPoolDriver = DIContainer.createInstance<IRedisClientPool>(DIConstants.RedisClientPool, IORedisClientPool, [connectionInjector]);
    const turnOverCallback = (newPageKey: string) => console.log(`Fresh page ${newPageKey} started.`);
    const redisWalInvokeArguments = [redisPoolDriver, timeToleranceInMs, timeWindowInMs, sizeWindowInBytes, writeWindow, undefined, undefined, maxPagesInBook, turnOverCallback];
    const redisWal = DIContainer.createInstance<RedisWAL>(DIConstants.RedisWAL, RedisWAL, redisWalInvokeArguments);
    await redisWal.initialize();
}

function setupRoutes(rootRouter: IRouter) {
    //Upsert Samples API
    rootRouter.put("/v1/series/upsert", async (req: Request, res: Response) => {
        try {
            const diagnostics = new Map<string, any>();
            let startTime = Date.now();
            diagnostics.set("timestamp", startTime);
            const DIContainer = req["DIProp"] as DisposableSingletonContainer;
            const redisWal = DIContainer.fetchInstance<RedisWAL>(DIConstants.RedisWAL) as RedisWAL;
            //TODO: Validate only certain number of samples to come be allowed 10 tags and 1000 samples per request, configure through env vars if needed.
            const samples = req.body as ISample[];
            diagnostics.set("numberOfSamples", samples.length);
            await redisWal.upsertBulkSamples(samples);
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
            const redisWal = DIContainer.fetchInstance<RedisWAL>(DIConstants.RedisWAL) as RedisWAL;
            const samplesPerPage = 1000; //TODO: Make this configurable through env vars if needed.
            const fetchRequest = req.body as IFetchRequest;
            const sampleSets = await redisWal.queryRange(fetchRequest.tagsFilter.in, fetchRequest.timeFilter.startInclusiveTime, fetchRequest.timeFilter.endExclusiveTime, samplesPerPage + 1);
            let morePages = false;
            const allSamples = new Array<ISample>();
            for (const set of sampleSets) {
                morePages = set.count > samplesPerPage || morePages;
                allSamples.push(...set.samples);
                diagnostics.set(`info_count${set.tag}`, set.count);
                diagnostics.set(`info_min_ts${set.tag}`, set.minTs);
                diagnostics.set(`info_max_ts${set.tag}`, set.maxTs);
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