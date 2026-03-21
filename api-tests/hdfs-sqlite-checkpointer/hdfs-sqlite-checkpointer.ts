import { ApplicationBuilder, ApplicationStartupStatus, ApplicationTypes, Convenience, DisposableSingletonContainer, EnvironmentVariables, IRouter, Request } from "express-service-bootstrap";
import { Job, Worker, WorkerOptions } from 'bullmq';
import { parseURL } from "ioredis/built/utils/index.js";
import { BullMQOtel } from "bullmq-otel";

import { IPageInfo } from "../../src/index.js";
import { DIConstants, EnvironmentVariableConstants } from "./constants.js";

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
    const redisConnectionString = env.getStringOrDefault(EnvironmentVariableConstants.RedisConnectionString, defaultRedisConnectionString);
    const parseRedisConnectionString = (connectionString: string) => parseURL(connectionString);
    const queName = env.getStringOrDefault(EnvironmentVariableConstants.DistributionQueueName, "distribution_queue");
    const queConnectionParams = parseRedisConnectionString(redisConnectionString);
    DIContainer.createInstance<DisposableWorker>(DIConstants.CheckpointQueue, DisposableWorker, [queName, checkpointHandler, {
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

async function checkpointHandler(job: Job<IPageInfo>, token?: string, abortSignal?: AbortSignal): Promise<void> {
    console.log(`Processing job for pageKey: ${job.data.pageKey}`);
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