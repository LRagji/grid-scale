import { ApplicationBuilder, ApplicationStartupStatus, ApplicationTypes, Convenience, DisposableSingletonContainer, IRouter, Request, Response } from "express-service-bootstrap";
import { ChunkPlanner } from "../../chunk-planner.js";
import { GridScale } from "../../grid-scale.js";
import { RedisHashMap } from "../../non-volatile-hash-map/redis-hash-map.js";
import { TConfig } from "../t-config.js";
import { CommonConfig } from "../utils.js";
import { StatefulProxyManager } from "node-apparatus";
import { fileURLToPath } from "node:url";
import { StringToNumberAlgos } from "../../string-to-number-algos.js";
//import * as OpenApiDefinition from "./reader-swagger.json" with { type: "json" };

const applicationName = "Writer REST API";
const app = new ApplicationBuilder(applicationName);
const utilities = new Convenience();
const threadCount = 10;

async function initializeGridScale(DIContainer: DisposableSingletonContainer) {
    const config = DIContainer.createInstanceWithoutConstructor<TConfig>("TConfig", CommonConfig);
    const chunkRegistry = DIContainer.createInstance<RedisHashMap>("ChunkRegistry", RedisHashMap, [config.redisConnection]);
    await chunkRegistry.initialize();
    const chunkPlanner = DIContainer.createInstance<ChunkPlanner>("ChunkPlanner", ChunkPlanner, [chunkRegistry, StringToNumberAlgos[config.activeCalculatorIndex], config.tagBucketWidth, config.timeBucketWidth, config.logicalChunkPrefix, config.logicalChunkSeparator, config.timeBucketTolerance, config.activePath, config.setPaths]);
    const workerFilePath = fileURLToPath(new URL("../../grid-thread-plugin.js", import.meta.url));
    const proxies = DIContainer.createInstance<StatefulProxyManager>("ProxyManager", StatefulProxyManager, [threadCount, workerFilePath]);
    await proxies.initialize();
    for (let idx = 0; idx < proxies.WorkerCount; idx++) {
        await proxies.invokeMethod("initialize", [`${process.pid.toString()}-${idx}`, config.fileNamePre, config.fileNamePost, config.maxDBOpen, 4, 0], idx);
    }
    DIContainer.createInstance<GridScale>("GS", GridScale, [chunkRegistry, chunkPlanner, proxies]);
}

function setupRoutes(rootRouter: IRouter) {
    rootRouter.post("/data", async (req: Request, res: Response) => {
        const DIContainer = req["DIProp"] as DisposableSingletonContainer;
        const gridScale = DIContainer.fetchInstance<GridScale>("GS");
        const data = new Map<string, any[]>(Object.entries(req.body));
        const diagnostics = new Map<string, number>();
        await gridScale.store(data, 4, 0, 1, undefined, diagnostics);
        res.json(Object.fromEntries(diagnostics.entries()));
    });
}

async function AppStartUp(rootRouter: IRouter, DIContainer: DisposableSingletonContainer, applicationBuilder: ApplicationBuilder) {

    await initializeGridScale(DIContainer);

    setupRoutes(rootRouter);

    //Configure your application.
    //const apiDocsMiddleware = utilities.swaggerAPIDocs(OpenApiDefinition);
    applicationBuilder
        .overrideAppPort(8080)                                                                                                   //override the default port 8080(Default 3000)
        .overrideHealthPort(8081)                                                                                                //override the default health port 8081(Default 5678)
        .registerApplicationHandler(utilities.helmetMiddleware(), "*", 1, ApplicationTypes.Both)                                 //register helmet middleware for both application and health
        .registerApplicationHandler(utilities.bodyParserURLEncodingMiddleware(), "*", 2, ApplicationTypes.Main)                  //register body parser url middleware for application
        .registerApplicationHandler(utilities.bodyParserJSONEncodingMiddleware({ limit: '50mb' }), "*", 3, ApplicationTypes.Main) //register body parser json middleware for application
        //.registerApplicationHandler(apiDocsMiddleware.router, apiDocsMiddleware.hostingPath, 4, ApplicationTypes.Main)           //register api docs
        .registerApplicationHandler(utilities.injectInRequestMiddleware("DIProp", DIContainer), "*", 5, ApplicationTypes.Main)   //register DI container middleware
        .overrideCatchAllErrorResponseTransformer((req, error: Error) => ({                                                      //override the default catch all error response transformer
            path: req.path,
            status: 500,
            body: { message: error.message }
        }))


    return {
        status: ApplicationStartupStatus.UP,            // Indicates startup was successful
        data: { message: "Connected to database" }      // Additional data to be returned(Optional)
    };
}

app.overrideStartupHandler(AppStartUp)
    .start()
    .then(() => console.log(`${applicationName} started successfully with ${threadCount} workers.`))
    .catch(console.error);