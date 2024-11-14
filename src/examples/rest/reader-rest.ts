import { ApplicationBuilder, ApplicationStartupStatus, ApplicationTypes, Convenience, DisposableSingletonContainer, IRouter, Request, Response } from "express-service-bootstrap";
import { GridScale } from "../../grid-scale.js";
import { RedisHashMap } from "../../non-volatile-hash-map/redis-hash-map.js";
import * as OpenApiDefinition from "./reader-swagger.json" with { type: "json" };
import { GridScaleConfig } from "../../grid-scale-config.js";
import { GridScaleFactory } from "../../grid-scale-factory.js";

const applicationName = "Reader REST API";
const app = new ApplicationBuilder(applicationName);
const utilities = new Convenience();
const threadCount = 10;
const redisConnectionString = "redis://localhost:6379";

async function initializeGridScale(DIContainer: DisposableSingletonContainer) {
    const config = DIContainer.createInstance<GridScaleConfig>("GSConfig", GridScaleConfig);
    config.workerCount = threadCount;
    const chunkRegistry = DIContainer.createInstance<RedisHashMap>("ChunkRegistry", RedisHashMap, [redisConnectionString]);
    await chunkRegistry.initialize();
    const gs = await GridScaleFactory.create(chunkRegistry, new URL("../chunk-factory-implementation/cached-chunk-factory.js", import.meta.url), config);
    DIContainer.registerInstance<GridScale>("GS", gs);
}

function setupRoutes(rootRouter: IRouter) {
    rootRouter.get("/data", async (req: Request, res: Response) => {
        const DIContainer = req["DIProp"] as DisposableSingletonContainer;
        const gridScale = DIContainer.fetchInstance<GridScale>("GS");
        let tags = typeof req.query.tags === "string" ? [req.query.tags] : req.query.tags;
        tags = tags || [];
        tags = tags.map((tag: string) => BigInt(tag));
        const startInclusiveTime = parseInt((req.query.startInclusiveTime as string || '0'), 10);
        const endExclusiveTime = parseInt((req.query.endExclusiveTime as string || '0'), 10);
        const queryId = req.query.queryId as string || `Q[${Date.now()}]`;
        const diagnostics = new Map<string, number | String>();
        diagnostics.set("queryId", queryId)
        diagnostics.set("workers", threadCount)
        const pageCursor = gridScale.iteratorByTimePage(tags, startInclusiveTime, endExclusiveTime, queryId, undefined, diagnostics);
        res.setHeader('Content-Type', 'application/json');
        res.write(`{ "data": [`);
        let first = true;
        for await (const page of pageCursor) {
            for (const item of page) {
                res.write((first === true ? "" : ",") + JSON.stringify(item));
                first = false;
            }
        }
        res.write(`], "diagnostics":`);
        res.write(JSON.stringify(Object.fromEntries(diagnostics.entries())));
        res.write(` }`);
        res.end();
    });
}

async function AppStartUp(rootRouter: IRouter, DIContainer: DisposableSingletonContainer, applicationBuilder: ApplicationBuilder) {
    await initializeGridScale(DIContainer);
    setupRoutes(rootRouter);

    //Configure your application.
    const apiDocsMiddleware = utilities.swaggerAPIDocs(OpenApiDefinition);
    applicationBuilder
        .overrideAppPort(8082)                                                                                                   //override the default port 8080(Default 3000)
        .overrideHealthPort(8083)                                                                                                //override the default health port 8081(Default 5678)
        .registerApplicationHandler(utilities.helmetMiddleware(), "*", 1, ApplicationTypes.Both)                                 //register helmet middleware for both application and health
        .registerApplicationHandler(utilities.bodyParserURLEncodingMiddleware(), "*", 2, ApplicationTypes.Main)                  //register body parser url middleware for application
        .registerApplicationHandler(utilities.bodyParserJSONEncodingMiddleware({ limit: '50M' }), "*", 3, ApplicationTypes.Main) //register body parser json middleware for application
        .registerApplicationHandler(apiDocsMiddleware.router, apiDocsMiddleware.hostingPath, 4, ApplicationTypes.Main)           //register api docs
        .registerApplicationHandler(utilities.injectInRequestMiddleware("DIProp", DIContainer), "*", 4, ApplicationTypes.Main)   //register DI container middleware
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