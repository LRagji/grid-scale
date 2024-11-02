import { StatefulProxyManager } from "node-apparatus";
import { GridScale } from "./grid-scale.js";
import { ChunkPlanner } from "./chunk-planner.js";
import { INonVolatileHashMap } from "./non-volatile-hash-map/i-non-volatile-hash-map";
import { fileURLToPath } from "node:url";
import { GridScaleConfig } from "./grid-scale-config.js";

export class GridScaleFactory {

    public static async create(chunkRegistry: INonVolatileHashMap, stringToNumber: (string) => number[], config: GridScaleConfig = new GridScaleConfig()): Promise<GridScale> {
        const chunkPlanner = new ChunkPlanner(chunkRegistry, stringToNumber, config.tagBucketWidth, config.timeBucketWidth, config.logicalChunkPrefix, config.logicalChunkSeparator, config.timeBucketTolerance, config.writerActivePath, config.diskSets);
        const workerFilePath = fileURLToPath(new URL("./grid-thread-plugin.js", import.meta.url));
        const proxies = new StatefulProxyManager(config.workerCount, workerFilePath);
        await proxies.initialize();
        for (let idx = 0; idx < proxies.WorkerCount; idx++) {
            await proxies.invokeMethod("initialize", [`${config.identity}-${idx}`, config.fileNamePre, config.fileNamePost, config.maxCachedDB, 4, 0], idx);
        }
        const gs = new GridScale(chunkRegistry, chunkPlanner, proxies);

        gs[Symbol.asyncDispose] = async () => {
            await proxies[Symbol.asyncDispose]();
        }
        return gs;
    }
}