import { StatefulProxyManager } from "node-apparatus";
import { GridScale } from "./grid-scale.js";
import { ChunkPlanner } from "./chunk-planner.js";
import { INonVolatileHashMap } from "./non-volatile-hash-map/i-non-volatile-hash-map";
import { fileURLToPath } from "node:url";
import { GridScaleConfig } from "./types/grid-scale-config.js";
import { IChunkMetadata } from "./chunk-metadata/i-chunk-metadata.js";

export class GridScaleFactory {

    public static async create(chunkRelations: INonVolatileHashMap, chunkFactoryPluginPath: URL, chunkMetaRegistry: IChunkMetadata, chunkCache: INonVolatileHashMap, config: GridScaleConfig = new GridScaleConfig()): Promise<GridScale> {
        const chunkPlanner = new ChunkPlanner(chunkRelations, config.TagBucketWidth, config.TimeBucketWidth, config.logicalChunkPrefix, config.logicalChunkSeparator, config.timeBucketTolerance, config.writerActiveShard, config.shardSets, chunkMetaRegistry);
        const workerFilePath = fileURLToPath(new URL("./grid-thread-plugin.js", import.meta.url));
        const proxies = new StatefulProxyManager(config.workerCount, workerFilePath);
        await proxies.initialize();
        for (let idx = 0; idx < proxies.WorkerCount; idx++) {
            await proxies.invokeMethod("initialize", [`${config.identity}-${idx}`, chunkFactoryPluginPath.toString()], idx);
        }

        const gs = new GridScale(chunkRelations, chunkPlanner, proxies, chunkFactoryPluginPath, chunkMetaRegistry, chunkCache);
        await gs.initialize();
        gs[Symbol.asyncDispose] = async () => {
            await proxies[Symbol.asyncDispose]();
        }
        return gs;
    }
}