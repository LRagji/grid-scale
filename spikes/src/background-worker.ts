import { InjectableConstructor } from "node-apparatus";
import { GridThreadPlugin } from "./grid-thread-plugin.js";
import { isMainThread, parentPort } from "node:worker_threads";

export default new GridThreadPlugin(!isMainThread, parentPort, new InjectableConstructor());