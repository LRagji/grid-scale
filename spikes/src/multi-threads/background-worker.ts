import { BootstrapConstructor } from "express-service-bootstrap";
import { GridThreadPlugin } from "./grid-thread-plugin.js";
import { isMainThread, parentPort } from "node:worker_threads";

const gridThreadPlugin = new GridThreadPlugin(!isMainThread, parentPort, new BootstrapConstructor());