#!/usr/bin/env node

/*
 * Minimal WebHDFS CLI for common operations.
 * Commands: ls, mkdir, touch, info, cat, write
 */

const BASE_URL = process.env.WEBHDFS_BASE_URL || "http://hdfs-name-node:9870";
const HDFS_USER = process.env.WEBHDFS_USER || "hadoop";

function printUsage() {
    console.log(`Usage:
    node webhdfs-cli.js ls <path>
    node webhdfs-cli.js mkdir <path>
    node webhdfs-cli.js touch <path>
    node webhdfs-cli.js info <path>
    node webhdfs-cli.js cat <path>
    node webhdfs-cli.js write <path> [content]

Environment variables:
  WEBHDFS_BASE_URL   default: http://hdfs-name-node:9870
  WEBHDFS_USER       default: hadoop
`);
}

function readStdinUtf8() {
    return new Promise((resolve, reject) => {
        let data = "";
        process.stdin.setEncoding("utf8");
        process.stdin.on("data", (chunk) => {
            data += chunk;
        });
        process.stdin.on("end", () => resolve(data));
        process.stdin.on("error", reject);
    });
}

function normalizePath(inputPath) {
    if (!inputPath) {
        throw new Error("Path is required.");
    }
    if (inputPath === "/") {
        return "/";
    }
    return inputPath.startsWith("/") ? inputPath : `/${inputPath}`;
}

function buildWebHdfsUrl(path, op, extraParams = {}) {
    const normalized = normalizePath(path);
    const encodedPath = normalized
        .split("/")
        .map((segment) => encodeURIComponent(segment))
        .join("/");

    const params = new URLSearchParams({
        op,
        "user.name": HDFS_USER,
        ...extraParams,
    });

    return `${BASE_URL}/webhdfs/v1${encodedPath}?${params.toString()}`;
}

async function parseJsonOrThrow(response, hint) {
    const bodyText = await response.text();
    let payload;

    try {
        payload = bodyText ? JSON.parse(bodyText) : {};
    } catch {
        payload = { raw: bodyText };
    }

    if (!response.ok) {
        const remoteMessage = payload?.RemoteException?.message || payload?.raw || "Unknown error";
        throw new Error(`${hint} failed (${response.status}): ${remoteMessage}`);
    }

    return payload;
}

async function webhdfsCreateFile(path, content = "", overwrite = false) {
    const initUrl = buildWebHdfsUrl(path, "CREATE", {
        overwrite: String(overwrite),
        createparent: "true",
    });

    const initResp = await fetch(initUrl, {
        method: "PUT",
        redirect: "manual",
    });

    if (initResp.status !== 307) {
        const payload = await parseJsonOrThrow(initResp, "CREATE init");
        throw new Error(`CREATE init expected redirect (307), got ${initResp.status}: ${JSON.stringify(payload)}`);
    }

    const location = initResp.headers.get("location");
    if (!location) {
        throw new Error("CREATE init did not return redirect location.");
    }

    const writeResp = await fetch(location, {
        method: "PUT",
        headers: {
            "Content-Type": "application/octet-stream",
            "Content-Length": String(Buffer.byteLength(content)),
        },
        body: content,
    });

    await parseJsonOrThrow(writeResp, "CREATE write");
}

async function webhdfsReadFile(path) {
    const initUrl = buildWebHdfsUrl(path, "OPEN");
    const initResp = await fetch(initUrl, {
        method: "GET",
        redirect: "manual",
    });

    if (initResp.status !== 307) {
        await parseJsonOrThrow(initResp, "OPEN init");
        throw new Error(`OPEN init expected redirect (307), got ${initResp.status}`);
    }

    const location = initResp.headers.get("location");
    if (!location) {
        throw new Error("OPEN init did not return redirect location.");
    }

    const readResp = await fetch(location, { method: "GET" });
    if (!readResp.ok) {
        const errBody = await readResp.text();
        throw new Error(`OPEN read failed (${readResp.status}): ${errBody || "Unknown error"}`);
    }

    return readResp.text();
}

async function cmdLs(path) {
    const url = buildWebHdfsUrl(path, "LISTSTATUS");
    const response = await fetch(url);
    const payload = await parseJsonOrThrow(response, "LISTSTATUS");

    const entries = payload?.FileStatuses?.FileStatus || [];

    if (entries.length === 0) {
        console.log("(empty)");
        return;
    }

    for (const item of entries) {
        const type = item.type === "DIRECTORY" ? "dir " : "file";
        const size = String(item.length).padStart(10, " ");
        const perms = item.permission;
        const owner = `${item.owner}:${item.group}`;
        console.log(`${type}  ${size}  ${perms}  ${owner}  ${item.pathSuffix}`);
    }
}

async function cmdMkdir(path) {
    const url = buildWebHdfsUrl(path, "MKDIRS");
    const response = await fetch(url, { method: "PUT" });
    const payload = await parseJsonOrThrow(response, "MKDIRS");

    if (!payload.boolean) {
        throw new Error(`MKDIRS returned false for path: ${path}`);
    }

    console.log(`Directory created: ${normalizePath(path)}`);
}

async function cmdTouch(path) {
    await webhdfsCreateFile(path);
    console.log(`File created: ${normalizePath(path)}`);
}

async function cmdCat(path) {
    const content = await webhdfsReadFile(path);
    process.stdout.write(content);
    if (!content.endsWith("\n")) {
        process.stdout.write("\n");
    }
}

async function cmdWrite(path, contentArg) {
    const content = contentArg !== undefined ? contentArg : await readStdinUtf8();
    await webhdfsCreateFile(path, content, true);
    console.log(`File written: ${normalizePath(path)} (${Buffer.byteLength(content)} bytes)`);
}

async function cmdInfo(path) {
    const url = buildWebHdfsUrl(path, "GETFILESTATUS");
    const response = await fetch(url);
    const payload = await parseJsonOrThrow(response, "GETFILESTATUS");
    console.log(JSON.stringify(payload?.FileStatus || {}, null, 2));
}

async function main() {
    const [command, path, ...rest] = process.argv.slice(2);

    if (!command || command === "--help" || command === "-h") {
        printUsage();
        process.exit(0);
    }

    try {
        switch (command) {
            case "ls":
                await cmdLs(path || "/");
                break;
            case "mkdir":
                await cmdMkdir(path);
                break;
            case "touch":
                await cmdTouch(path);
                break;
            case "info":
                await cmdInfo(path);
                break;
            case "cat":
                await cmdCat(path);
                break;
            case "write": {
                const contentArg = rest.length > 0 ? rest.join(" ") : undefined;
                await cmdWrite(path, contentArg);
                break;
            }
            default:
                throw new Error(`Unknown command: ${command}`);
        }
    } catch (error) {
        console.error(error.message || String(error));
        process.exit(1);
    }
}

main();
