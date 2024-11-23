import { gzipSync } from "node:zlib";


export default function zipWindowData(page: any[][], acc: Map<string, number> = new Map<string, number>()): any[] {
    const compressedData = gzipSync(JSON.stringify(page));
    return Array.from(compressedData);
}