import { gzipSync } from "node:zlib";


export default function zipWindowData(page: any[][]): any[] {
    const compressedData = gzipSync(JSON.stringify(page));
    return Array.from(compressedData);
}