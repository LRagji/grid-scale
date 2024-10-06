import Database from "better-sqlite3";
import { readdirSync } from "fs";
import { join } from "path";



const dbFileNameRegex = new RegExp(`^w-[a-z0-9]+\\.db$`);
const directory = "/Users/105050656/Documents/git/grid-scale/data/scaled-set1/disk1/";
const matchingDatabases = readdirSync(directory, { recursive: true, withFileTypes: true })
    .filter(dirent => dbFileNameRegex.test(dirent.name) && dirent.isFile())
    .slice(0, 100)
    .map(dirent => join(dirent.path || "", dirent.name));

const resultPointers = new Array<IterableIterator<unknown>>();
for (const dbFile of matchingDatabases) {
    const db = new Database(dbFile, { readonly: true });
    resultPointers.push(db.prepare(`SELECT type,random() FROM sqlite_master WHERE type='table'`).iterate());
}

let frame = new Array();
let completed = false;
for (let index = 0; index <= resultPointers.length; index++) {
    if (index === resultPointers.length) {
        if (completed === true) {
            break;
        }
        else {
            //Emit results
            console.table(frame);
            index = -1;//cause this will increment after this iteration
            frame = new Array();
        }
    }
    else {
        const result = resultPointers[index].next();
        completed = index === 0 ? result.done : completed && result.done;
        if (result.done === true) {
            continue;
        }
        frame.push(result.value);
    }
}

