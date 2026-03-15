
import { RWal } from './redis-wal/r-wal.js';
import { type IKeyBuilder, RKeyBuilder } from './redis-wal/r-key-builder.js';
import { type ISampleSet, type ISample } from './interfaces/i-sample.js';

//Implementation to export
export {
    RWal,
    RKeyBuilder as RedisKeyBuilder
};

//Types to export
export type {
    ISample,
    IKeyBuilder,
    ISampleSet,
};