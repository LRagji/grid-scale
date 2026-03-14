export interface IRDriver {

    timeToleranceInMs: number;

    initialize(): Promise<void>

    usingRedisDriver<T>(commands: any[][], tokenName: string, type: "run" | "pipeline"): Promise<T>
}

export class RedisKeywords {
    static BITFIELD = "bitfield";
    static INCRBY = "incrby";
    static OVERFLOW = "overflow";
    static FAIL = "fail";
    static PEXPIRE = "pexpire";
    static ZADD = "zadd";
    static ZREMRANGEBYRANK = "zremrangebyrank";
    static TIME = "time";
    static ZRANGE = "zrange";
    static BYSCORE = "byscore";
    static LIMIT = "limit";
    static WITHSCORES = "withscores";
    static SET_ONLY_IF_NO_EXPIRY = "nx";
    static SET = "set";
    static GET = "get";
}
