
export class DIConstants {
    public static readonly EnvVars = "EnvVars";
    public static readonly RedisClientPool = "RedisClientPool";
    public static readonly CheckpointQueue = "CheckpointQueue";
    public static readonly RDriver = "RDriver";
}

export class EnvironmentVariableConstants {
    public static readonly RedisConnectionString = "REDIS_CONNECTION_STRING";
    public static readonly DistributionQueueName = "DISTRIBUTION_QUEUE_NAME";
    public static readonly TimeToleranceInMs = "TIME_TOLERANCE_IN_MS";
    public static readonly TempCheckPointingPath = "TEMP_CHECKPOINTING_PATH";
}

export class PageWindowDefaults {
    public static readonly timeToleranceInMs = (1 * 60 * 1000).toString() // 1 minute
}