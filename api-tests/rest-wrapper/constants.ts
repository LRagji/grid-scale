
export class DIConstants {
    public static readonly EnvVars = "EnvVars";
    public static readonly RedisClientPool = "RedisClientPool";
    public static readonly RedisCascadingBook = "RedisCascadingBook";
    public static readonly CheckpointQueue = "CheckpointQueue";
    public static readonly RDriver = "RDriver";
}

export class EnvironmentVariableConstants {
    public static readonly RedisConnectionString = "REDIS_CONNECTION_STRING";
    public static readonly TimeToleranceInMs = "TIME_TOLERANCE_IN_MS";
    public static readonly TimeWindowInMs = "TIME_WINDOW_IN_MS";
    public static readonly SizeWindowInBytes = "SIZE_WINDOW_IN_BYTES";
    public static readonly MaxPagesInBook = "MAX_PAGES_IN_BOOK";
    public static readonly DistributionQueueName = "DISTRIBUTION_QUEUE_NAME";
}

export class PageWindowDefaults {
    public static readonly timeToleranceInMs = (1 * 60 * 1000).toString() // 1 minute
    public static readonly timeWindowInMs = (24 * 60 * 60 * 1000).toString() // 24 hours
    public static readonly sizeWindowInBytes = (300 * 1024 * 1024).toString() // 300 MB
    public static readonly maxPagesInBook = "100" // 100 pages
}