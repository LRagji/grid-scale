export class IteratorCacheConfig {
    public updateCache: boolean = true
    public readCache: boolean = true;
    public resultCoolDownTime: number = 5 * 60 * 1000;//5 minutes
}