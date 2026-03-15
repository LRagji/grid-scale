
export class Utilities {

    public static readonly u63Max = BigInt("0x7FFFFFFFFFFFFFFF"); // 63-bit max value redis counter will only go upto this.

    public static readonly u48Max = Number("0xFFFFFFFFFFFF"); // 48-bit max value for time header and counters.

    public static readonly u48In3 = Utilities.u48Max / 3; // Used for calculating time windows and tolerances to ensure we don't exceed redis sorted set score limits.


    public static modMinus(value: number, divisor: number): number {
        return value - (value % divisor);
    }

    public static roughSizeEstimator(samples: any[]): number {
        //This needs to be tweaked later based on actual encoding and Redis storage overhead, but this is a starting point for estimation.
        if (samples.length === 0) {
            return 2;
        }

        let total = 2 + (samples.length - 1);
        for (const sample of samples) {
            const groupKey = sample?.gk ?? sample?.tag ?? "";
            total += 94 + (6 * String(groupKey).length);
        }

        return total;
    }
}