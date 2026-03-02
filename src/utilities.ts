
export class Utilities {

    public static readonly u63Max = BigInt("0x7FFFFFFFFFFFFFFF"); // 63-bit max value redis counter will only go upto this.


    public static modMinus(value: bigint, divisor: bigint): bigint {
        return value - (value % divisor);
    }

    public static roughSizeEstimator(samples: any[]): bigint {
        //This needs to be tweaked later based on actual encoding and Redis storage overhead, but this is a starting point for estimation.
        if (samples.length === 0) {
            return 2n;
        }

        let total = 2n + BigInt(samples.length - 1);
        for (const sample of samples) {
            total += 94n + (6n * BigInt(sample.tag.length));
        }

        return total;
    }
}