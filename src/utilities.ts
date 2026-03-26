import { IDimensionalElement } from "./cascading-data-containers/interfaces/i-dimensional-element";

export class Utilities {

    private static readonly utf8Encoder = new TextEncoder();

    public static readonly u48Max = Number("0xFFFFFFFFFFFF"); // 48-bit max value for time header and counters.

    public static readonly u48In3 = Utilities.u48Max / 3; // Used for calculating time windows and tolerances to ensure we don't exceed redis sorted set score limits.


    public static modMinus(value: number, divisor: number): number {
        return value - (value % divisor);
    }

    public static roughSizeEstimator(samples: any[]): number {
        return Utilities.utf8Encoder.encode(JSON.stringify(samples)).byteLength;
    }

    public static hashElement(data: IDimensionalElement): string {
        //This is a placeholder hash function. In production, you would want to use a proper hashing library like crypto or a third-party library for better performance and collision resistance.
        let hash = 0;
        const dimensionString = Object.entries(data.dim)
            .sort()//TODO: This has to be stable sort else results will vary across runs.
            .map(([key, value]) => `${key}:${value}`).join("|");

        for (let i = 0; i < dimensionString.length; i++) {
            const char = dimensionString.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash |= 0; // Convert to 32bit integer
        }
        return hash.toString();
    }
}