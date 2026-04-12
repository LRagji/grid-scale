import { IDimensionalElement } from "../interfaces/i-dimensional-element.js";

export class ConvenienceMethods {

    private static readonly utf8Encoder = new TextEncoder();

    public static readonly u48Max = Number("0xFFFFFFFFFFFF"); // 48-bit max value for time header and counters.

    public static readonly u48In3 = ConvenienceMethods.u48Max / 3; // Used for calculating time windows and tolerances to ensure we don't exceed redis sorted set score limits.


    public static modMinus(value: number, divisor: number): number {
        return value - (value % divisor);
    }

    public static roughSizeEstimator(samples: any[]): number {
        return ConvenienceMethods.utf8Encoder.encode(JSON.stringify(samples)).byteLength;
    }

    public static hashElement(data: IDimensionalElement, identityDimSet = new Set<string>(Object.keys(data.dim))): string {
        //This is a placeholder hash function. In production, you would want to use a proper hashing library like crypto or a third-party library for better performance and collision resistance.
        let hash = 0;
        const dimensionString = Object.entries(data.dim)
            .filter(([key]) => identityDimSet.has(key))
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