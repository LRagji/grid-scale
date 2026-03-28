
export interface IDimensionalElement {
    dim: {
        [dimensionName: string]: number | string;
    };

    identityDim?: string[]; // Optional list of dimension names that should be used for hashing and deduplication. If not provided, all dimensions will be used.

    pld: any;//Payload can be used to store actual data.
}