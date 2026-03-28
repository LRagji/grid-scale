
export interface IDimensionalElement {

    mvccId?: number;//Multi Version Concurrency Control Id, can be used to resolve conflicts between different versions of the same element, if needed.

    dim: {
        [dimensionName: string]: number | string;
    };

    pld: any;//Payload can be used to store actual data.
}