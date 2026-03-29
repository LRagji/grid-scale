
export interface IDimensionalElement {

    globalIdentityHash?: string;//This is a hash that can be used to identify the element across different versions and pages, it can be used for deduplication and conflict resolution and update cases.

    mvccId?: number;//Multi Version Concurrency Control Id, can be used to resolve conflicts between different versions of the same element, if needed.

    dim: {
        [dimensionName: string]: number | string;
    };

    pld: any;//Payload can be used to store actual data.
}