export interface IDimensionalElement {
    dim: {
        //Dimensions can be used for filtering grouping and other operations
        [dimensionName: string]: number | string;
    }
    pld: any;//Payload can be used to store actual data.
}