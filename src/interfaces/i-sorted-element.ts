export interface ISortedElement {
    elementRank: number;
    sn: number;
    gk: string;
    pld: {
        nV: number //Mandatory
        [key: string]: any
    };
}