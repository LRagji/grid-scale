
export class IteratorPlanConfig {
    public queryId: string = "qId_" + Date.now().toString() + Math.random().toString();
    public affinityBasedPlan: boolean = false;
    public perThreadPageSize: number = 10000;
    public diagnostics = new Array<Map<string, any>>();
}