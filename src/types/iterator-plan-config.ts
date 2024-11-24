
export class IteratorPlanConfig {
    public queryId: string = "qId_" + Date.now().toString() + Math.random().toString();
    public affinityBasedPlan: boolean = false;
    public diagnostics = new Array<Map<string, any>>();
}