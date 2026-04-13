import { IMetadata, IPolicy, IPolicyEvaluator } from "../interfaces/i-q-acc.js";

/**
 * Evaluates registered policies and invokes the action callback whenever one or
 * more policies are satisfied.
 */
export class QPolicyEvaluator implements IPolicyEvaluator<(policyMeta: string[]) => Promise<void>, IMetadata> {

    private readonly policies: IPolicy[] = [];
    private actionCallback: (policyMeta: string[]) => Promise<void> = async () => { };

    public initialize(actionCallback: (policyMeta: string[]) => Promise<void>): void {
        this.actionCallback = actionCallback;
    }

    public registerPolicy(policy: IPolicy): void {
        this.policies.push(policy);
    }

    public async evaluatePolicies(meta: IMetadata): Promise<string[]> {
        const matchedPolicies: string[] = [];

        for (const policy of this.policies) {
            const isPolicySatisfied = await policy.evaluate(meta);
            if (isPolicySatisfied) {
                matchedPolicies.push(policy.name);
            }
        }

        if (matchedPolicies.length > 0) {
            await this.actionCallback(matchedPolicies);
        }

        return matchedPolicies;
    }
}