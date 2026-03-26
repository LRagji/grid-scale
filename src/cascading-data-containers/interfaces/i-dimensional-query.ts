// Allowed operators for string dimensions
type StringOperator = "eq" | "noteq" | "in" | "notin";

// Allowed operators for number dimensions
type NumberOperator = "eq" | "noteq" | "lt" | "gt" | "between";

// Logical operators for combining conditions
type LogicalOperator = "AND" | "OR";

// String condition
interface StringCondition {
    dimension: string;
    operator: StringOperator;
    value: string | string[];
}

// Number condition
interface NumberCondition {
    dimension: string;
    operator: NumberOperator;
    value: number | [number, number];
}

// A condition can be either string or number
type Condition = StringCondition | NumberCondition;

// Group of conditions combined with AND/OR
interface ConditionGroup {
    operator: LogicalOperator;
    conditions: Array<Condition | ConditionGroup>;
}

export interface IDimensionalQuery {
    query: ConditionGroup;
}

const sampleQuery: ConditionGroup = {
    operator: "AND",
    conditions: [
        {
            dimension: "country",
            operator: "in",
            value: ["India", "USA"]
        },
        {
            operator: "OR",
            conditions: [
                {
                    dimension: "status",
                    operator: "eq",
                    value: "active"
                },
                {
                    dimension: "age",
                    operator: "between",
                    value: [25, 40]
                }
            ]
        },
        {
            dimension: "score",
            operator: "gt",
            value: 80
        }
    ]
};
