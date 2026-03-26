// Allowed operators for string dimensions
export type StringOperator = "eq" | "noteq" | "in" | "notin";

// Allowed operators for number dimensions
export type NumberOperator = "eq" | "noteq" | "lt" | "gt" | "between";

// Logical operators for combining conditions
export type LogicalOperator = "AND" | "OR";

// String condition
export interface StringCondition {
    dimension: string;
    operator: StringOperator;
    value: string | string[];
}

// Number condition
export interface NumberCondition {
    dimension: string;
    operator: NumberOperator;
    value: number | [number, number];
}

// A condition can be either string or number
export type Condition = StringCondition | NumberCondition;

// Group of conditions combined with AND/OR
export interface ConditionGroup {
    operator: LogicalOperator;
    conditions: Array<Condition | ConditionGroup>;
}

export interface IDimensionalQuery {
    query: ConditionGroup;
}
