import { IDimensionalElement } from "../interfaces/i-dimensional-element.js";
import {
    Condition,
    ConditionGroup,
    IDimensionalQuery,
    NumberCondition,
    StringCondition
} from "../interfaces/i-dimensional-query.js";

function isConditionGroup(condition: Condition | ConditionGroup): condition is ConditionGroup {
    return "conditions" in condition;
}

function evaluateStringCondition(fieldValue: string | number | undefined, condition: StringCondition): boolean {
    if (typeof fieldValue !== "string") {
        return false;
    }

    switch (condition.operator) {
        case "eq":
            return typeof condition.value === "string" && fieldValue === condition.value;
        case "noteq":
            return typeof condition.value === "string" && fieldValue !== condition.value;
        case "in":
            return Array.isArray(condition.value) && condition.value.includes(fieldValue);
        case "notin":
            return Array.isArray(condition.value) && !condition.value.includes(fieldValue);
        default:
            return false;
    }
}

function evaluateNumberCondition(fieldValue: string | number | undefined, condition: NumberCondition): boolean {
    if (typeof fieldValue !== "number") {
        return false;
    }

    switch (condition.operator) {
        case "eq":
            return typeof condition.value === "number" && fieldValue === condition.value;
        case "noteq":
            return typeof condition.value === "number" && fieldValue !== condition.value;
        case "lt":
            return typeof condition.value === "number" && fieldValue < condition.value;
        case "gt":
            return typeof condition.value === "number" && fieldValue > condition.value;
        case "between": {
            if (!Array.isArray(condition.value) || condition.value.length !== 2) {
                return false;
            }
            const [min, max] = condition.value;
            return fieldValue >= min && fieldValue <= max;
        }
        default:
            return false;
    }
}

function evaluateCondition(element: IDimensionalElement, condition: Condition): boolean {
    const fieldValue = element.dim[condition.dimension];

    if (["lt", "gt", "between"].includes(condition.operator)) {
        return evaluateNumberCondition(fieldValue, condition as NumberCondition);
    }

    if (["in", "notin"].includes(condition.operator)) {
        return evaluateStringCondition(fieldValue, condition as StringCondition);
    }

    if (typeof condition.value === "string" || Array.isArray(condition.value)) {
        return evaluateStringCondition(fieldValue, condition as StringCondition);
    }

    return evaluateNumberCondition(fieldValue, condition as NumberCondition);
}

function evaluateConditionGroup(element: IDimensionalElement, group: ConditionGroup): boolean {
    if (group.operator === "AND") {
        return group.conditions.every(condition => {
            return isConditionGroup(condition)
                ? evaluateConditionGroup(element, condition)
                : evaluateCondition(element, condition);
        });
    }

    return group.conditions.some(condition => {
        return isConditionGroup(condition)
            ? evaluateConditionGroup(element, condition)
            : evaluateCondition(element, condition);
    });
}

export function evaluateDimensionalQuery(element: IDimensionalElement, query: IDimensionalQuery): boolean {
    return evaluateConditionGroup(element, query.query);
}

export function filterByDimensionalQuery(
    elements: IDimensionalElement[],
    query: IDimensionalQuery,
    maxElementsCount: number = Number.POSITIVE_INFINITY
): IDimensionalElement[] {
    const result: IDimensionalElement[] = [];
    for (const element of elements) {
        if (evaluateDimensionalQuery(element, query)) {
            result.push(element);
            if (result.length >= maxElementsCount) {
                break;
            }
        }
    }
    return result;
}
