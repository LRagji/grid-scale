export type DirectionalStepCallback = (timeSteps: [number, number][], tagsSteps: bigint[][], previousTimeStep: [number, number] | undefined, previousTagStep: bigint[] | undefined) => { nextTimeStep?: [number, number], nextTagStep?: bigint[] }
export class IteratorPaginationConfig {

    public nextStepCallback: DirectionalStepCallback = this.singleTimeWiseStepDirector;
    private timeStepSize = 2 ** 16;
    private tagStepSize = 2 ** 6;

    public set TimeStepSize(timeStepSize: number) {
        // Check if timeStepSize is a power of 2 and not less than 1 else it will become infinite loop
        if (timeStepSize <= 1 || (timeStepSize & (timeStepSize - 1)) !== 0) {
            throw new Error("'timeStepSize' must be a power of 2 and greater than 0");
        }
        this.timeStepSize = timeStepSize;
    }

    public get TimeStepSize(): number {
        return this.timeStepSize;
    }

    public set TagStepSize(tagStepSize: number) {
        // Check if tagStepSize is a power of 2 and not less than 1 else it will become infinite loop
        if (tagStepSize <= 1 || (tagStepSize & (tagStepSize - 1)) !== 0) {
            throw new Error("'tagStepSize' must be a power of 2 and greater than 0");
        }
        this.tagStepSize = tagStepSize;
    }

    public get TagStepSize(): number {
        return this.tagStepSize;
    }

    private singleTimeWiseStepDirector(timePages: [number, number][], tagPages: bigint[][], previousTimeStep: [number, number] | undefined, previousTagStep: bigint[] | undefined) {
        let timeStepIndex = timePages.indexOf(previousTimeStep);
        let tagStepIndex = tagPages.indexOf(previousTagStep);
        if (timeStepIndex === -1 || tagStepIndex === -1) {
            return { nextTimeStep: timePages[0], nextTagStep: tagPages[0] };
        }
        timeStepIndex++;
        if (timeStepIndex >= timePages.length) {
            timeStepIndex = 0;
            tagStepIndex++;
            if (tagStepIndex >= tagPages.length) {
                return { nextTimeStep: undefined, nextTagStep: undefined };
            }
        }
        return { nextTimeStep: timePages[timeStepIndex], nextTagStep: tagPages[tagStepIndex] };
    }
}

