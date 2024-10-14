function* generatorA() {
    try {
        console.log('generatorA yield 1');
        yield [0, 1];
        console.log('generatorA yield 2');
        yield [10, 1];
        console.log('generatorA yield 3');
        yield [30, 1];

    }
    finally {
        console.log('generatorA is done');
    }
}

function* generatorB() {
    try {
        yield* yield* generatorA();
        console.log('generatorA yield 4');
        yield 4;
        yield 5;
        console.log('generatorB');
    }
    finally {
        console.log('generatorB is done');
    }
}

// Using the generator
const gen = generatorB();
console.log([...gen]); // Output: [1, 2, 3, 4, 5]

// const x = [generatorA()];
// console.log('Done');