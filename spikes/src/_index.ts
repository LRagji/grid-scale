

import { isMainThread, parentPort } from 'worker_threads';
import { Worker } from 'worker_threads';
import { fileURLToPath } from 'node:url';
import { dirname, join } from "node:path";
import { IThreadCommunication } from './long-running-thread';

if (isMainThread) {
    // Path to the worker file

    const currentFilePath = join(dirname(fileURLToPath(import.meta.url)), 'long-running-thread.js');
    // Create a new worker
    const worker = new Worker(currentFilePath);

    // Listen for messages from the worker
    worker.on('message', (message) => {
        console.log('Received from worker:', message);
    });

    // Send a message to the worker
    // worker.postMessage('Hello, worker!');

    // Example of sending multiple messages
    setTimeout(() => {
        const payload = {
            header: 'Execute',
            payload: 'Some payload'
        } as IThreadCommunication;
        worker.postMessage(JSON.stringify(payload));
    }, 2000);

    setTimeout(() => {
        const payload = {
            header: 'Execute',
            payload: 'Some payload2'
        } as IThreadCommunication;
        worker.postMessage(JSON.stringify(payload));
    }, 4000);

    setTimeout(() => {
        const payload = {
            header: "Shutdown",
            payload: 'Some payload2'
        } as IThreadCommunication;
        worker.postMessage(JSON.stringify(payload));
    }, 3000);
}
else {
    // if (!parentPort) {
    //     throw new Error('This script should be run as a worker thread.');
    // }

    // // Function to handle the received input and return the result
    // function handleInput(input: any): any {
    //     // Simulate some processing
    //     const result = `Processed: ${input}`;
    //     return result;
    // }

    // // Listen for messages from the main thread
    // parentPort.on('message', (message) => {
    //     const result = handleInput(message);
    //     parentPort.postMessage(result);
    // });
}   