1. We need limits on following(Redis WAL) /per call and should be configurable 
    - Maximum number of samples to write 1000
    - Maximum number of samples to read per tag 1000
    - Maximum number of tags to read in one call 10
    - Maximum number of active redis connection per instance 6.
    - Maximum JSON payload in Size bytes.

Use `async-sema` package if required to restrict, the reason to restrict is we can have definite limits of the load of the service to have a flat line for p90.
```typescript 

import Fastify from 'fastify';
import IORedis from 'ioredis';
import { Sema } from 'async-sema';

const fastify = Fastify({ logger: true });
const redis = new IORedis(process.env.REDIS_URL);

// Limit concurrent Redis ops to avoid server-side queuing
const REDIS_CONCURRENCY = parseInt(process.env.REDIS_CONCURRENCY || '32', 10);
const redisGate = new Sema(REDIS_CONCURRENCY);

fastify.post('/write', async (req, reply) => {
  // Admission control for the whole request if you want:
  if (redisGate.nrWaiting() > 2 * REDIS_CONCURRENCY) { // backpressure
    return reply.code(429).send({ error: 'too_many_requests' });
  }

  await redisGate.acquire();
  try {
    const { key, value } = req.body;
    // Single-key write -> good for cluster sharding
    await redis.set(key, value); // or pipeline if batching makes sense
    return { ok: true };
  } finally {
    redisGate.release();
  }
});

fastify.listen({ port: process.env.PORT || 3000, host: '0.0.0.0' });
```
3. Add K6 support.
4. Add multiple integration test files for partitioning on time, size and writes.
5. Add validations for maximum 48bit integer or uint. 
6. Performance nightmare with multiple for loops for read and writes.

2 big problems to think of 
1. If the page turns within the tolerance time then the book simply has 2 different pages but with same score which needs to be ranked while reading to understand which page takes precedent.
2. Within a page when we have 2 samples of the same sample time we need to understand how do we rank them.
3. While reading the enforced limit of 1000 samples per page per tag doens't work if we have multiple pages thus throwing off the data to be returned more.