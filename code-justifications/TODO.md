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
1. Move all code paths outside of the redis acquire/release life cycle such that connections are not blocked.
2. Add OTEL support
3. Add K6 support.
4. Add multiple integration test files for partitioning on time, size and writes.
5. Add validations for maximum 48bit integer or uint. 
6. Performance nightmare with multiple for loops for read and writes.