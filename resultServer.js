'use strict';

/**
 * resultServer.js: High-throughput exam result microservice
 *
 * Stack : Fastify + ioredis + pg + @fastify/rate-limit
 * Flow  : L1 (in-process, 5s) -> L2 (Redis) -> PostgreSQL (single-flight, load-shed)
 *
 * Install:
 *   npm i fastify @fastify/rate-limit ioredis pg
 *
 * Run:
 *   NODE_ENV=production CLUSTER=1 node resultServer.js
 *
 * Expected PostgreSQL schema (adapt column names to your own):
 *
 *   CREATE TABLE results (
 *     exam_code    TEXT     NOT NULL,        -- e.g. 'ALIM'
 *     exam_year    SMALLINT NOT NULL,
 *     roll_no      TEXT     NOT NULL,
 *     reg_no       TEXT     NOT NULL,
 *     student_name TEXT     NOT NULL,
 *     father_name  TEXT,
 *     institution  TEXT,
 *     group_name   TEXT,
 *     gpa          NUMERIC(3,2),
 *     grade        TEXT,
 *     status       TEXT     NOT NULL,        -- PASSED / FAILED / WITHHELD
 *     subjects     JSONB    NOT NULL DEFAULT '[]',
 *     PRIMARY KEY (exam_code, exam_year, roll_no, reg_no)
 *   );
 */

const cluster = require('node:cluster');
const os = require('node:os');

// ----------------------------------------------------------------------------
// Configuration
// ----------------------------------------------------------------------------
const cfg = {
  port: Number(process.env.PORT || 3000),
  host: process.env.HOST || '0.0.0.0',
  cluster: process.env.CLUSTER === '1',
  workers: Number(process.env.WORKERS || os.availableParallelism?.() || os.cpus().length),

  redisUrl: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
  pgUrl: process.env.DATABASE_URL || 'postgres://postgres:postgres@127.0.0.1:5432/results',
  pgPoolMax: Number(process.env.PG_POOL_MAX || 20),          // per worker
  pgMaxConcurrentQueries: Number(process.env.PG_MAX_INFLIGHT || 40), // load-shedding threshold

  cacheTtlSec: Number(process.env.CACHE_TTL || 6 * 60 * 60), // 6h (results are immutable once published)
  negativeTtlSec: Number(process.env.NEG_CACHE_TTL || 60),   // cache "not found" briefly
  l1TtlMs: 5_000,
  l1MaxEntries: 50_000,

  rateLimitGlobalMax: Number(process.env.RL_GLOBAL_MAX || 300),   // req / minute / IP (all routes)
  rateLimitResultMax: Number(process.env.RL_RESULT_MAX || 30),    // req / minute / IP (result route)
  rateLimitBanAfter: Number(process.env.RL_BAN_AFTER || 10),      // 403 after N violations in window
};

// ----------------------------------------------------------------------------
// Cluster primary: fork one worker per core, respawn on crash
// ----------------------------------------------------------------------------
if (cfg.cluster && cluster.isPrimary) {
  console.log(`[primary ${process.pid}] starting ${cfg.workers} workers`);
  for (let i = 0; i < cfg.workers; i++) cluster.fork();
  cluster.on('exit', (worker, code, signal) => {
    console.error(`[primary] worker ${worker.process.pid} died (${signal || code}), respawning`);
    cluster.fork();
  });
  return; // primary does nothing else
}

// ----------------------------------------------------------------------------
// Worker
// ----------------------------------------------------------------------------
const Fastify = require('fastify');
const rateLimit = require('@fastify/rate-limit');
const Redis = require('ioredis');
const { Pool } = require('pg');

// Redis for data cache: fail fast, never queue commands while down
const redis = new Redis(cfg.redisUrl, {
  enableOfflineQueue: false,
  maxRetriesPerRequest: 1,
  connectTimeout: 1_000,
  commandTimeout: 150, // ms; slow Redis must not stall requests
  retryStrategy: (times) => Math.min(times * 100, 2_000),
});
// Separate connection for the rate limiter (as recommended by @fastify/rate-limit)
const redisRL = new Redis(cfg.redisUrl, {
  enableOfflineQueue: false,
  connectTimeout: 500,
  maxRetriesPerRequest: 1,
});
redis.on('error', () => {}); // errors are handled per call; avoid unhandled events
redisRL.on('error', () => {});

const pool = new Pool({
  connectionString: cfg.pgUrl,
  max: cfg.pgPoolMax,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 1_500,
  statement_timeout: 2_000,
  query_timeout: 2_500,
});
pool.on('error', (err) => console.error('[pg] idle client error', err.message));

// ----------------------------------------------------------------------------
// L1 in-process cache (tiny TTL) absorbs bursts of identical lookups
// ----------------------------------------------------------------------------
const l1 = new Map();
function l1Get(key) {
  const e = l1.get(key);
  if (!e) return undefined;
  if (e.exp < Date.now()) { l1.delete(key); return undefined; }
  return e.val;
}
function l1Set(key, val) {
  if (l1.size >= cfg.l1MaxEntries) l1.delete(l1.keys().next().value); // evict oldest
  l1.set(key, { val, exp: Date.now() + cfg.l1TtlMs });
}

// ----------------------------------------------------------------------------
// Data access
// ----------------------------------------------------------------------------
const NOT_FOUND = '0'; // sentinel; real payloads are JSON objects and start with '{'

const SQL_FIND_RESULT = {
  name: 'find_result', // prepared statement: parsed once per connection
  text: `SELECT student_name, father_name, institution, group_name,
                gpa, grade, status, subjects
           FROM results
          WHERE exam_code = $1 AND exam_year = $2 AND roll_no = $3 AND reg_no = $4`,
};

class OverloadedError extends Error {}

const inflight = new Map(); // key -> Promise  (single-flight / stampede protection)
let dbInflight = 0;

function ttlWithJitter(base) {
  return base + Math.floor(Math.random() * base * 0.1); // +0-10% so keys don't expire together
}

async function loadFromDb(key, q) {
  if (dbInflight >= cfg.pgMaxConcurrentQueries) throw new OverloadedError('db saturated');
  dbInflight++;
  try {
    const { rows } = await pool.query({
      ...SQL_FIND_RESULT,
      values: [q.exam, q.year, q.roll, q.reg],
    });

    let value;
    let ttl;
    if (rows.length === 0) {
      value = NOT_FOUND;
      ttl = cfg.negativeTtlSec; // negative caching blocks cache-penetration attacks
    } else {
      const r = rows[0];
      value = JSON.stringify({
        exam: q.exam,
        year: Number(q.year),
        roll: q.roll,
        reg: q.reg,
        name: r.student_name,
        father: r.father_name,
        institution: r.institution,
        group: r.group_name,
        gpa: r.gpa === null ? null : Number(r.gpa),
        grade: r.grade,
        status: r.status,
        subjects: r.subjects,
      });
      ttl = ttlWithJitter(cfg.cacheTtlSec);
    }

    // Fire-and-forget: never make the user wait on the cache write
    redis.set(key, value, 'EX', ttl).catch(() => {});
    l1Set(key, value);
    return value;
  } finally {
    dbInflight--;
  }
}

async function getResult(q) {
  const key = `res:${q.exam}:${q.year}:${q.roll}:${q.reg}`;

  // 1) L1
  const hit1 = l1Get(key);
  if (hit1 !== undefined) return hit1;

  // 2) Redis
  try {
    const hit2 = await redis.get(key);
    if (hit2 !== null) {
      l1Set(key, hit2);
      return hit2;
    }
  } catch {
    // Redis down/slow: degrade to DB (protected by load shedding below)
  }

  // 3) PostgreSQL, one query per key no matter how many concurrent requests
  let p = inflight.get(key);
  if (!p) {
    p = loadFromDb(key, q).finally(() => inflight.delete(key));
    inflight.set(key, p);
  }
  return p;
}

// ----------------------------------------------------------------------------
// HTTP server
// ----------------------------------------------------------------------------
async function build() {
  const app = Fastify({
    logger: { level: process.env.LOG_LEVEL || 'warn' },
    disableRequestLogging: true,
    trustProxy: true,          // required so req.ip is the real client behind LB/CDN
    connectionTimeout: 10_000,
    keepAliveTimeout: 65_000,  // > typical LB idle timeout (60s)
    requestTimeout: 8_000,
    bodyLimit: 1_024,          // this service takes no bodies
    maxParamLength: 64,
    return503OnClosing: true,
  });

  // Rate limiting (Redis-backed so limits are shared across workers/instances)
  await app.register(rateLimit, {
    global: true,
    max: cfg.rateLimitGlobalMax,
    timeWindow: '1 minute',
    redis: redisRL,
    nameSpace: 'rl:',
    skipOnError: true, // if Redis is down, fail open rather than take the site down
    ban: cfg.rateLimitBanAfter,
    keyGenerator: (req) => req.ip,
    allowList: (req) => req.url === '/healthz',
    addHeadersOnExceeding: { 'x-ratelimit-limit': true, 'x-ratelimit-remaining': true },
    errorResponseBuilder: (_req, ctx) => ({
      statusCode: ctx.statusCode,
      error: 'Too Many Requests',
      message: `Rate limit exceeded. Retry in ${ctx.after}.`,
    }),
  });

  app.get('/healthz', async () => ({ ok: true }));

  app.get(
    '/api/v1/result',
    {
      config: { rateLimit: { max: cfg.rateLimitResultMax, timeWindow: '1 minute' } },
      schema: {
        querystring: {
          type: 'object',
          required: ['exam', 'year', 'roll', 'reg'],
          additionalProperties: false,
          properties: {
            exam: { type: 'string', pattern: '^[A-Za-z0-9_-]{2,16}$' },
            year: { type: 'string', pattern: '^(19|20)\\d{2}$' },
            roll: { type: 'string', pattern: '^\\d{4,12}$' },
            reg:  { type: 'string', pattern: '^\\d{4,15}$' },
          },
        },
      },
    },
    async (req, reply) => {
      try {
        const value = await getResult(req.query);

        if (value === NOT_FOUND) {
          return reply
            .code(404)
            .header('Cache-Control', 'public, max-age=30')
            .type('application/json; charset=utf-8')
            .send('{"error":"Result not found"}');
        }

        // Send the cached JSON string as-is: no parse/serialize on the hot path
        return reply
          .header('Cache-Control', 'public, max-age=60, s-maxage=300') // lets a CDN absorb repeats
          .type('application/json; charset=utf-8')
          .send(value);
      } catch (err) {
        if (err instanceof OverloadedError) {
          return reply
            .code(503)
            .header('Retry-After', '3')
            .type('application/json; charset=utf-8')
            .send('{"error":"Server busy, please retry shortly"}');
        }
        req.log.error(err);
        return reply.code(500).send({ error: 'Internal error' });
      }
    }
  );

  return app;
}

// ----------------------------------------------------------------------------
// Boot + graceful shutdown
// ----------------------------------------------------------------------------
(async () => {
  const app = await build();
  await app.listen({ port: cfg.port, host: cfg.host });
  console.log(`[worker ${process.pid}] listening on ${cfg.host}:${cfg.port}`);

  let closing = false;
  const shutdown = async (sig) => {
    if (closing) return;
    closing = true;
    console.log(`[worker ${process.pid}] ${sig}, draining...`);
    const force = setTimeout(() => process.exit(1), 10_000);
    force.unref();
    try {
      await app.close();
      await Promise.allSettled([redis.quit(), redisRL.quit(), pool.end()]);
      process.exit(0);
    } catch {
      process.exit(1);
    }
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
})().catch((err) => {
  console.error('fatal startup error', err);
  process.exit(1);
});
