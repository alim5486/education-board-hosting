'use strict';

/**
 * gateway.js: API gateway / reverse proxy for the result system
 *
 *   Client / CDN / LB
 *        |
 *   gateway.js  (port 80 in production, 3000 locally)
 *     |-- GET /api/results      --> resultServer.js  GET /api/v1/result   (load-balanced)
 *     |-- GET /api/v1/result    --> same (alias, so loadtest.k6.js works unchanged)
 *     |-- GET /healthz          --> gateway health (503 if no upstream is healthy)
 *     `-- everything else       --> static files from ./public (dashboard, notices, 404)
 *
 * Install:
 *   npm i fastify @fastify/static @fastify/rate-limit undici
 *
 * Local test (resultServer must NOT use the gateway's port):
 *   PORT=4000 node resultServer.js
 *   UPSTREAMS=http://127.0.0.1:4000 PORT=3000 node gateway.js
 *   open http://localhost:3000
 *
 * Production (several resultServer instances, gateway in cluster mode):
 *   NODE_ENV=production CLUSTER=1 TRUST_PROXY=true \
 *   UPSTREAMS=http://10.0.1.11:4000,http://10.0.1.12:4000,http://10.0.1.13:4000 \
 *   node gateway.js
 *   (port 80 needs root or:  setcap 'cap_net_bind_service=+ep' $(which node))
 *
 * Environment:
 *   PORT                 default 80 when NODE_ENV=production, otherwise 3000
 *   HOST                 default 0.0.0.0
 *   CLUSTER=1            one gateway worker per core (WORKERS=N to override)
 *   UPSTREAMS            comma-separated resultServer base URLs (default http://127.0.0.1:4000)
 *   TRUST_PROXY          false (default) | true | hop count | comma-separated CIDRs.
 *                        Set it only if the gateway sits behind your CDN/LB and is not
 *                        reachable directly. It decides which client IP the rate limiter
 *                        and resultServer see (see "Client IP" below).
 *   PUBLIC_DIR           static files directory (default ./public)
 *   UPSTREAM_CONNECTIONS keep-alive connections per upstream per worker (default 256)
 *   RETRIES              extra attempts on ANOTHER upstream after a failure (default 1)
 *   GW_RL_MAX / GW_RL_API_MAX   coarse per-IP limits per minute (default 600 / 120)
 *   GW_RL_DISABLE=1      turn off the gateway limiter (e.g. during load tests)
 *
 * Client IP:
 *   resultServer.js runs with trustProxy:true and rate-limits on the IP it reads from
 *   X-Forwarded-For. The gateway therefore NEVER forwards a client-supplied
 *   X-Forwarded-For: it overwrites it with the IP it determined itself. Otherwise any
 *   client could dodge the per-IP limit by sending a random header.
 *   Keep resultServer reachable only from the gateway (firewall / private network).
 *
 * Rate limiting split:
 *   The precise per-IP limit (30/min, shared through Redis) stays in resultServer.js.
 *   The gateway adds a cheap in-memory limit as a first shield. It is per worker,
 *   so treat it as coarse; keep it above the resultServer limit.
 */

const cluster = require('node:cluster');
const os = require('node:os');
const fs = require('node:fs');
const path = require('node:path');

// ----------------------------------------------------------------------------
// Configuration
// ----------------------------------------------------------------------------
function parseTrustProxy(v) {
  if (v === undefined || v === '' || v === 'false') return false;
  if (v === 'true') return true;
  if (/^\d+$/.test(v)) return Number(v);
  return v; // comma-separated IPs / CIDRs
}

const cfg = {
  port: Number(process.env.PORT || (process.env.NODE_ENV === 'production' ? 80 : 3000)),
  host: process.env.HOST || '0.0.0.0',
  cluster: process.env.CLUSTER === '1',
  workers: Number(process.env.WORKERS || os.availableParallelism?.() || os.cpus().length),

  upstreams: (process.env.UPSTREAMS || 'http://127.0.0.1:4000')
    .split(',').map((s) => s.trim().replace(/\/+$/, '')).filter(Boolean),
  upstreamConnections: Number(process.env.UPSTREAM_CONNECTIONS || 256),
  headersTimeoutMs: 4_000, // resultServer's own DB statement_timeout is 2s
  bodyTimeoutMs: 4_000,
  retries: Number(process.env.RETRIES ?? 1),
  healthIntervalMs: 2_000,
  healthTimeoutMs: 1_000,
  ejectAfterFailures: 3,

  trustProxy: parseTrustProxy(process.env.TRUST_PROXY),
  publicDir: path.resolve(process.env.PUBLIC_DIR || path.join(__dirname, 'public')),

  rlDisable: process.env.GW_RL_DISABLE === '1',
  rlGlobalMax: Number(process.env.GW_RL_MAX || 600),
  rlApiMax: Number(process.env.GW_RL_API_MAX || 120),
};

if (cfg.upstreams.length === 0) {
  console.error('UPSTREAMS is empty');
  process.exit(1);
}

// ----------------------------------------------------------------------------
// Cluster primary
// ----------------------------------------------------------------------------
if (cfg.cluster && cluster.isPrimary) {
  console.log(`[gateway primary ${process.pid}] starting ${cfg.workers} workers`);
  for (let i = 0; i < cfg.workers; i++) cluster.fork();
  cluster.on('exit', (worker, code, signal) => {
    console.error(`[gateway primary] worker ${worker.process.pid} died (${signal || code}), respawning`);
    cluster.fork();
  });
  return;
}

// ----------------------------------------------------------------------------
// Worker
// ----------------------------------------------------------------------------
const Fastify = require('fastify');
const fastifyStatic = require('@fastify/static');
const rateLimit = require('@fastify/rate-limit');
const { Pool } = require('undici');

// ----------------------------------------------------------------------------
// Upstream pool: least-pending balancing, active + passive health checks
// ----------------------------------------------------------------------------
class Upstream {
  constructor(url) {
    this.url = url;
    this.pool = new Pool(url, {
      connections: cfg.upstreamConnections,
      pipelining: 1,
      keepAliveTimeout: 30_000, // below resultServer's 65s keep-alive, so we close first
      keepAliveMaxTimeout: 60_000,
      headersTimeout: cfg.headersTimeoutMs,
      bodyTimeout: cfg.bodyTimeoutMs,
      connect: { timeout: 1_000 },
    });
    this.pending = 0;
    this.healthy = true;
    this.fails = 0;
  }

  markFailure() {
    this.fails++;
    if (this.fails >= cfg.ejectAfterFailures) this.healthy = false;
  }

  markSuccess() {
    this.fails = 0;
    this.healthy = true;
  }

  async probe() {
    try {
      const res = await this.pool.request({
        method: 'GET',
        path: '/healthz',
        headersTimeout: cfg.healthTimeoutMs,
        bodyTimeout: cfg.healthTimeoutMs,
      });
      await res.body.text();
      if (res.statusCode === 200) this.markSuccess();
      else this.markFailure();
    } catch {
      this.markFailure();
    }
  }
}

const upstreams = cfg.upstreams.map((u) => new Upstream(u));
let rr = 0;

function pick(tried) {
  let candidates = upstreams.filter((u) => u.healthy && !tried.has(u));
  // Fail open: if everything is marked down, still try them (avoids self-inflicted
  // outages from false positives); never retry one we already tried this request.
  if (candidates.length === 0 && !upstreams.some((u) => u.healthy)) {
    candidates = upstreams.filter((u) => !tried.has(u));
  }
  if (candidates.length === 0) return null;
  rr = (rr + 1) % 1_000_000_000;
  let best = null;
  for (let i = 0; i < candidates.length; i++) {
    const u = candidates[(rr + i) % candidates.length]; // rotating start = round-robin on ties
    if (best === null || u.pending < best.pending) best = u;
  }
  return best;
}

const healthTimer = setInterval(() => {
  for (const u of upstreams) u.probe();
}, cfg.healthIntervalMs);
healthTimer.unref();

// ----------------------------------------------------------------------------
// Proxying
// ----------------------------------------------------------------------------
const PASS_RESPONSE_HEADERS = [
  'content-type', 'cache-control', 'retry-after', 'etag',
  'x-ratelimit-limit', 'x-ratelimit-remaining', 'x-ratelimit-reset',
];

async function forward(req, reply, upstreamPath) {
  // Whitelist: nothing else from the client (cookies, auth, ...) reaches resultServer
  const headers = {
    accept: 'application/json',
    'user-agent': req.headers['user-agent'] || 'gateway',
    'x-request-id': req.id,
    'x-forwarded-for': req.ip, // overwritten, never appended (see "Client IP")
    'x-forwarded-proto': req.protocol,
    'x-forwarded-host': req.hostname,
  };

  const tried = new Set();
  let last = null; // last upstream response, returned if all attempts are used up

  for (let attempt = 0; attempt <= cfg.retries; attempt++) {
    const up = pick(tried);
    if (!up) break;
    tried.add(up);
    up.pending++;
    try {
      const res = await up.pool.request({ method: 'GET', path: upstreamPath, headers });
      const buf = Buffer.from(await res.body.arrayBuffer()); // results are tiny; buffering keeps retries safe
      up.fails = 0;
      last = { status: res.statusCode, headers: res.headers, buf };
      // 502/503/504 = that instance is struggling (503 is resultServer's load shedding):
      // try a different instance if there is one. 429 is per-IP, so never retried.
      if (res.statusCode === 502 || res.statusCode === 503 || res.statusCode === 504) continue;
      break;
    } catch (err) {
      up.markFailure();
      req.log.warn({ upstream: up.url, err: err.code || err.message }, 'upstream error');
    } finally {
      up.pending--;
    }
  }

  if (last) {
    reply.code(last.status);
    for (const h of PASS_RESPONSE_HEADERS) {
      const v = last.headers[h];
      if (v !== undefined) reply.header(h, v);
    }
    return reply.send(last.buf);
  }

  return reply
    .code(502)
    .header('Retry-After', '3')
    .type('application/json; charset=utf-8')
    .send('{"error":"Service temporarily unavailable"}');
}

// ----------------------------------------------------------------------------
// Static assets
// ----------------------------------------------------------------------------
let notFoundHtml;
try {
  notFoundHtml = fs.readFileSync(path.join(cfg.publicDir, '404.html'));
} catch {
  notFoundHtml = Buffer.from('<!doctype html><meta charset="utf-8"><title>404</title><h1>404 Not Found</h1>');
}

// ----------------------------------------------------------------------------
// App
// ----------------------------------------------------------------------------
let reqCounter = 0;

async function build() {
  const app = Fastify({
    logger: { level: process.env.LOG_LEVEL || 'warn' },
    disableRequestLogging: true,
    trustProxy: cfg.trustProxy,
    genReqId: () => `${process.pid}-${(reqCounter++).toString(36)}`,
    connectionTimeout: 10_000,
    keepAliveTimeout: 65_000, // > typical LB idle timeout (60s)
    requestTimeout: 8_000,
    bodyLimit: 1_024,
    maxParamLength: 128,
    return503OnClosing: true,
  });

  if (!fs.existsSync(cfg.publicDir)) {
    console.warn(`[gateway] PUBLIC_DIR not found: ${cfg.publicDir} (static pages will 404)`);
    fs.mkdirSync(cfg.publicDir, { recursive: true });
  }

  if (!cfg.rlDisable) {
    await app.register(rateLimit, {
      global: true,
      max: cfg.rlGlobalMax,
      timeWindow: '1 minute',
      cache: 200_000, // distinct IPs tracked per worker
      keyGenerator: (req) => req.ip,
      allowList: (req) => req.url === '/healthz',
      errorResponseBuilder: (_req, ctx) => ({
        statusCode: ctx.statusCode,
        error: 'Too Many Requests',
        message: `Rate limit exceeded. Retry in ${ctx.after}.`,
      }),
    });
  }

  // Security headers + request id on every response
  app.addHook('onSend', async (req, reply, payload) => {
    reply.header('x-request-id', req.id);
    reply.header('x-content-type-options', 'nosniff');
    reply.header('referrer-policy', 'no-referrer');
    if (!req.url.startsWith('/api/')) {
      reply.header('x-frame-options', 'DENY');
      reply.header('content-security-policy',
        "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; " +
        "connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'");
    }
    return payload;
  });

  // Health for the LB in front of the gateway: 503 when no upstream is healthy
  app.get('/healthz', async (_req, reply) => {
    const ok = upstreams.some((u) => u.healthy);
    return reply.code(ok ? 200 : 503).send({ ok });
  });

  // ---- API: /api/results -> resultServer GET /api/v1/result -----------------
  // Same validation as resultServer.js: junk is rejected here without touching
  // an upstream. KEEP THESE PATTERNS IN SYNC with resultServer.js.
  const querystring = {
    type: 'object',
    required: ['exam', 'year', 'roll', 'reg'],
    additionalProperties: false,
    properties: {
      exam: { type: 'string', pattern: '^[A-Za-z0-9_-]{2,16}$' },
      year: { type: 'string', pattern: '^(19|20)\\d{2}$' },
      roll: { type: 'string', pattern: '^\\d{4,12}$' },
      reg:  { type: 'string', pattern: '^\\d{4,15}$' },
    },
  };

  const apiHandler = (req, reply) => {
    const { exam, year, roll, reg } = req.query;
    // Rebuild the query from validated fields instead of forwarding the raw string
    // (no parameter smuggling, fixed order = identical cache keys/URLs for CDNs).
    const qs = new URLSearchParams({ exam, year, roll, reg }).toString();
    return forward(req, reply, `/api/v1/result?${qs}`);
  };

  const apiRoute = {
    schema: { querystring },
    ...(cfg.rlDisable ? {} : { config: { rateLimit: { max: cfg.rlApiMax, timeWindow: '1 minute' } } }),
  };
  app.get('/api/results', apiRoute, apiHandler);
  app.get('/api/v1/result', apiRoute, apiHandler);

  // ---- Static dashboard / notices -------------------------------------------
  await app.register(fastifyStatic, {
    root: cfg.publicDir,
    index: ['index.html'],
    dotfiles: 'ignore',
    setHeaders(res, filePath) {
      res.setHeader('Cache-Control', filePath.endsWith('.html')
        ? 'public, max-age=60'          // notices can change; stay fresh
        : 'public, max-age=86400');     // css/js
    },
  });

  app.setNotFoundHandler((req, reply) => {
    if (req.url.startsWith('/api/')) {
      return reply.code(404).type('application/json; charset=utf-8').send('{"error":"Not found"}');
    }
    return reply.code(404).type('text/html; charset=utf-8').send(notFoundHtml);
  });

  return app;
}

// ----------------------------------------------------------------------------
// Boot + graceful shutdown
// ----------------------------------------------------------------------------
(async () => {
  const app = await build();
  await app.listen({ port: cfg.port, host: cfg.host });
  console.log(`[gateway ${process.pid}] :${cfg.port} -> ${cfg.upstreams.join(', ')} | static: ${cfg.publicDir}`);

  let closing = false;
  const shutdown = async (sig) => {
    if (closing) return;
    closing = true;
    console.log(`[gateway ${process.pid}] ${sig}, draining...`);
    const force = setTimeout(() => process.exit(1), 10_000);
    force.unref();
    try {
      clearInterval(healthTimer);
      await app.close();
      await Promise.allSettled(upstreams.map((u) => u.pool.close()));
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
