'use strict';

/**
 * prewarm.js: Load published results from PostgreSQL into Redis BEFORE publication.
 *
 * The cache key and JSON payload are identical to resultServer.js, so every
 * pre-warmed result is served as a Redis hit from the first request.
 *
 * Usage:
 *   node prewarm.js --exam ALIM --year 2026
 *   node prewarm.js --exam ALIM --year 2026 --batch 5000 --concurrency 4 --ttl 172800
 *   node prewarm.js --exam ALIM --year 2026 --dry-run
 *   node prewarm.js --exam ALIM --year 2026 --after 123456,987654321   # resume
 *
 * Env: DATABASE_URL, REDIS_URL (same as resultServer.js)
 *
 * Design:
 *   - Keyset pagination on (roll_no, reg_no): constant cost per batch, no OFFSET,
 *     no long-running cursor holding a snapshot open.
 *   - Redis pipelines of N SET ... EX commands, several batches in flight.
 *   - Resumable: progress logs print the last (roll,reg) so you can use --after.
 *   - Spot-check at the end: a sample of written keys is read back from Redis.
 */

const Redis = require('ioredis');
const { Pool } = require('pg');

// ----------------------------------------------------------------------------
// CLI / config
// ----------------------------------------------------------------------------
function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    const key = a.slice(2);
    const next = argv[i + 1];
    if (next === undefined || next.startsWith('--')) out[key] = true;
    else { out[key] = next; i++; }
  }
  return out;
}

const args = parseArgs(process.argv);
const cfg = {
  exam: args.exam,
  year: args.year,
  batch: Number(args.batch || 5000),
  concurrency: Number(args.concurrency || 4),
  ttlSec: Number(args.ttl || 48 * 60 * 60), // 48h: outlives the server's 6h on-demand TTL
  dryRun: Boolean(args['dry-run']),
  after: args.after ? String(args.after).split(',') : ['', ''],
  sampleMax: 2000,
  redisUrl: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
  pgUrl: process.env.DATABASE_URL || 'postgres://postgres:postgres@127.0.0.1:5432/results',
};

if (!cfg.exam || !cfg.year) {
  console.error('Usage: node prewarm.js --exam <CODE> --year <YYYY> [--batch N] [--concurrency N] [--ttl SEC] [--dry-run] [--after roll,reg]');
  process.exit(1);
}
// Must match the server's validation, otherwise keys would never be requested
if (!/^[A-Za-z0-9_-]{2,16}$/.test(cfg.exam) || !/^(19|20)\d{2}$/.test(String(cfg.year))) {
  console.error('Invalid --exam or --year format');
  process.exit(1);
}
cfg.year = String(cfg.year);

// ----------------------------------------------------------------------------
// Connections
// ----------------------------------------------------------------------------
const redis = new Redis(cfg.redisUrl, { maxRetriesPerRequest: 3, connectTimeout: 5_000 });
redis.on('error', (e) => console.error('[redis]', e.message));
const pool = new Pool({ connectionString: cfg.pgUrl, max: 2, statement_timeout: 60_000 });

// ----------------------------------------------------------------------------
// Must stay in sync with resultServer.js
// ----------------------------------------------------------------------------
const cacheKey = (roll, reg) => `res:${cfg.exam}:${cfg.year}:${roll}:${reg}`;

function buildPayload(r) {
  return JSON.stringify({
    exam: cfg.exam,
    year: Number(cfg.year),
    roll: r.roll_no,
    reg: r.reg_no,
    name: r.student_name,
    father: r.father_name,
    institution: r.institution,
    group: r.group_name,
    gpa: r.gpa === null ? null : Number(r.gpa),
    grade: r.grade,
    status: r.status,
    subjects: r.subjects,
  });
}

const ttlWithJitter = (base) => base + Math.floor(Math.random() * base * 0.1);

const SQL_PAGE = `
  SELECT roll_no, reg_no, student_name, father_name, institution, group_name,
         gpa, grade, status, subjects
    FROM results
   WHERE exam_code = $1 AND exam_year = $2
     AND (roll_no, reg_no) > ($3, $4)
   ORDER BY roll_no, reg_no
   LIMIT $5`;

// ----------------------------------------------------------------------------
// Main
// ----------------------------------------------------------------------------
const stats = { read: 0, written: 0, failed: 0, bytes: 0 };
const sampleKeys = [];
const pending = new Set();

async function enqueue(task) {
  const p = task().finally(() => pending.delete(p));
  pending.add(p);
  if (pending.size >= cfg.concurrency) await Promise.race(pending);
}

async function writeBatch(entries) {
  try {
    const pipe = redis.pipeline();
    for (const [key, val] of entries) pipe.set(key, val, 'EX', ttlWithJitter(cfg.ttlSec));
    const res = await pipe.exec();
    for (const [err] of res) {
      if (err) stats.failed++; else stats.written++;
    }
  } catch (e) {
    stats.failed += entries.length;
    console.error('[batch] failed:', e.message);
  }
}

(async () => {
  const t0 = Date.now();
  console.log(`Pre-warming exam=${cfg.exam} year=${cfg.year} batch=${cfg.batch} ` +
              `concurrency=${cfg.concurrency} ttl=${cfg.ttlSec}s${cfg.dryRun ? ' (dry run)' : ''}`);

  let [lastRoll, lastReg] = cfg.after;
  let batchNo = 0;

  for (;;) {
    const { rows } = await pool.query(SQL_PAGE, [cfg.exam, cfg.year, lastRoll, lastReg, cfg.batch]);
    if (rows.length === 0) break;

    const entries = rows.map((r) => [cacheKey(r.roll_no, r.reg_no), buildPayload(r)]);
    stats.read += rows.length;
    for (const [, v] of entries) stats.bytes += v.length;

    // Reservoir-ish sampling of keys for the final spot-check
    for (const [k] of entries) {
      if (sampleKeys.length < cfg.sampleMax) sampleKeys.push(k);
      else if (Math.random() < cfg.sampleMax / stats.read) sampleKeys[Math.floor(Math.random() * cfg.sampleMax)] = k;
    }

    if (!cfg.dryRun) await enqueue(() => writeBatch(entries));

    const last = rows[rows.length - 1];
    lastRoll = last.roll_no;
    lastReg = last.reg_no;
    batchNo++;

    if (batchNo % 10 === 0) {
      const secs = (Date.now() - t0) / 1000;
      console.log(`  ${stats.read.toLocaleString()} read | ${stats.written.toLocaleString()} cached | ` +
                  `${stats.failed} failed | ${(stats.read / secs).toFixed(0)} rows/s | last=${lastRoll},${lastReg}`);
    }
  }

  await Promise.all(pending);

  // Spot-check
  let missing = 0;
  if (!cfg.dryRun && sampleKeys.length) {
    const vals = await redis.mget(sampleKeys);
    missing = vals.filter((v) => v === null).length;
  }

  const secs = (Date.now() - t0) / 1000;
  const avgBytes = stats.read ? Math.round(stats.bytes / stats.read) : 0;
  console.log('\n--- Done ---');
  console.log(`Rows read     : ${stats.read.toLocaleString()}`);
  console.log(`Keys cached   : ${stats.written.toLocaleString()}`);
  console.log(`Failed        : ${stats.failed}`);
  console.log(`Avg payload   : ${avgBytes} bytes (~${(stats.bytes / 1024 / 1024).toFixed(0)} MB raw, plus Redis overhead of ~100 bytes/key)`);
  console.log(`Elapsed       : ${secs.toFixed(1)}s`);
  if (!cfg.dryRun) console.log(`Spot-check    : ${sampleKeys.length - missing}/${sampleKeys.length} sampled keys present in Redis`);
  console.log(`Last key      : ${lastRoll},${lastReg}   (use --after "${lastRoll},${lastReg}" to resume)`);

  await Promise.allSettled([redis.quit(), pool.end()]);
  process.exit(stats.failed > 0 || missing > 0 ? 2 : 0);
})().catch(async (err) => {
  console.error('fatal:', err);
  await Promise.allSettled([redis.quit(), pool.end()]);
  process.exit(1);
});
