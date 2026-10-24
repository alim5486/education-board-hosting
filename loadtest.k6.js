/**
 * loadtest.k6.js: Load test for resultServer.js
 *
 * 1) Export real roll/reg pairs from PostgreSQL (these must exist in the DB):
 *      psql "$DATABASE_URL" -c "\copy (SELECT roll_no, reg_no FROM results \
 *        WHERE exam_code='ALIM' AND exam_year=2026 ORDER BY random() LIMIT 200000) \
 *        TO 'rolls.csv' CSV HEADER"
 *
 * 2) Run:
 *      k6 run -e BASE_URL=http://staging:3000 -e EXAM=ALIM -e YEAR=2026 loadtest.k6.js
 *      k6 run -e PROFILE=load  -e TARGET_RPS=5000  ... loadtest.k6.js
 *      k6 run -e PROFILE=spike -e TARGET_RPS=20000 ... loadtest.k6.js
 *
 * Env:
 *   BASE_URL    server or CDN URL                          (default http://localhost:3000)
 *   EXAM, YEAR  must match the pre-warmed / stored data    (default ALIM, 2026)
 *   PROFILE     smoke | load | spike                       (default smoke)
 *   TARGET_RPS  peak requests per second                   (default 5000)
 *   SPOOF_IP    1 = send a random X-Forwarded-For per request. STAGING ONLY, so the
 *               per-IP rate limiter sees many "clients" instead of this one machine.
 *
 * IMPORTANT: the server limits each IP to 30 result requests/min. A load generator
 * uses very few IPs, so either set SPOOF_IP=1 or start the server with a high
 * RL_RESULT_MAX / RL_GLOBAL_MAX for the test. Otherwise you are measuring the rate
 * limiter, not the server.
 */

import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';
import { Counter } from 'k6/metrics';

const BASE_URL   = __ENV.BASE_URL || 'http://localhost:3000';
const EXAM       = __ENV.EXAM || 'ALIM';
const YEAR       = __ENV.YEAR || '2026';
const PROFILE    = __ENV.PROFILE || 'smoke';
const TARGET_RPS = Number(__ENV.TARGET_RPS || 5000);
const SPOOF_IP   = __ENV.SPOOF_IP === '1';

// Loaded once and shared by all VUs (no per-VU copy)
const students = new SharedArray('students', () =>
  open('./rolls.csv')
    .replace(/"/g, '')
    .split('\n')
    .slice(1) // header
    .map((l) => l.trim())
    .filter(Boolean)
    .map((l) => {
      const [roll, reg] = l.split(',');
      return { roll, reg };
    })
);

const rateLimited = new Counter('resp_429_rate_limited');
const shed        = new Counter('resp_503_load_shed');
const serverErr   = new Counter('resp_5xx_other');

// ----------------------------------------------------------------------------
// Load profiles (open model: we control arrival rate, not VU count, so a slow
// server cannot "protect itself" by slowing the test down)
// ----------------------------------------------------------------------------
const preAllocatedVUs = Math.max(100, Math.ceil(TARGET_RPS * 0.1));
const maxVUs = Math.max(preAllocatedVUs * 4, 500);

const profiles = {
  smoke: {
    executor: 'constant-arrival-rate',
    rate: 10, timeUnit: '1s', duration: '30s',
    preAllocatedVUs: 10, maxVUs: 50,
  },
  load: {
    executor: 'ramping-arrival-rate',
    startRate: 50, timeUnit: '1s', preAllocatedVUs, maxVUs,
    stages: [
      { target: Math.round(TARGET_RPS * 0.25), duration: '2m' },
      { target: Math.round(TARGET_RPS * 0.50), duration: '3m' },
      { target: TARGET_RPS,                    duration: '3m' },
      { target: TARGET_RPS,                    duration: '10m' }, // hold peak
      { target: 0,                             duration: '2m' },
    ],
  },
  spike: {
    // Results go live: traffic goes from near-idle to peak in seconds
    executor: 'ramping-arrival-rate',
    startRate: 50, timeUnit: '1s', preAllocatedVUs, maxVUs,
    stages: [
      { target: Math.round(TARGET_RPS * 0.05), duration: '1m' },
      { target: TARGET_RPS,                    duration: '20s' }, // the spike
      { target: TARGET_RPS,                    duration: '4m' },
      { target: Math.round(TARGET_RPS * 0.05), duration: '30s' },
      { target: 0,                             duration: '30s' },
    ],
  },
};

if (!profiles[PROFILE]) throw new Error(`Unknown PROFILE "${PROFILE}" (use smoke | load | spike)`);

export const options = {
  scenarios: { results: profiles[PROFILE] },
  discardResponseBodies: true, // we only check status; saves k6 memory/CPU
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    // Network latency is included, so run k6 close to the server for a fair number.
    'http_req_duration{kind:valid}': ['p(95)<100', 'p(99)<300'],
    'http_req_failed': ['rate<0.01'],   // 429/503/5xx all count as failures here
    'resp_5xx_other': ['count==0'],
    'resp_503_load_shed': ['count<100'],
    'checks': ['rate>0.99'],
  },
};

// ----------------------------------------------------------------------------
// Request mix
//   ~92% valid lookups (70% uniform, 30% from a "hot" set of 1000 students)
//    ~5% well-formed but non-existent (exercises negative caching)
//    ~3% malformed input (exercises schema validation, should be 400)
// ----------------------------------------------------------------------------
const HOT = Math.min(1000, students.length);

function randInt(n) { return Math.floor(Math.random() * n); }
function randDigits(n) {
  let s = '';
  for (let i = 0; i < n; i++) s += randInt(10);
  return s;
}
function randomIp() {
  return `${1 + randInt(223)}.${randInt(256)}.${randInt(256)}.${1 + randInt(254)}`;
}

function pickRequest() {
  const r = Math.random();
  if (r < 0.92) {
    const s = Math.random() < 0.3 ? students[randInt(HOT)] : students[randInt(students.length)];
    return { kind: 'valid', roll: s.roll, reg: s.reg, ok: [200] };
  }
  if (r < 0.97) {
    // Assumes real rolls don't start with 9 and have 8+ digits; adjust to your numbering
    return { kind: 'unknown', roll: '9' + randDigits(9), reg: '9' + randDigits(9), ok: [404] };
  }
  return { kind: 'invalid', roll: "12'; DROP--", reg: 'abc', ok: [400] };
}

export default function () {
  const q = pickRequest();
  const url = `${BASE_URL}/api/v1/result?exam=${EXAM}&year=${YEAR}` +
              `&roll=${encodeURIComponent(q.roll)}&reg=${encodeURIComponent(q.reg)}`;

  const headers = SPOOF_IP ? { 'X-Forwarded-For': randomIp() } : {};

  const res = http.get(url, {
    headers,
    tags: { kind: q.kind },
    responseCallback: http.expectedStatuses(...q.ok),
  });

  if (res.status === 429) rateLimited.add(1);
  else if (res.status === 503) shed.add(1);
  else if (res.status >= 500) serverErr.add(1);

  check(res, {
    [`${q.kind}: expected status`]: (r) => q.ok.includes(r.status),
  });
}
