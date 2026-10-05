'use strict';

const { spawn, execFileSync } = require('node:child_process');
const { mkdtempSync, rmSync, mkdirSync, writeFileSync, readFileSync, statSync, readdirSync } = require('node:fs');
const { join, resolve, dirname } = require('node:path');
const { createHash, randomBytes } = require('node:crypto');
const os = require('node:os');
const assert = require('node:assert/strict');

const ROOT = resolve(__dirname, '..');
const SQLITE = process.argv.includes('--sqlite');
const BINARY = join(ROOT, 'target', 'release', SQLITE ? 'sqlite-benchmark' : (process.platform === 'win32' ? 'niodb.exe' : 'niodb'));
// Use the repository filesystem, rather than /tmp (which may be a RAM disk).
const PORT = Number(process.env.NIODB_BENCHMARK_PORT || 7488);
const BASE = `http://127.0.0.1:${PORT}`;
const OUTPUT = resolve(process.env.NIODB_BENCHMARK_OUTPUT || join(ROOT, SQLITE ? 'benchmarks/sqlite.json' : 'benchmarks/latest.json'));
const TOTAL = Number(process.env.NIODB_BENCHMARK_RECORDS || 10000);
const BATCH = Number(process.env.NIODB_BENCHMARK_BATCH_SIZE || 500);
const DIMENSIONS = Number(process.env.NIODB_BENCHMARK_DIMENSIONS || 8);
if (!Number.isInteger(TOTAL) || TOTAL < 1000 || !Number.isInteger(BATCH) || BATCH < 1 ||
    !Number.isInteger(DIMENSIONS) || DIMENSIONS < 1 || DIMENSIONS > 4096) {
  throw new Error('Use at least 1000 records, a positive batch size, and 1–4096 dimensions');
}
const TELEMETRY = Math.floor(TOTAL * 0.5);
const CUSTOMERS = Math.floor(TOTAL * 0.3);
const VECTOR_START = TELEMETRY + CUSTOMERS;
const DATA = mkdtempSync(join(ROOT, '.benchmark-data-'));
let server;
let stderr = '';
let token;
let seed = 42;
function random() { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296; }
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
function percentile(values, percent) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.max(0, Math.ceil(percent / 100 * sorted.length) - 1)];
}
function summary(values) {
  return { samples: values.length, mean_ms: values.reduce((a, b) => a + b, 0) / values.length,
    p50_ms: percentile(values, 50), p95_ms: percentile(values, 95), p99_ms: percentile(values, 99), max_ms: Math.max(...values) };
}
async function request(path, { method = 'GET', body, raw = false, auth = true, expected = 200, headers = {} } = {}) {
  const response = await fetch(BASE + path, { method,
    headers: { ...(auth ? { Authorization: `Bearer ${token}` } : {}), ...(body && !raw ? { 'Content-Type': 'application/json' } : {}), ...headers },
    body: body === undefined ? undefined : raw ? body : JSON.stringify(body), signal: AbortSignal.timeout(15000) });
  const value = raw ? Buffer.from(await response.arrayBuffer()) : await response.json();
  assert.equal(response.status, expected, `${method} ${path}: ${JSON.stringify(value)}`);
  return value;
}
async function measure(samples, warmups, operation) {
  for (let i = 0; i < warmups; i++) await operation(i);
  const times = [];
  for (let i = 0; i < samples; i++) {
    const start = performance.now();
    await operation(i);
    times.push(performance.now() - start);
  }
  return summary(times);
}
function bytesUnder(path) {
  return readdirSync(path, { withFileTypes: true }).reduce((sum, entry) => {
    const file = join(path, entry.name);
    return sum + (entry.isDirectory() ? bytesUnder(file) : statSync(file).size);
  }, 0);
}
function processMemory(pid) {
  try {
    if (process.platform === 'darwin') {
      const rss = Number(execFileSync('ps', ['-o', 'rss=', '-p', String(pid)], { encoding: 'utf8' }).trim()) * 1024;
      return Number.isFinite(rss) && rss > 0 ? { rss, peak: null, pss: null } : null;
    }
    const status = readFileSync(`/proc/${pid}/status`, 'utf8');
    const rollup = readFileSync(`/proc/${pid}/smaps_rollup`, 'utf8');
    return { rss: Number(status.match(/^VmRSS:\s+(\d+)/m)[1]) * 1024,
      peak: Number(status.match(/^VmHWM:\s+(\d+)/m)[1]) * 1024,
      pss: Number(rollup.match(/^Pss:\s+(\d+)/m)[1]) * 1024 };
  } catch { return null; }
}
const memory = [];
function resourceSnapshot(stage) {
  const rust = processMemory(server.pid);
  if (!rust) return null;
  const pids = new Set();
  // Node can be spawned by any Tokio thread, so inspect every task's children.
  try {
    for (const task of readdirSync(`/proc/${server.pid}/task`)) {
      try {
        for (const pid of readFileSync(`/proc/${server.pid}/task/${task}/children`, 'utf8').trim().split(/\s+/)) {
          if (pid) pids.add(Number(pid));
        }
      } catch {}
    }
  } catch {}
  if (process.platform === 'darwin') {
    try {
      for (const line of execFileSync('ps', ['-axo', 'pid=,ppid='], { encoding: 'utf8' }).trim().split('\n')) {
        const [pid, parent] = line.trim().split(/\s+/).map(Number);
        if (parent === server.pid) pids.add(pid);
      }
    } catch {}
  }
  const workers = [...pids].map(pid => processMemory(pid)).filter(Boolean);
  const sqlRss = workers.reduce((sum, worker) => sum + worker.rss, 0);
  const sqlPss = rust.pss === null ? null : workers.reduce((sum, worker) => sum + worker.pss, 0);
  const snapshot = { stage, rust_rss_bytes: rust.rss, rust_peak_rss_bytes: rust.peak,
    sql_worker_count: workers.length, sql_workers_rss_bytes: sqlRss, combined_rss_bytes: rust.rss + sqlRss,
    rust_pss_bytes: rust.pss, sql_workers_pss_bytes: sqlPss, combined_pss_bytes: rust.pss === null ? null : rust.pss + sqlPss };
  memory.push(snapshot);
  console.log(`Memory ${stage}: server ${(rust.rss/1048576).toFixed(2)} MiB, SQL workers ${(sqlRss/1048576).toFixed(2)} MiB (${workers.length} processes), combined RSS ${((rust.rss+sqlRss)/1048576).toFixed(2)} MiB.`);
  return snapshot;
}
async function cleanup() {
  if (server && server.exitCode === null && server.signalCode === null) {
    const exited = new Promise(resolve => server.once('exit', resolve));
    server.kill('SIGTERM');
    const force = setTimeout(() => server.kill('SIGKILL'), 3000);
    await exited;
    clearTimeout(force);
  }
  rmSync(DATA, { recursive: true, force: true });
}

async function main() {
  console.log(`${SQLITE ? 'SQLite adapter' : 'NioDB'} local HTTP benchmark: ${TOTAL} records, ${DIMENSIONS} vector dimensions, 4 files; one request at a time.`);
  if (SQLITE) token = randomBytes(32).toString('hex');
  else {
    const credentials = JSON.parse(execFileSync(BINARY, ['init-auth', '--dir', DATA, '--name', 'benchmark'], { encoding: 'utf8' }));
    token = `${credentials.client_token}:${credentials.secret_token}`;
  }
  server = spawn(BINARY, SQLITE ? ['--dir', DATA, '--listen', `127.0.0.1:${PORT}`] :
    ['serve', '--dir', DATA, '--listen', `127.0.0.1:${PORT}`, '--no-demo'],
    { stdio: ['ignore', 'ignore', 'pipe'], env: { ...process.env, ...(SQLITE ? { NIODB_BENCHMARK_TOKEN: token } : {}) } });
  server.stderr.on('data', chunk => { stderr = (stderr + chunk.toString()).slice(-8192); });
  let ready = false;
  for (let i = 0; i < 100; i++) {
    if (server.exitCode !== null) throw new Error(`Benchmark server exited: ${stderr}`);
    try { await request('/health', { auth: false }); ready = true; break; } catch {}
    await sleep(100);
  }
  if (!ready) throw new Error(`Benchmark server did not start: ${stderr}`);
  const health = await request('/health', { auth: false });
  resourceSnapshot('startup');
  await request('/api/v1/records', { auth: false, expected: 401 });
  assert.equal((await request('/api/v1/records')).total, 0);

  await request('/api/v1/storage', { method: 'POST', body: { name: 'assets' }, expected: 201 });
  const assets = [
    { name: 'schema.json', mime: 'application/json', bytes: Buffer.from('{"title":"Benchmark schema","version":1}\n') },
    { name: 'architecture.md', mime: 'text/markdown', bytes: Buffer.from('# Benchmark fixture\nAppend-only journal, records, files, SQL and vectors.\n') },
    { name: 'logo.svg', mime: 'image/svg+xml', bytes: Buffer.from('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10 10"><circle cx="5" cy="5" r="4"/></svg>') },
    { name: 'weights.bin', mime: 'application/octet-stream', bytes: Buffer.alloc(64 * 1024, 42) },
  ];
  for (const asset of assets) {
    const response = await fetch(`${BASE}/api/v1/storage/assets/upload?filename=${asset.name}`, {
      method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': asset.mime }, body: asset.bytes });
    const file = await response.json();
    assert.equal(response.status, 201, JSON.stringify(file));
    assert.equal(file.size, asset.bytes.length);
    assert.equal(hash(await request(`/api/v1/storage/assets/${file.id}`, { raw: true })), hash(asset.bytes));
  }
  console.log('Authentication and 4 file uploads/downloads verified.');
  resourceSnapshot('after_files');
  // No workers/webhooks/subscribers registered: measure publish acceptance only.
  const events = await measure(50, 0, i => request('/api/v1/events', {
    method: 'POST', expected: 202, body: { name: 'benchmark.ping', data: { sequence: i } } }));

  const records = Array.from({ length: TOTAL }, (_, index) => {
    const i = index + 1;
    if (i <= TELEMETRY) return { collection: 'telemetry', data: {
      metric_id: `met_${i}`, service: ['auth', 'billing', 'api', 'vectors', 'sync'][i % 5],
      cpu_percent: Math.round((Math.sin(i) * 30 + 50) * 10) / 10, mem_mb: 128 + i % 256,
      status: i % 100 === 0 ? 'degraded' : 'ok', latency_ms: 15 + i % 80,
      timestamp: 1728000000000 + i * 1000 } };
    if (i <= VECTOR_START) return { collection: 'customers', data: {
      customer_id: `cust_${i - TELEMETRY}`, name: `Client ${i - TELEMETRY}`, email: `client${i - TELEMETRY}@example.com`,
      plan: ['free', 'pro', 'enterprise'][i % 3], credits: 100 + i * 7 % 5000,
      country: ['US', 'DE', 'SG', 'JP', 'FR', 'UK', 'CA'][i % 7], active: i % 15 !== 0 } };
    const vector = Array.from({ length: DIMENSIONS }, (_, d) => Math.sin((i - VECTOR_START) * 0.1 + d));
    const norm = Math.hypot(...vector);
    return { collection: 'ai_memories', data: {
      memory_id: `mem_${i - VECTOR_START}`, summary: `Codebase architecture step ${i - VECTOR_START}`,
      embedding: vector.map(x => Math.round(x / norm * 1000) / 1000), importance: i % 10 / 10 } };
  });
  const ids = [];
  const batches = [];
  const start = performance.now();
  for (let offset = 0; offset < TOTAL; offset += BATCH) {
    const t = performance.now();
    const result = await request('/api/v1/records/bulk', { method: 'POST', body: records.slice(offset, offset + BATCH) });
    batches.push(performance.now() - t);
    assert.equal(result.inserted, Math.min(BATCH, TOTAL - offset));
    ids.push(...result.records.map(record => record.id));
  }
  const ingestionMs = performance.now() - start;
  assert.equal(ids.length, TOTAL);
  assert.equal((await request('/api/v1/records')).total, TOTAL + 4);
  console.log(`Ingested ${TOTAL} records in ${(ingestionMs / 1000).toFixed(2)} s.`);
  resourceSnapshot('after_ingestion');
  const lookups = await measure(1000, 20, async () => {
    const id = ids[Math.floor(random() * ids.length)];
    assert.equal((await request(`/api/v1/records/${id}`)).id, id);
  });
  resourceSnapshot('after_lookups');
  const scans = await measure(100, 5, async () => {
    const result = await request('/api/v1/records?collection=telemetry&limit=50');
    assert.equal(result.items.length, 50);
    assert.equal(result.total, TELEMETRY);
  });
  resourceSnapshot('after_scans');
  const vector = Array.from({ length: DIMENSIONS }, (_, d) => Math.sin(0.5 + d));
  const norm = Math.hypot(...vector);
  const vectors = await measure(100, 5, async () => {
    const result = await request('/api/v1/records/search', { method: 'POST', body: {
      collection: 'ai_memories', vector: vector.map(x => x / norm), top_k: 10, min_score: 0.6 } });
    assert.equal(result.items.length, 10);
  });
  resourceSnapshot('after_vectors');
  const queries = [
    'SELECT plan, COUNT(*) AS user_count, AVG(credits) AS avg_credits FROM customers GROUP BY plan',
    "SELECT service, AVG(latency_ms) AS avg_latency FROM telemetry WHERE status = 'ok' GROUP BY service",
    'SELECT name, email, credits FROM customers ORDER BY credits DESC LIMIT 5',
  ];
  const customers = records.filter(record => record.collection === 'customers').map(record => record.data);
  const customerGroups = new Map();
  const telemetryGroups = new Map();
  for (const customer of customers) {
    const group = customerGroups.get(customer.plan) || { count: 0, sum: 0 };
    group.count++; group.sum += customer.credits;
    customerGroups.set(customer.plan, group);
  }
  for (const { collection, data } of records) {
    if (collection !== 'telemetry' || data.status !== 'ok') continue;
    const group = telemetryGroups.get(data.service) || { count: 0, sum: 0 };
    group.count++; group.sum += data.latency_ms;
    telemetryGroups.set(data.service, group);
  }
  const topCustomers = [...customers].sort((a, b) => b.credits - a.credits).slice(0, 5)
    .map(({ name, email, credits }) => ({ name, email, credits }));
  const sqlTimes = queries.map(() => []);
  const sql = await measure(90, 9, async i => {
    const queryIndex = i % queries.length;
    const start = performance.now();
    const result = await request('/api/v1/query', { method: 'POST', body: { sql: queries[queryIndex] } });
    const elapsed = performance.now() - start;
    assert.equal(result.items.length, [3, 5, 5][queryIndex]);
    if (queryIndex === 0) {
      assert.equal(new Set(result.items.map(row => row.plan)).size, customerGroups.size);
      for (const row of result.items) {
        const expected = customerGroups.get(row.plan);
        assert.ok(expected);
        assert.equal(row.user_count, expected.count);
        assert.ok(Math.abs(row.avg_credits - expected.sum / expected.count) < 1e-9);
      }
    } else if (queryIndex === 1) {
      assert.equal(new Set(result.items.map(row => row.service)).size, telemetryGroups.size);
      for (const row of result.items) {
        const expected = telemetryGroups.get(row.service);
        assert.ok(expected);
        assert.ok(Math.abs(row.avg_latency - expected.sum / expected.count) < 1e-9);
      }
    } else {
      assert.deepEqual(result.items.map(row => row.credits), topCustomers.map(row => row.credits));
      for (const row of result.items) {
        const expected = customers.find(customer => customer.name === row.name);
        assert.ok(expected);
        assert.equal(row.email, expected.email);
        assert.equal(row.credits, expected.credits);
      }
    }
    sqlTimes[queryIndex].push(elapsed);
  });
  const sqlByQuery = queries.map((query, i) => ({ query, ...summary(sqlTimes[i].slice(3)) }));
  resourceSnapshot('after_sql');
  await sleep(2000);
  const finalMemory = resourceSnapshot('idle_2s');
  let filesystem = null;
  try { filesystem = execFileSync('df', ['-T', DATA], { encoding: 'utf8' }).trim().split('\n').pop().split(/\s+/)[1]; } catch {}
  const report = {
    measured_at: new Date().toISOString(), engine: SQLITE ? 'SQLite HTTP adapter' : 'NioDB',
    engine_version: SQLITE ? health.sqlite_version : JSON.parse(readFileSync(join(ROOT, 'package.json'), 'utf8')).version,
    binary_sha256: hash(readFileSync(BINARY)), version: JSON.parse(readFileSync(join(ROOT, 'package.json'), 'utf8')).version,
    environment: { os: `${os.type()} ${os.release()}`, arch: os.arch(), cpu: os.cpus()[0].model,
      logical_cpus: os.cpus().length, total_memory_bytes: os.totalmem(), node: process.version,
      build: SQLITE ? 'cargo build --release --locked --features sqlite-benchmark --bin sqlite-benchmark' : 'cargo build --release --locked', filesystem },
    methodology: { transport: 'HTTP over loopback, full response body parsed', concurrency: 1,
      dataset_records: TOTAL, file_metadata_records: 4, collections: { telemetry: TELEMETRY, customers: CUSTOMERS, ai_memories: TOTAL - VECTOR_START },
      vector_dimensions: DIMENSIONS, batch_size: BATCH, random_seed: 42, warmups: { lookups: 20, scans: 5, vectors: 5, sql: 9 },
      sql_queries: queries, demos: false, llm_included: false, event_handlers: 0,
      ...(SQLITE ? { sqlite_journal_mode: 'WAL', sqlite_synchronous: 'FULL', adapter: 'Rust/Axum; typed SQLite tables for benchmark SQL; vector similarity computed in Rust after SQLite scan' } : {}) },
    ingestion: { total_ms: ingestionMs, records_per_second: TOTAL / (ingestionMs / 1000), batch_latency: summary(batches) },
    latency: { point_lookup: lookups, collection_scan: scans, vector_search: vectors, sql, sql_by_query: sqlByQuery, event_publish: events },
    storage: { total_bytes: bytesUnder(DATA), journal_bytes: SQLITE ? null : statSync(join(DATA, 'journal.jsonl')).size,
      file_payload_bytes: assets.reduce((sum, asset) => sum + asset.bytes.length, 0),
      server_rss_bytes: finalMemory?.rust_rss_bytes ?? null, memory_stages: memory, rss_note: 'Stage snapshots include Rust and direct SQL child processes; peak applies to Rust only; macOS peak and PSS unavailable' },
    verification: { unauthenticated_status: 401, file_sha256_verified: 4, inserted_records: ids.length,
      query_results_checked: true, failures: 0 },
  };
  mkdirSync(dirname(OUTPUT), { recursive: true });
  writeFileSync(OUTPUT, JSON.stringify(report, null, 2) + '\n');
  console.table(Object.fromEntries(Object.entries(report.latency).filter(([, result]) => !Array.isArray(result)).map(([name, result]) => [name, {
    samples: result.samples, p50_ms: result.p50_ms.toFixed(2), p95_ms: result.p95_ms.toFixed(2), p99_ms: result.p99_ms.toFixed(2) }])));
  console.log(`Throughput: ${report.ingestion.records_per_second.toFixed(0)} records/s; data: ${(report.storage.total_bytes / 1048576).toFixed(2)} MiB.`);
  console.log(`Report saved to ${OUTPUT}. Temporary benchmark data is removed after shutdown.`);
}

main().catch(error => { console.error(error); process.exitCode = 1; }).finally(cleanup);
