'use strict';

const { spawn, execFileSync } = require('node:child_process');
const { rmSync, mkdirSync, existsSync, writeFileSync, readFileSync, statSync, readdirSync } = require('node:fs');
const { join, resolve } = require('node:path');
const { createHash, randomBytes } = require('node:crypto');

const ROOT_DIR = resolve(__dirname, '..');
const BINARY = join(ROOT_DIR, 'target', 'release', process.platform === 'win32' ? 'niodb.exe' : 'niodb');
const DATA_DIR = '/tmp/niodb-simulation-data';
const PORT = 7488;
const BASE_URL = `http://127.0.0.1:${PORT}`;

const TOTAL_RECORDS = 10000;
const BATCH_SIZE = 500;

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function sha256(buf) {
  return createHash('sha256').update(buf).digest('hex');
}

function percentile(arr, p) {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[idx];
}

async function request(path, options = {}) {
  const url = `${BASE_URL}${path}`;
  const res = await fetch(url, options);
  if (options.raw) {
    return { status: res.status, headers: res.headers, body: Buffer.from(await res.arrayBuffer()) };
  }
  const contentType = res.headers.get('content-type') || '';
  let body;
  if (contentType.includes('application/json')) {
    body = await res.json();
  } else if (contentType.includes('text/') || contentType.includes('xml')) {
    body = await res.text();
  } else {
    body = Buffer.from(await res.arrayBuffer());
  }
  return { status: res.status, headers: res.headers, body };
}

async function main() {
  console.log('='.repeat(70));
  console.log('   NioDB Complete Simulation & Benchmark Suite');
  console.log(`   Target: ${TOTAL_RECORDS.toLocaleString()} records, 4 files, event streams & auth`);
  console.log('='.repeat(70));

  // 1. Prepare Sandbox Directory
  if (existsSync(DATA_DIR)) {
    rmSync(DATA_DIR, { recursive: true, force: true });
  }
  mkdirSync(DATA_DIR, { recursive: true });

  // 2. Auth Simulation
  console.log('\n[1/6] 🔐 Initializing Authentication & Principals...');
  const initAuthOutput = execFileSync(BINARY, ['init-auth', '--dir', DATA_DIR, '--name', 'admin'], {
    encoding: 'utf8'
  });
  const adminTokens = JSON.parse(initAuthOutput);
  console.log('  ✓ Admin client token generated');
  console.log('  ✓ Admin secret token generated');

  const addSecretOutput = execFileSync(
    BINARY,
    ['add-secret', '--dir', DATA_DIR],
    { encoding: 'utf8' }
  );
  const workerSecretToken = addSecretOutput.trim();
  console.log('  ✓ Worker secret token generated and appended to auth.json');

  const adminAuth = { Authorization: `Bearer ${adminTokens.client_token}` };
  const workerAuth = { Authorization: `Bearer ${workerSecretToken}` };

  // 3. Start NioDB Server
  console.log('\n[2/6] 🚀 Launching Standalone NioDB Engine...');
  const serverProcess = spawn(
    BINARY,
    ['serve', '--dir', DATA_DIR, '--listen', `127.0.0.1:${PORT}`, '--auth-file', join(DATA_DIR, 'auth.json')],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  );

  let serverStarted = false;
  serverProcess.stderr.on('data', d => {
    // console.error('[niodb-stderr]', d.toString());
  });

  // Wait for health endpoint
  const startTime = Date.now();
  while (Date.now() - startTime < 10000) {
    try {
      const res = await request('/health');
      if (res.status === 200) {
        serverStarted = true;
        break;
      }
    } catch {}
    await sleep(100);
  }

  if (!serverStarted) {
    throw new Error('NioDB server failed to start within 10 seconds');
  }
  console.log(`  ✓ NioDB Server running at ${BASE_URL} (pid: ${serverProcess.pid})`);

  // Verify Auth Gates
  const unauthRes = await request('/api/v1/records');
  if (unauthRes.status !== 401) {
    throw new Error(`Expected 401 Unauthorized, got ${unauthRes.status}`);
  }
  console.log('  ✓ Security check: Unauthenticated access strictly blocked (401 Unauthorized)');

  const authRes = await request('/api/v1/records', { headers: adminAuth });
  if (authRes.status !== 200) {
    throw new Error(`Expected 200 OK, got ${authRes.status}`);
  }
  console.log('  ✓ Security check: Bearer token authentication verified (200 OK)');

  // 4. File / Blob Simulation (4 files)
  console.log('\n[3/6] 📁 Simulating File Storage & Blobs (4 Diverse Assets)...');
  await request('/api/v1/storage', {
    method: 'POST',
    headers: { ...adminAuth, 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: 'system_assets' })
  });
  console.log('  ✓ Bucket "system_assets" created');

  const filesToUpload = [
    {
      name: 'app_schema.json',
      mime: 'application/json',
      content: Buffer.from(JSON.stringify({
        $schema: 'http://json-schema.org/draft-07/schema#',
        title: 'NioDB App Entity Schema',
        properties: { id: { type: 'string' }, timestamp: { type: 'integer' } }
      }, null, 2))
    },
    {
      name: 'architecture_spec.md',
      mime: 'text/markdown',
      content: Buffer.from(`# NioDB Architecture Specification\n\n- Pure Rust Storage Engine\n- Append-Only Journaling with Fast Recovery\n- In-Memory Vector Search with Cosine Distance\n- Embedded AlaSQL Sandboxed Execution\n- Single Binary Zero Daemon Footprint\n`)
    },
    {
      name: 'brand_logo.svg',
      mime: 'image/svg+xml',
      content: Buffer.from(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><circle cx="50" cy="50" r="45" fill="#4F46E5"/><text x="50" y="58" font-size="24" fill="#ffffff" text-anchor="middle" font-family="sans-serif">NIO</text></svg>`)
    },
    {
      name: 'weights_matrix.bin',
      mime: 'application/octet-stream',
      content: randomBytes(64 * 1024) // 64 KB binary dataset
    }
  ];

  const uploadedFiles = [];
  for (const file of filesToUpload) {
    const t0 = performance.now();
    const upRes = await request(`/api/v1/storage/system_assets/upload?filename=${file.name}`, {
      method: 'POST',
      headers: { ...adminAuth, 'Content-Type': file.mime },
      body: file.content
    });
    const uploadDuration = (performance.now() - t0).toFixed(2);
    if (upRes.status !== 201) {
      throw new Error(`Upload failed for ${file.name}: ${JSON.stringify(upRes.body)}`);
    }
    const fileId = upRes.body.id || upRes.body.file_id || upRes.body.filename;

    // Download & Verify
    const dlRes = await request(`/api/v1/storage/system_assets/${fileId}`, { headers: adminAuth, raw: true });
    if (dlRes.status !== 200) {
      throw new Error(`Download failed for ${file.name}: status ${dlRes.status}`);
    }
    const dlHash = sha256(dlRes.body);
    const origHash = sha256(file.content);
    if (dlHash !== origHash) {
      throw new Error(`SHA256 mismatch for ${file.name}! Original: ${origHash}, Downloaded: ${dlHash}`);
    }
    uploadedFiles.push({ name: file.name, size: file.content.length, durationMs: uploadDuration });
    console.log(`  ✓ [File] ${file.name.padEnd(22)} (${(file.content.length / 1024).toFixed(1)} KB) -> Upload & SHA-256 match in ${uploadDuration}ms`);
  }

  // 5. Events Simulation
  console.log('\n[4/6] ⚡ Simulating Event Streaming & Pub/Sub...');
  const eventTypes = ['user.signup', 'task.dispatched', 'order.fulfilled', 'agent.checkpoint', 'backup.completed'];
  const eventTimes = [];
  for (let i = 0; i < 50; i++) {
    const evtName = eventTypes[i % eventTypes.length];
    const t0 = performance.now();
    const evRes = await request('/api/v1/events', {
      method: 'POST',
      headers: { ...workerAuth, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: evtName,
        data: {
          event_seq: i + 1,
          agent: 'agent-worker',
          timestamp: Date.now(),
          trace_id: randomBytes(8).toString('hex')
        }
      })
    });
    eventTimes.push(performance.now() - t0);
    if (evRes.status < 200 || evRes.status >= 300) {
      throw new Error(`Event publish failed: ${JSON.stringify(evRes.body)}`);
    }
  }
  console.log(`  ✓ Published 50 domain events across 5 topics (Avg publish latency: ${(eventTimes.reduce((a,b)=>a+b,0)/eventTimes.length).toFixed(2)}ms)`);

  // 6. Ingesting 10,000 Records
  console.log(`\n[5/6] 📊 Ingesting ${TOTAL_RECORDS.toLocaleString()} Realistic Documents Across 3 Collections...`);
  console.log(`  - 5,000 "telemetry" metric time-series logs`);
  console.log(`  - 3,000 "customers" user account profiles`);
  console.log(`  - 2,000 "ai_memories" with 8D float embedding vectors`);

  const plans = ['free', 'pro', 'enterprise'];
  const services = ['auth-service', 'billing-worker', 'api-gateway', 'vector-index', 'db-syncer'];
  const countries = ['US', 'DE', 'SG', 'JP', 'FR', 'UK', 'CA'];

  const allRecords = [];
  for (let i = 1; i <= TOTAL_RECORDS; i++) {
    if (i <= 5000) {
      allRecords.push({
        collection: 'telemetry',
        data: {
          metric_id: `met_${i}`,
          service: services[i % services.length],
          cpu_percent: Math.round((Math.sin(i) * 30 + 50) * 10) / 10,
          mem_mb: 128 + (i % 256),
          status: i % 100 === 0 ? 'degraded' : 'ok',
          latency_ms: Math.round(15 + (i % 80) + Math.random() * 5),
          timestamp: 1728000000000 + i * 1000
        }
      });
    } else if (i <= 8000) {
      const custIdx = i - 5000;
      allRecords.push({
        collection: 'customers',
        data: {
          customer_id: `cust_${custIdx}`,
          name: `Client User ${custIdx}`,
          email: `client${custIdx}@example.com`,
          plan: plans[custIdx % plans.length],
          credits: 100 + (custIdx * 7) % 5000,
          country: countries[custIdx % countries.length],
          active: custIdx % 15 !== 0,
          created_at: 1720000000000 + custIdx * 100000
        }
      });
    } else {
      const memIdx = i - 8000;
      // 8-dim normalized embedding vector
      const v = Array.from({ length: 8 }, (_, d) => Math.sin(memIdx * 0.1 + d));
      const norm = Math.sqrt(v.reduce((s, x) => s + x * x, 0)) || 1;
      const normalizedVector = v.map(x => Math.round((x / norm) * 1000) / 1000);

      allRecords.push({
        collection: 'ai_memories',
        data: {
          memory_id: `mem_${memIdx}`,
          agent: memIdx % 2 === 0 ? 'claude-3-7-sonnet' : 'gpt-4o',
          goal: `Task resolution session ${memIdx % 200}`,
          summary: `Synthesized codebase architecture step ${memIdx}`,
          embedding: normalizedVector,
          importance: Math.round(Math.random() * 10) / 10
        }
      });
    }
  }

  const batchLatencies = [];
  const insertedIds = [];
  const ingestStart = performance.now();

  for (let offset = 0; offset < TOTAL_RECORDS; offset += BATCH_SIZE) {
    const chunk = allRecords.slice(offset, offset + BATCH_SIZE);
    const b0 = performance.now();
    const bulkRes = await request('/api/v1/records/bulk', {
      method: 'POST',
      headers: { ...adminAuth, 'Content-Type': 'application/json' },
      body: JSON.stringify(chunk)
    });
    const bTime = performance.now() - b0;
    batchLatencies.push(bTime);

    if (bulkRes.status !== 200) {
      throw new Error(`Bulk insert batch at ${offset} failed: ${JSON.stringify(bulkRes.body)}`);
    }

    const recs = bulkRes.body.records || [];
    for (const r of recs) {
      if (r.id) insertedIds.push(r.id);
    }

    const pct = Math.round(((offset + chunk.length) / TOTAL_RECORDS) * 100);
    process.stdout.write(`\r  ⚡ Ingested ${(offset + chunk.length).toLocaleString()} / ${TOTAL_RECORDS.toLocaleString()} records [${pct}%] (Batch: ${bTime.toFixed(1)}ms)...`);
  }

  const ingestElapsedSec = (performance.now() - ingestStart) / 1000;
  const throughput = Math.round(TOTAL_RECORDS / ingestElapsedSec);
  console.log(`\n  ✓ Ingested ${TOTAL_RECORDS.toLocaleString()} records in ${ingestElapsedSec.toFixed(2)}s (${throughput.toLocaleString()} records/sec)`);

  // 7. Benchmark Suite: Lookups, Scans, Vector Search & AlaSQL
  console.log('\n[6/6] 🔬 Running Performance & Latency Benchmark...');

  // A. Random Point Lookups by ID
  const pointLookupTimes = [];
  const sampleIds = [];
  for (let i = 0; i < 50; i++) {
    const randIdx = Math.floor(Math.random() * insertedIds.length);
    sampleIds.push(insertedIds[randIdx]);
  }

  for (const id of sampleIds) {
    const t0 = performance.now();
    const lRes = await request(`/api/v1/records/${id}`, { headers: adminAuth });
    pointLookupTimes.push(performance.now() - t0);
    if (lRes.status !== 200) {
      throw new Error(`Point lookup failed for ${id}`);
    }
  }

  // B. Filtered Collection Scans
  const scanTimes = [];
  for (let i = 0; i < 20; i++) {
    const t0 = performance.now();
    const sRes = await request('/api/v1/records?collection=telemetry&limit=50', { headers: adminAuth });
    scanTimes.push(performance.now() - t0);
    if (sRes.status !== 200) {
      throw new Error('Scan failed');
    }
  }

  // C. Vector Similarity Search
  const vectorTimes = [];
  const queryVector = Array.from({ length: 8 }, (_, d) => Math.sin(0.5 + d));
  const vNorm = Math.sqrt(queryVector.reduce((s, x) => s + x * x, 0)) || 1;
  const normQueryVector = queryVector.map(x => Math.round((x / vNorm) * 1000) / 1000);

  for (let i = 0; i < 20; i++) {
    const t0 = performance.now();
    const vRes = await request('/api/v1/records/search', {
      method: 'POST',
      headers: { ...adminAuth, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        collection: 'ai_memories',
        vector: normQueryVector,
        top_k: 10,
        min_score: 0.6
      })
    });
    vectorTimes.push(performance.now() - t0);
    if (vRes.status !== 200) {
      throw new Error(`Vector search failed: ${JSON.stringify(vRes.body)}`);
    }
  }

  // D. Sandboxed AlaSQL Engine Queries
  const sqlTimes = [];
  const sqlQueries = [
    'SELECT plan, COUNT(*) as user_count, AVG(credits) as avg_credits FROM customers GROUP BY plan',
    "SELECT service, AVG(latency_ms) as avg_latency FROM telemetry WHERE status = 'ok' GROUP BY service",
    'SELECT name, email, credits FROM customers ORDER BY credits DESC LIMIT 5'
  ];

  let sqlResults = null;
  for (const query of sqlQueries) {
    const t0 = performance.now();
    const qRes = await request('/api/v1/query', {
      method: 'POST',
      headers: { ...adminAuth, 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: query })
    });
    sqlTimes.push(performance.now() - t0);
    if (qRes.status !== 200) {
      throw new Error(`SQL query failed: ${JSON.stringify(qRes.body)}`);
    }
    if (!sqlResults) sqlResults = qRes.body;
  }

  // E. Disk Footprint & Process Resource Measurement
  function getDirectorySize(dir) {
    let total = 0;
    const entries = readdirSync(dir, { withFileTypes: true });
    for (const entry of entries) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) {
        total += getDirectorySize(full);
      } else {
        total += statSync(full).size;
      }
    }
    return total;
  }

  const diskBytes = getDirectorySize(DATA_DIR);
  const diskMB = (diskBytes / (1024 * 1024)).toFixed(2);

  // Terminate server cleanly
  serverProcess.kill('SIGTERM');
  await sleep(500);

  // Results Presentation
  console.log('\n' + '='.repeat(70));
  console.log('                 FINAL PERFORMANCE REPORT');
  console.log('='.repeat(70));

  console.log('\n📈 INGESTION & THROUGHPUT:');
  console.log(`  • Total Simulated Records : ${TOTAL_RECORDS.toLocaleString()}`);
  console.log(`  • Ingestion Time          : ${ingestElapsedSec.toFixed(2)} seconds`);
  console.log(`  • Sustained Throughput    : ${throughput.toLocaleString()} records / sec`);
  console.log(`  • Batch Latency (500 rec) : P50 = ${percentile(batchLatencies, 50).toFixed(1)}ms | P95 = ${percentile(batchLatencies, 95).toFixed(1)}ms | Max = ${Math.max(...batchLatencies).toFixed(1)}ms`);

  console.log('\n⚡ QUERY LATENCY (10,000 Record Corpus):');
  console.log(`  • Point Lookup (by ID)    : P50 = ${percentile(pointLookupTimes, 50).toFixed(2)}ms | P95 = ${percentile(pointLookupTimes, 95).toFixed(2)}ms | P99 = ${percentile(pointLookupTimes, 99).toFixed(2)}ms`);
  console.log(`  • Filtered Scan (limit 50): P50 = ${percentile(scanTimes, 50).toFixed(2)}ms | P95 = ${percentile(scanTimes, 95).toFixed(2)}ms`);
  console.log(`  • 8D Vector Search (2K vec): P50 = ${percentile(vectorTimes, 50).toFixed(2)}ms | P95 = ${percentile(vectorTimes, 95).toFixed(2)}ms`);
  console.log(`  • AlaSQL Aggregation Query: P50 = ${percentile(sqlTimes, 50).toFixed(2)}ms | P95 = ${percentile(sqlTimes, 95).toFixed(2)}ms`);

  console.log('\n🗄️ DISK & STORAGE FOOTPRINT:');
  console.log(`  • Total Data Directory    : ${diskMB} MB for ${TOTAL_RECORDS.toLocaleString()} records + 4 files`);
  console.log(`  • Average Record Footprint: ${((diskBytes / TOTAL_RECORDS) / 1024).toFixed(2)} KB / record (including index & journal)`);

  console.log('\n🛡️ FILES & ASSETS STORED:');
  for (const f of uploadedFiles) {
    console.log(`  • ${f.name.padEnd(24)}: ${(f.size / 1024).toFixed(1)} KB (verified SHA-256)`);
  }

  console.log('\n🎯 SQL ENGINE SAMPLE RESULT:');
  console.log(JSON.stringify(sqlResults?.items || sqlResults, null, 2));

  console.log('\n' + '='.repeat(70));
  console.log('   NioDB Stress Test & Data Simulation: 100% SUCCESS');
  console.log('='.repeat(70) + '\n');
}

main().catch(err => {
  console.error('\n❌ Benchmark simulation failed:', err);
  process.exit(1);
});
