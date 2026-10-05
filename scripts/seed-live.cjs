'use strict';

const { readFileSync } = require('node:fs');
const { randomBytes } = require('node:crypto');

const BASE_URL = 'http://127.0.0.1:7432';
const TOKEN = readFileSync('/home/mn/nio-labs/niodb/client-token', 'utf8').trim();
const AUTH = { Authorization: `Bearer ${TOKEN}` };

const TOTAL_RECORDS = 10000;
const BATCH_SIZE = 500;

async function request(path, options = {}) {
  const url = `${BASE_URL}${path}`;
  const res = await fetch(url, options);
  const contentType = res.headers.get('content-type') || '';
  let body;
  if (contentType.includes('application/json')) {
    body = await res.json();
  } else {
    body = await res.text();
  }
  return { status: res.status, headers: res.headers, body };
}

async function main() {
  console.log('='.repeat(70));
  console.log(`Seeding NioDB Live Server at ${BASE_URL}`);
  console.log(`Database Directory: /home/mn/nio-labs/niodb`);
  console.log('='.repeat(70));

  // 1. Files / Storage
  console.log('\n[1/3] Creating Storage Bucket & Uploading 4 Files...');
  await request('/api/v1/storage', {
    method: 'POST',
    headers: { ...AUTH, 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: 'system_assets' })
  });
  console.log('  ✓ Bucket "system_assets" ready');

  const files = [
    {
      name: 'app_schema.json',
      mime: 'application/json',
      content: Buffer.from(JSON.stringify({
        $schema: 'http://json-schema.org/draft-07/schema#',
        title: 'NioDB Live Application Schema',
        collections: ['telemetry', 'customers', 'ai_memories'],
        version: '1.0.1'
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
      content: randomBytes(64 * 1024)
    }
  ];

  for (const f of files) {
    const res = await request(`/api/v1/storage/system_assets/upload?filename=${f.name}`, {
      method: 'POST',
      headers: { ...AUTH, 'Content-Type': f.mime },
      body: f.content
    });
    console.log(`  ✓ Uploaded ${f.name} (${(f.content.length / 1024).toFixed(1)} KB) -> status ${res.status}`);
  }

  // 2. Events
  console.log('\n[2/3] Publishing Domain Events...');
  const eventTypes = ['user.signup', 'task.dispatched', 'order.fulfilled', 'agent.checkpoint', 'backup.completed'];
  for (let i = 0; i < 25; i++) {
    const evtName = eventTypes[i % eventTypes.length];
    await request('/api/v1/events', {
      method: 'POST',
      headers: { ...AUTH, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: evtName,
        data: {
          event_seq: i + 1,
          source: 'niodb-live-seed',
          timestamp: Date.now(),
          trace_id: randomBytes(8).toString('hex')
        }
      })
    });
  }
  console.log('  ✓ Published 25 events across 5 topics');

  // 3. 10,000 Records
  console.log(`\n[3/3] Generating & Bulk-Inserting ${TOTAL_RECORDS.toLocaleString()} Records...`);
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

  const t0 = performance.now();
  for (let offset = 0; offset < TOTAL_RECORDS; offset += BATCH_SIZE) {
    const chunk = allRecords.slice(offset, offset + BATCH_SIZE);
    const res = await request('/api/v1/records/bulk', {
      method: 'POST',
      headers: { ...AUTH, 'Content-Type': 'application/json' },
      body: JSON.stringify(chunk)
    });
    if (res.status !== 200) {
      throw new Error(`Bulk insert failed at offset ${offset}: ${JSON.stringify(res.body)}`);
    }
    const pct = Math.round(((offset + chunk.length) / TOTAL_RECORDS) * 100);
    process.stdout.write(`\r  ⚡ Ingested ${(offset + chunk.length).toLocaleString()} / ${TOTAL_RECORDS.toLocaleString()} records [${pct}%]...`);
  }

  const elapsed = ((performance.now() - t0) / 1000).toFixed(2);
  console.log(`\n  ✓ 10,000 records successfully populated into live server in ${elapsed}s!`);
}

main().catch(err => {
  console.error('\n❌ Seeding failed:', err);
  process.exit(1);
});
