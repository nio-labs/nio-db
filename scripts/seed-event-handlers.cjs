#!/usr/bin/env node
'use strict';

const { createServer } = require('node:http');
const { createHmac, timingSafeEqual } = require('node:crypto');
const { readFileSync, writeFileSync, appendFileSync, existsSync, chmodSync } = require('node:fs');
const { resolve, join } = require('node:path');

const DATA_DIR = resolve(process.env.NIODB_DATA_DIR || join(__dirname, '../nio-db'));
const BASE_URL = process.env.NIODB_BASE_URL || 'http://127.0.0.1:7432';
const TOKEN = readFileSync(join(DATA_DIR, 'client-token'), 'utf8').trim();
const EVENT_NAME = 'demo.order.created';
const PORT = Number(process.env.NIODB_DEMO_WEBHOOK_PORT || 7431);
const URL = `http://127.0.0.1:${PORT}/webhook`;
const STATE_FILE = join(DATA_DIR, 'seed-event-handlers.json');
const WEBHOOK_LOG = join(DATA_DIR, 'seed-webhook-events.jsonl');
const WORKER_LOG = join(DATA_DIR, 'seed-worker-events.jsonl');
const LISTEN = process.argv.includes('--listen');
let state = existsSync(STATE_FILE) ? JSON.parse(readFileSync(STATE_FILE, 'utf8')) : {};

async function request(path, body) {
  const res = await fetch(BASE_URL + path, {
    method: body ? 'POST' : 'GET',
    headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
    ...(body ? { body: JSON.stringify(body) } : {})
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`${path}: ${res.status} ${data.error?.message || 'Request failed'}`);
  return data;
}

const received = new Set();
const receiver = createServer(async (req, res) => {
  if (req.method === 'GET' && req.url === '/webhook') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>NioDB Demo Webhook</title>
<style>body{font:14px ui-monospace,monospace;color:#0f172a;background:#f8fafc;margin:0;padding:32px}main{max-width:900px;margin:auto}h1{font-size:22px}p{color:#64748b;line-height:1.6}.status{color:#0d9488}article{background:white;border:1px solid #e2e8f0;border-radius:12px;padding:16px;margin:12px 0}pre{overflow:auto;white-space:pre-wrap;word-break:break-word;font-size:12px}</style></head><body><main>
<h1>NioDB Demo Webhook</h1><p class="status" id="status">Receiver listening</p><p>This endpoint accepts signed POST deliveries for <strong>demo.order.created</strong>. Recent deliveries appear below. This page does not publish events.</p><section id="deliveries"></section></main>
<script>async function refresh(){try{const response=await fetch('/deliveries');if(!response.ok)throw new Error();const events=await response.json();const list=document.getElementById('deliveries');list.replaceChildren();document.getElementById('status').textContent='Receiver listening · '+events.length+' recent deliveries';if(!events.length){const p=document.createElement('p');p.textContent='No deliveries yet. Publish demo.order.created from the console to test the webhook.';list.append(p)}for(const event of events){const card=document.createElement('article');const heading=document.createElement('strong');heading.textContent=event.name;const pre=document.createElement('pre');pre.textContent=JSON.stringify(event,null,2);card.append(heading,pre);list.append(card)}}catch{document.getElementById('status').textContent='Receiver unavailable. Reconnecting...'}}refresh();setInterval(refresh,2000);</script></body></html>`);
    return;
  }
  if (req.method === 'GET' && req.url === '/deliveries') {
    const events = existsSync(WEBHOOK_LOG) ? readFileSync(WEBHOOK_LOG, 'utf8').split('\n').filter(Boolean).slice(-50).flatMap(line => {
      try { return [JSON.parse(line)]; } catch { return []; }
    }).reverse() : [];
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }).end(JSON.stringify(events));
    return;
  }
  if (req.method !== 'POST' || req.url !== '/webhook') {
    res.writeHead(404).end();
    return;
  }
  try {
    const chunks = [];
    let length = 0;
    for await (const chunk of req) {
      length += chunk.length;
      if (length > 65536) throw new Error('Payload too large');
      chunks.push(chunk);
    }
    const bytes = Buffer.concat(chunks);
    const signature = Buffer.from(String(req.headers['x-niodb-signature'] || ''));
    const expected = Buffer.from('sha256=' + createHmac('sha256', state.webhook_secret).update(bytes).digest('hex'));
    if (signature.length !== expected.length || !timingSafeEqual(signature, expected)) {
      res.writeHead(401).end();
      return;
    }
    const event = JSON.parse(bytes.toString('utf8'));
    if (!received.has(event.id)) {
      appendFileSync(WEBHOOK_LOG, JSON.stringify(event) + '\n', { mode: 0o600 });
      received.add(event.id);
      console.log(`Webhook received ${event.name} (${event.id}); signature verified`);
    }
    res.writeHead(204).end();
  } catch {
    res.writeHead(400).end();
  }
});

async function main() {
  await new Promise((resolve, reject) => {
    receiver.once('error', reject);
    receiver.listen(PORT, '127.0.0.1', resolve);
  });
  const hooks = (await request('/api/v1/webhooks')).items;
  let webhook = hooks.find(hook => hook.event_name === EVENT_NAME && hook.url === URL);
  if (!webhook) {
    webhook = await request('/api/v1/webhooks', { event_name: EVENT_NAME, url: URL });
    state.webhook_id = webhook.id;
    state.webhook_secret = webhook.secret;
    writeFileSync(STATE_FILE, JSON.stringify(state, null, 2) + '\n', { mode: 0o600 });
    chmodSync(STATE_FILE, 0o600);
  } else if (state.webhook_id !== webhook.id || !state.webhook_secret) {
    throw new Error('The demo webhook signing secret is missing from the private seed state file');
  }
  const workers = (await request('/api/v1/events/workers')).items;
  let worker = workers.find(worker => worker.event_name === EVENT_NAME);
  if (!worker) {
    const source = `import { appendFile } from 'node:fs/promises';
export default async function(event: { id: string; name: string; data: unknown }) {
  await appendFile(${JSON.stringify(WORKER_LOG)}, JSON.stringify({ event_id: event.id, name: event.name, data: event.data, processed_at: new Date().toISOString() }) + '\\n', { mode: 0o600 });
}`;
    worker = await request('/api/v1/events/workers', { event_name: EVENT_NAME, source });
  }
  console.log(`Webhook: ${webhook.id} -> ${URL}`);
  console.log(`Event worker: ${worker.id} -> ${WORKER_LOG}`);
  const event = await request('/api/v1/events', {
    name: EVENT_NAME,
    data: { source: 'handler-seed-verification', order_id: 'demo-order-001', total: 49.95 }
  });
  const deadline = Date.now() + 10000;
  let processed = false;
  while (Date.now() < deadline) {
    processed = existsSync(WORKER_LOG) && readFileSync(WORKER_LOG, 'utf8').split('\n').some(line => {
      try { return JSON.parse(line).event_id === event.id; } catch { return false; }
    });
    if (received.has(event.id) && processed) break;
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  if (!received.has(event.id) || !processed) throw new Error('The verification event did not reach both handlers');
  console.log('Verified one test event: signed webhook delivered and TypeScript worker executed.');
  console.log(`Webhook log: ${WEBHOOK_LOG}`);
  if (LISTEN) console.log('Webhook receiver listening; no events are generated automatically. Press Ctrl+C to stop.');
  else receiver.close();
}

main().catch(error => {
  console.error(`Seed failed: ${error.message}`);
  receiver.close();
  process.exitCode = 1;
});
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => receiver.close(() => process.exit(0)));
