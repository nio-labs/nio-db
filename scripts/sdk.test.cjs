'use strict';
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { NioDB, createNioDB } = require('../packages/nio-db.js/index.js');

let server;
let baseUrl;
const recordedRequests = [];

before((_, done) => {
  server = http.createServer((req, res) => {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      let parsedBody = null;
      if (body) {
        try { parsedBody = JSON.parse(body); } catch {}
      }
      recordedRequests.push({
        method: req.method,
        url: req.url,
        headers: req.headers,
        body: parsedBody,
      });

      // Simple mock router for NioDB endpoints
      res.setHeader('Content-Type', 'application/json');

      if (req.url.startsWith('/api/v1/records/search')) {
        res.writeHead(200);
        res.end(JSON.stringify({ items: [{ id: 'rec_1', score: 0.95, data: { text: 'memory 1' } }], count: 1 }));
      } else if (req.url.startsWith('/api/v1/records/bulk')) {
        res.writeHead(200);
        res.end(JSON.stringify({
          success: true,
          inserted: parsedBody?.action === 'insert' ? (parsedBody?.records?.length || 0) : 0,
          updated: parsedBody?.action === 'update' ? (parsedBody?.records?.length || 0) : 0,
          deleted: parsedBody?.action === 'delete' ? (parsedBody?.ids?.length || 0) : 0,
          records: parsedBody?.records || [],
          deleted_ids: parsedBody?.ids || [],
        }));
      } else if (req.url.startsWith('/api/v1/records/') && req.method === 'GET') {
        res.writeHead(200);
        res.end(JSON.stringify({ id: 'rec_123', type: 'memories', data: { key: 'val' } }));
      } else if (req.url.startsWith('/api/v1/records/') && req.method === 'PATCH') {
        res.writeHead(200);
        res.end(JSON.stringify({ id: 'rec_123', ...parsedBody }));
      } else if (req.url.startsWith('/api/v1/records/') && req.method === 'DELETE') {
        res.writeHead(200);
        res.end(JSON.stringify({ success: true, deleted: true, id: 'rec_123' }));
      } else if (req.url.startsWith('/api/v1/records') && req.method === 'POST') {
        res.writeHead(200);
        res.end(JSON.stringify({ id: 'rec_new', ...parsedBody }));
      } else if (req.url.startsWith('/api/v1/records') && req.method === 'GET') {
        res.writeHead(200);
        res.end(JSON.stringify({ items: [{ id: 'rec_1' }], count: 1 }));
      } else if (req.url.startsWith('/api/v1/admin/vacuum')) {
        res.writeHead(200);
        res.end(JSON.stringify({ success: true, stats: { initial_bytes: 1000, compacted_bytes: 400, purged_expired: 5 } }));
      } else if (req.url.startsWith('/api/v1/agent/tools')) {
        res.writeHead(200);
        res.end(JSON.stringify({ tools: [{ name: 'query_database' }] }));
      } else if (req.url.includes('/turns') && req.method === 'POST') {
        res.writeHead(200);
        res.end(JSON.stringify({ success: true, session_id: 'sess_1', turn: parsedBody }));
      } else if (req.url.includes('/manifest') && req.method === 'GET') {
        res.writeHead(200);
        res.end(JSON.stringify({ session_id: 'sess_1', title: 'Test Session', manifest_text: '### Session Manifest' }));
      } else if (req.url.includes('/dead-ends') && req.method === 'POST') {
        res.writeHead(200);
        res.end(JSON.stringify({ success: true, count: 1 }));
      } else if (req.url.includes('/dead-ends') && req.method === 'GET') {
        res.writeHead(200);
        res.end(JSON.stringify({ session_id: 'sess_1', dead_ends: [{ hypothesis: 'bad idea' }], count: 1 }));
      } else if (req.url.startsWith('/api/v1/sessions') && req.method === 'POST') {
        res.writeHead(200);
        res.end(JSON.stringify({ id: 'sess_1', title: parsedBody?.title || 'Session 1' }));
      } else if (req.url.startsWith('/api/v1/sessions') && req.method === 'GET') {
        res.writeHead(200);
        res.end(JSON.stringify({ items: [{ id: 'sess_1' }], count: 1 }));
      } else if (req.url.startsWith('/api/v1/tasks') && req.method === 'POST') {
        res.writeHead(200);
        res.end(JSON.stringify({ id: 'task_1', ...parsedBody }));
      } else if (req.url.startsWith('/api/v1/tasks') && req.method === 'GET') {
        res.writeHead(200);
        res.end(JSON.stringify({ items: [{ id: 'task_1' }], count: 1 }));
      } else if (req.url.startsWith('/api/v1/tasks/task_1') && req.method === 'PATCH') {
        res.writeHead(200);
        res.end(JSON.stringify({ id: 'task_1', ...parsedBody }));
      } else if (req.url.startsWith('/api/v1/events') && req.method === 'POST') {
        res.writeHead(200);
        res.end(JSON.stringify({ success: true, event: parsedBody?.name }));
      } else if (req.url.startsWith('/api/v1/query') && req.method === 'POST') {
        res.writeHead(200);
        res.end(JSON.stringify({ items: [{ total: 42 }], count: 1 }));
      } else if (req.url.startsWith('/mcp') && req.method === 'POST') {
        res.writeHead(200);
        res.end(JSON.stringify({ jsonrpc: '2.0', id: parsedBody?.id, result: { tools: [] } }));
      } else {
        res.writeHead(404);
        res.end(JSON.stringify({ error: { message: 'Not found' } }));
      }
    });
  });

  server.listen(0, '127.0.0.1', () => {
    baseUrl = `http://127.0.0.1:${server.address().port}`;
    done();
  });
});

after((_, done) => {
  server.close(done);
});

test('nio-db.js collection CRUD, TTL and vector search', async () => {
  const dbComb = createNioDB({ url: baseUrl, clientToken: 'cli-tok', secretToken: 'sec-tok' });
  assert.equal(dbComb.token, 'cli-tok:sec-tok');

  const db = createNioDB({ url: baseUrl, token: 'secret-token', workspaceId: 'ws-main' });
  const col = db.collection('memories');

  // Insert
  const inserted = await col.insert({ text: 'Hello AI', embedding: [0.1, 0.2] }, { ttl: 3600, idempotencyKey: 'idem-123' });
  assert.equal(inserted.id, 'rec_new');
  assert.equal(inserted.data.ttl, 3600);

  // Find
  const list = await col.find({ limit: 10 });
  assert.equal(list.count, 1);

  // Get
  const item = await col.get('rec_123');
  assert.equal(item.id, 'rec_123');

  // Update
  const updated = await col.update('rec_123', { key: 'updated_val' });
  assert.equal(updated.id, 'rec_123');

  // Delete
  const deleted = await col.delete('rec_123');
  assert.equal(deleted.success, true);

  // Bulk Insert
  const bulkIns = await col.insertMany([{ name: 'A' }, { name: 'B' }]);
  assert.equal(bulkIns.inserted, 2);

  // Bulk Update
  const bulkUpd = await col.updateMany([{ id: 'rec_1', name: 'A2' }]);
  assert.equal(bulkUpd.updated, 1);

  // Bulk Delete
  const bulkDel = await col.deleteMany(['rec_1', 'rec_2']);
  assert.equal(bulkDel.deleted, 2);

  // Search Vector
  const vecResult = await col.searchVector({ vector: [0.1, 0.2], topK: 3, minScore: 0.8 });
  assert.equal(vecResult.count, 1);
  assert.equal(vecResult.items[0].score, 0.95);
});

test('nio-db.js query, vacuum and getTools', async () => {
  const db = new NioDB({ url: baseUrl });
  const queryRes = await db.query('SELECT COUNT(*) AS total FROM artifacts');
  assert.equal(queryRes.items[0].total, 42);

  const vacRes = await db.vacuum();
  assert.equal(vacRes.stats.purged_expired, 5);

  const tools = await db.getTools();
  assert.equal(tools.tools.length, 1);
});

test('nio-db.js NioBridge sessions and dead ends', async () => {
  const db = createNioDB({ url: baseUrl });
  const sessionCreated = await db.sessions.create({ title: 'Debugging Task', goal: 'Fix bug', modelTier: 'strong' });
  assert.equal(sessionCreated.id, 'sess_1');

  const sessList = await db.sessions.list();
  assert.equal(sessList.count, 1);

  const s = db.session('sess_1');
  const turnRes = await s.appendTurn({ agent: 'architect', model: 'claude-3-7-sonnet', summary: 'Analyzed AST' });
  assert.equal(turnRes.success, true);

  const manifest = await s.getManifest();
  assert.match(manifest.manifest_text, /Manifest/);

  const deadEnd = await s.logDeadEnd({ hypothesis: 'Regex parsing', reason: 'Too fragile for recursive rules' });
  assert.equal(deadEnd.success, true);

  const deadEnds = await s.getDeadEnds();
  assert.equal(deadEnds.count, 1);
});

test('nio-db.js Nio0 tasks and events', async () => {
  const db = createNioDB({ url: baseUrl });
  const task = await db.tasks.create({ title: 'Run Workflow', prompt: 'execute step 1' });
  assert.equal(task.id, 'task_1');

  const tasksList = await db.tasks.list();
  assert.equal(tasksList.count, 1);

  const updated = await db.tasks.update('task_1', { status: 'running', progress: 50 });
  assert.equal(updated.status, 'running');

  const ev = await db.events.publish('task:progress', { percent: 50 });
  assert.equal(ev.event, 'task:progress');
});

test('nio-db.js mcp client', async () => {
  const db = createNioDB({ url: baseUrl });
  const res = await db.mcp('tools/list');
  assert.equal(res.jsonrpc, '2.0');
});
