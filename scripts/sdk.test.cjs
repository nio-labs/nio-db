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
      } else if (req.url.includes('/switch') && req.method === 'POST') {
        res.writeHead(200);
        res.end(JSON.stringify({
          success: true,
          session_id: 'sess_1',
          previous_agent: 'agy',
          active_agent: parsedBody?.to_agent || 'codex',
          reason: parsedBody?.reason || 'Handoff',
          manifest: { session_id: 'sess_1', manifest_text: '### Manifest after switch' }
        }));
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
      } else if (req.url.startsWith('/api/v1/sessions/') && req.method === 'PATCH') {
        res.writeHead(200);
        res.end(JSON.stringify({ id: 'sess_1', ...parsedBody }));
      } else if (req.url.startsWith('/api/v1/sessions/') && req.method === 'GET') {
        res.writeHead(200);
        res.end(JSON.stringify({ id: 'sess_1', title: 'Session 1', data: { title: 'Session 1', active_agent: 'agy' } }));
      } else if (req.url.startsWith('/api/v1/sessions') && req.method === 'POST') {
        res.writeHead(200);
        res.end(JSON.stringify({ id: 'sess_1', title: parsedBody?.title || 'Session 1' }));
      } else if (req.url.startsWith('/api/v1/sessions') && req.method === 'GET') {
        res.writeHead(200);
        res.end(JSON.stringify({ items: [{ id: 'sess_1' }], count: 1 }));
      } else if (req.url.startsWith('/api/v1/tasks/') && req.method === 'PATCH') {
        res.writeHead(200);
        res.end(JSON.stringify({ id: 'task_1', ...parsedBody, data: { ...parsedBody } }));
      } else if (req.url.startsWith('/api/v1/tasks') && req.method === 'POST') {
        res.writeHead(200);
        res.end(JSON.stringify({ id: 'task_1', ...parsedBody }));
      } else if (req.url.startsWith('/api/v1/tasks') && req.method === 'GET') {
        res.writeHead(200);
        res.end(JSON.stringify({ items: [{ id: 'task_1', data: { status: 'pending' } }], count: 1 }));
      } else if (req.url.startsWith('/api/v1/events') && req.method === 'POST') {
        res.writeHead(200);
        res.end(JSON.stringify({ success: true, event: parsedBody?.name }));
      } else if (req.url.startsWith('/api/v1/query') && req.method === 'POST') {
        res.writeHead(200);
        res.end(JSON.stringify({ items: [{ total: 42 }], count: 1 }));
      } else if (req.url.startsWith('/api/v1/ledger/verify') && req.method === 'GET') {
        res.writeHead(200);
        res.end(JSON.stringify({ verified: true, total_frames: 42, root_hash: '9f8a3c2e', latest_seq: 42, tampering_detected: false }));
      } else if (req.url.startsWith('/api/v1/ledger/root') && req.method === 'GET') {
        res.writeHead(200);
        res.end(JSON.stringify({ root_hash: '9f8a3c2e', latest_seq: 42 }));
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

test('nio-db.js ledger client', async () => {
  const db = createNioDB({ url: baseUrl });
  const report = await db.ledger.verify();
  assert.equal(report.verified, true);
  assert.equal(report.tampering_detected, false);
  assert.equal(report.latest_seq, 42);

  const root = await db.ledger.root();
  assert.equal(root.root_hash, '9f8a3c2e');
  assert.equal(root.latest_seq, 42);
});

test('nio-db.js NioBridge multi-agent switching and adapters', async () => {
  const db = createNioDB({ url: baseUrl });

  // 1. Create Bridge Session
  const session = await db.bridge.create({
    title: 'Bridge Integration Session',
    goal: 'Hot-swap agents with 0 memory loss',
    agent: 'agy',
    persona: 'architect',
  });
  assert.equal(session.id, 'sess_1');

  // 2. Append Turn
  const turn = await db.bridge.appendTurn(session.id, {
    agent: 'agy',
    model: 'gemini-2.5-pro',
    summary: 'Designed modular architecture',
    filesTouched: ['src/api.rs'],
  });
  assert.equal(turn.success, true);

  // 3. Log Dead-End
  const deadEnd = await db.bridge.logDeadEnd(session.id, {
    hypothesis: 'Single mega-prompt',
    reason: 'Context overflow',
    agent: 'agy',
  });
  assert.equal(deadEnd.success, true);

  // 4. Switch Agent
  const switchRes = await db.bridge.switch(session.id, {
    toAgent: 'codex',
    reason: 'Implement backend in Rust',
    persona: 'rust-specialist',
  });
  assert.equal(switchRes.success, true);
  assert.equal(switchRes.active_agent, 'codex');
  assert.ok(switchRes.manifest);

  // 5. Test Agent Adapters
  const manifest = await db.bridge.getManifest(session.id);
  const promptAgy = db.bridge.adapters.agy.formatPrompt(manifest, 'Continue task');
  assert.ok(promptAgy.includes('[NioBridge Manifest]'));
  assert.ok(promptAgy.includes('Continue task'));

  const promptClaude = db.bridge.adapters.claude.formatPrompt(manifest, 'Refactor code');
  assert.ok(promptClaude.includes('<nio_bridge_manifest>'));
  assert.ok(promptClaude.includes('Refactor code'));

  const promptCodex = db.bridge.adapters.codex.formatPrompt(manifest, 'Write tests');
  assert.ok(promptCodex.includes('/* NIO_BRIDGE_MANIFEST'));
  assert.ok(promptCodex.includes('Write tests'));

  const promptCursor = db.bridge.adapters.cursor.formatPrompt(manifest, 'Inspect diff');
  assert.ok(promptCursor.includes('# CONTEXT FROM NIOBRIDGE'));
});

test('nio-db.js NioAssemble swarm coordination and status', async () => {
  const db = createNioDB({ url: baseUrl });

  // 1. Create Assemble Swarm
  const swarm = await db.assemble.create({
    title: 'Assemble Refactor Swarm',
    goal: 'Deliver Merkle V2 and Bridge',
    agents: {
      planner: 'agy',
      coder: 'codex',
      tester: 'claude',
    },
  });
  assert.ok(swarm.sessionId);
  assert.equal(swarm.agents.planner, 'agy');

  // 2. Plan and decompose tasks
  const plan = await db.assemble.plan(swarm.sessionId, 'Step-by-step refactor plan', [
    { title: 'Update Schema', prompt: 'Add session routes', role: 'coder', assignedAgent: 'codex' },
    { title: 'Write Tests', prompt: 'Verify session routes', role: 'tester', assignedAgent: 'claude' },
  ]);
  assert.equal(plan.count, 2);
  assert.equal(plan.tasks.length, 2);

  // 3. Claim and update task
  const claimed = await db.assemble.claimTask('task_1', 'codex');
  assert.equal(claimed.assigned_agent, 'codex');
  assert.equal(claimed.status, 'in_progress');

  // 4. Complete task
  const completed = await db.assemble.completeTask('task_1', { diff: 'added routes' });
  assert.equal(completed.status, 'completed');
  assert.equal(completed.progress, 100);

  // 5. Fail task
  const failed = await db.assemble.failTask('task_1', new Error('Timeout'));
  assert.equal(failed.status, 'failed');

  // 6. Check Aggregate Status
  const status = await db.assemble.status(swarm.sessionId);
  assert.equal(status.sessionId, swarm.sessionId);
  assert.ok(status.totalTasks >= 1);
});


