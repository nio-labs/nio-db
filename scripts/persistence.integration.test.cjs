'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { randomBytes, createHash } = require('node:crypto');
const { NioDB } = require('../packages/nio-db.js');

const binary = process.env.NIODB_TEST_BINARY;
test('real server + SDK atomic storage, guards, bound queries and persistent restart', { skip: !binary, timeout: 30000 }, async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'niodb-persistence-'));
  const token = randomBytes(32).toString('hex');
  const data = path.join(directory, 'data');
  fs.mkdirSync(data, { mode: 0o700 });
  fs.writeFileSync(path.join(data, 'auth.json'), JSON.stringify({ principals: [{ name: 'app', token_sha256: createHash('sha256').update(token).digest('hex'), workspaces: ['default'], nio_skills: [], nio_plugins: [] }] }), { mode: 0o600 });
  const config = path.join(directory, 'nio-config.json');
  fs.writeFileSync(config, '{}', { mode: 0o600 });
  let child;
  async function stop() {
    if (!child || child.exitCode !== null) return;
    const exited = once(child, 'exit');
    child.kill('SIGTERM');
    await exited;
    child = null;
  }
  async function start() {
    child = spawn(path.resolve(binary), ['serve', '--dir', data, '--listen', '127.0.0.1:0', '--no-pg', '--no-demo', '--nio-bin', '/usr/bin/false', '--node-bin', '/usr/bin/false'], { env: { ...process.env, NIO_CONFIG: config }, stdio: ['ignore', 'ignore', 'pipe'] });
    const url = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Server startup deadline exceeded')), 5000);
      let logs = '';
      const onExit = () => { clearTimeout(timer); reject(new Error('Server exited before readiness: ' + logs.replaceAll(token, '[redacted]'))); };
      child.once('exit', onExit);
      child.once('error', error => { clearTimeout(timer); reject(error); });
      child.stderr.on('data', chunk => {
        logs += chunk.toString();
        const match = logs.match(/Server\s+(http:\/\/127\.0\.0\.1:\d+)/);
        if (match) { clearTimeout(timer); child.removeListener('exit', onExit); resolve(match[1]); }
      });
    });
    return new NioDB({ url, token, timeoutMs: 3000 });
  }
  try {
    let db = await start();
    await db.configureCollection('nioguru_gurus', { unique: ['appId'] });
    await db.configureCollection('nioguru_conversations', { unique: ['appId'], references: [{ field: 'guruId', collection: 'nioguru_gurus', target_field: 'appId' }], lease: { field: 'appId', prefix: 'conversation:' } });
    await db.configureCollection('nioguru_messages', { unique: ['appId'], references: [{ field: 'conversationId', collection: 'nioguru_conversations', target_field: 'appId' }], lease: { field: 'conversationId', prefix: 'conversation:' } });
    const { lease } = await db.acquireLease('conversation:c', 'run');
    const { resource, owner, fence } = lease;
    const proof = { resource, owner, fence };
    assert.equal((await db.getLease(resource)).active, true);
    const seed = { idempotency_key: 'seed', operations: [
      { action: 'create', collection: 'nioguru_gurus', data: { appId: 'g', skills: [], isPinned: true } },
      { action: 'create', collection: 'nioguru_conversations', data: { appId: 'c', guruId: 'g' } },
      { action: 'create', collection: 'nioguru_messages', data: { appId: 'm', conversationId: 'c', createdAt: 1000, content: 'x'.repeat(400000), attachments: [], toolCalls: [] } },
    ], leases: [proof] };
    const result = await db.mutate(seed);
    assert.equal(result.records.length, 3);
    assert.deepEqual(await db.mutate(seed), result);
    const page = await db.query('SELECT appId, createdAt, attachments FROM nioguru_messages WHERE conversationId = $1 ORDER BY createdAt DESC, appId DESC LIMIT 31', ['c']);
    assert.equal(page.engine, 'native');
    assert.deepEqual(page.items, [{ appId: 'm', createdAt: 1000, attachments: [] }]);
    await assert.rejects(db.mutate({ idempotency_key: 'duplicate', operations: [
      { action: 'create', collection: 'nioguru_gurus', data: { appId: 'temporary' } },
      { action: 'create', collection: 'nioguru_gurus', data: { appId: 'g' } },
    ] }), e => e.code === 'unique_conflict');
    assert.deepEqual((await db.query('SELECT appId FROM nioguru_gurus ORDER BY appId')).items, [{ appId: 'g' }]);
    const conversation = result.records[1];
    await assert.rejects(db.collection('nioguru_conversations').delete(conversation.id), e => e.code === 'lease_required');
    await stop();
    db = await start();
    assert.deepEqual(await db.mutate(seed), result);
    assert.equal((await db.collection('nioguru_messages').get(result.records[2].id)).data.content.length, 400000);
    await db.releaseLease(lease);
    const { lease: next } = await db.acquireLease(resource, 'next-run');
    assert.ok(next.fence > fence);
    await assert.rejects(db.mutate({ idempotency_key: 'stale', operations: [{ action: 'update', id: conversation.id, expected_revision: 1, data: { title: 'stale' } }], leases: [proof] }), e => e.code === 'lease_conflict');
    const nextProof = { resource: next.resource, owner: next.owner, fence: next.fence };
    await db.mutate({ idempotency_key: 'cascade', operations: result.records.map(record => ({ action: 'delete', id: record.id, expected_revision: record.revision })), leases: [nextProof] });
    // An acknowledged create retry after deletion returns its receipt without resurrection.
    assert.deepEqual(await db.mutate(seed), result);
    assert.deepEqual((await db.query('SELECT appId FROM nioguru_messages')).items, []);
    const status = await db.getPersistenceStatus();
    assert.equal(typeof status.active_receipts, 'number');
    const { lease: cleanupLease } = await db.acquireLease('conversation:cleanup', 'cleanup-run');
    const cleanup = await db.mutate({ idempotency_key: 'cleanup-seed', operations: [
      { action: 'create', collection: 'nioguru_gurus', data: { appId: 'cleanup-g' } },
      { action: 'create', collection: 'nioguru_conversations', data: { appId: 'cleanup', guruId: 'cleanup-g' } },
      { action: 'create', collection: 'nioguru_messages', data: { appId: 'cleanup-m', conversationId: 'cleanup' } },
    ], leases: [{ resource: cleanupLease.resource, owner: cleanupLease.owner, fence: cleanupLease.fence }] });
    await db.releaseLease(cleanupLease);
    const started = await db.startDeletionJob(cleanup.records[0].id, 'cleanup-job');
    assert.equal(started.job.total, 3);
    let job = started.job;
    for (let attempt = 0; attempt < 100 && job.status === 'pending'; attempt++) {
      await new Promise(resolve => setTimeout(resolve, 10));
      job = (await db.getDeletionJob(job.id)).job;
    }
    assert.equal(job.status, 'complete');
    assert.equal(job.deleted, 3);
    assert.equal((await db.startDeletionJob(cleanup.records[0].id, 'cleanup-job')).job.id, job.id);
    assert.deepEqual((await db.query('SELECT appId FROM nioguru_messages')).items, []);
  } finally {
    await stop();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
