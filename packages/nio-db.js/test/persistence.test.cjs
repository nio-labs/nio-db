const { test } = require('node:test');
const assert = require('node:assert/strict');
const { NioDB } = require('../index.js');
const fs = require('node:fs');
const path = require('node:path');
const { pathToFileURL } = require('node:url');

function client(handler, options = {}) {
  return new NioDB({ token: 'private-token', fetch: handler, ...options });
}
const response = value => new Response(JSON.stringify(value), { headers: { 'content-type': 'application/json' } });

test('bound queries preserve values and authenticated request shape', async () => {
  const calls = [];
  const db = client(async (url, options) => { calls.push({ url, ...options }); return response({ items: [] }); });
  await db.query('SELECT appId FROM messages WHERE conversationId = $1', ["x' OR 1=1 --"]);
  await db.query('SELECT appId FROM messages');
  assert.deepEqual(JSON.parse(calls[0].body).parameters, ["x' OR 1=1 --"]);
  assert.equal(calls[0].headers.Authorization, 'Bearer private-token');
  assert.deepEqual(JSON.parse(calls[1].body).parameters, []);
});

test('mutation and lease methods transmit exact contracts', async () => {
  const calls = [];
  const db = client(async (url, options) => { calls.push({ url, body: JSON.parse(options.body), method: options.method }); return response({}); });
  const policy = { unique: ['appId'], references: [], lease: { field: 'conversationId', prefix: 'conversation:' } };
  const proof = { resource: 'conversation:c', owner: 'run', fence: 3 };
  const transaction = { idempotency_key: 'run:final', operations: [{ action: 'create', collection: 'messages', data: { appId: 'm' } }], leases: [proof] };
  await db.configureCollection('messages', policy);
  await db.mutate(transaction);
  await db.acquireLease(proof.resource, proof.owner, 1000);
  await db.renewLease(proof, 2000);
  await db.releaseLease(proof);
  assert.equal(calls[0].method, 'PUT');
  assert.deepEqual(calls[0].body, policy);
  assert.deepEqual(calls[1].body, transaction);
  assert.deepEqual(calls.slice(2).map(c => c.body.action), ['acquire', 'renew', 'release']);
  assert.equal(calls[3].body.fence, 3);
});

test('stable HTTP errors carry status, code and request ID', async () => {
  const db = client(async () => new Response(JSON.stringify({ error: { code: 'lease_conflict', message: 'Expired' }, request_id: 'req-1' }), { status: 409 }));
  await assert.rejects(db.query('SELECT * FROM messages'), error => error.status === 409 && error.code === 'lease_conflict' && error.requestId === 'req-1');
});

test('streamed responses enforce byte bounds and cancel the reader', async () => {
  let cancelled = false;
  const db = client(async () => new Response(new ReadableStream({
    pull(controller) { controller.enqueue(new Uint8Array(64)); },
    cancel() { cancelled = true; },
  })), { maxResponseBytes: 32 });
  await assert.rejects(db.query('SELECT * FROM messages'), /byte limit/);
  assert.equal(cancelled, true);
});

test('deadline and caller cancellation reach fetch and remove listeners', async () => {
  const pending = (_, { signal }) => new Promise((_, reject) => {
    if (signal.aborted) reject(signal.reason);
    else signal.addEventListener('abort', () => reject(signal.reason), { once: true });
  });
  const db = client(pending, { timeoutMs: 10 });
  await assert.rejects(db.query('SELECT * FROM messages'), /deadline exceeded/);
  const controller = new AbortController();
  const promise = db.query('SELECT * FROM messages', [], { signal: controller.signal, timeoutMs: 1000 });
  controller.abort(new Error('caller cancelled'));
  await assert.rejects(promise, /caller cancelled/);
});

test('generated ESM contains no CommonJS imports and behaves like the CJS client', async () => {
  const source = fs.readFileSync(path.join(__dirname, '../index.js'), 'utf8');
  const built = fs.readFileSync(path.join(__dirname, '../index.mjs'), 'utf8');
  assert.equal(built, source.replace('module.exports = {\n  NioDB,\n  createNioDB,\n};', 'export { NioDB, createNioDB };'));
  const esm = await import(pathToFileURL(path.join(__dirname, '../index.mjs')).href);
  const db = new esm.NioDB({ fetch: async () => response({ items: [{ appId: 'm' }] }) });
  assert.deepEqual(await db.query('SELECT appId FROM messages'), { items: [{ appId: 'm' }] });
});
