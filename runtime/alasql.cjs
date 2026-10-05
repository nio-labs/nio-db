'use strict';
const { compileSql } = require('./sql.cjs');
const MAX_INPUT = 64 * 1024 * 1024;
const MAX_OUTPUT = 4 * 1024 * 1024;
const persistent = process.argv.includes('--worker');
function output(value) {
  let body = Buffer.from(JSON.stringify(value));
  if (body.length > MAX_OUTPUT) body = Buffer.from('{"error":"query_too_large"}');
  if (persistent) {
    const header = Buffer.alloc(4);
    header.writeUInt32BE(body.length);
    process.stdout.write(header);
    process.stdout.write(body);
  } else { process.stdout.write(body.toString() + '\n'); }
}
function execute(alasql, request) {
  let tables;
  if (request.protocol === 2) {
    if (!Array.isArray(request.records) || request.records.length > 500000) throw new Error('query_too_large');
    tables = Object.create(null);
    tables.artifacts = request.records;
    for (const row of request.records) {
      const kind = row.collection;
      if (typeof kind !== 'string' || ['__proto__', 'constructor', 'prototype', 'artifacts', 'records'].includes(kind)) continue;
      (tables[kind] ||= []).push(row);
    }
  } else if (request.protocol === 1) { tables = request.tables; }
  else { throw new Error('query_rejected'); }
  const query = compileSql(request.sql, request.parameters || [], tables);
  const items = alasql(query.sql, query.bindings);
  if (!Array.isArray(items) || items.length > 1000) throw new Error('query_rejected');
  let total = null;
  if (request.include_count) {
    if (!query.countSql) throw new Error('query_rejected');
    total = alasql(query.countSql, query.bindings)[0].total;
  }
  const result = { items, total };
  return result;
}
function handle(alasql, bytes) {
  try { output(execute(alasql, JSON.parse(bytes.toString('utf8')))); }
  catch (error) { output({ error: error.message === 'query_too_large' ? 'query_too_large' : 'query_rejected' }); if (!persistent) process.exitCode = 1; }
}
async function main() {
  let alasql;
  try { alasql = require('./alasql.min.js'); }
  catch { try { alasql = require('alasql'); } catch { output({ error: 'alasql_unavailable' }); process.exitCode = 1; return; } }
  // Compiled query state may hold bindings from earlier callers. Disable that
  // cache so data does not survive between independently authorized requests.
  alasql.options.cache = false;
  if (process.argv.includes('--check')) { output({ status: 'ready' }); return; }
  if (!persistent) {
    const chunks = []; let size = 0;
    for await (const chunk of process.stdin) {
      size += chunk.length;
      if (size > MAX_INPUT) throw new Error('query_too_large');
      chunks.push(chunk);
    }
    handle(alasql, Buffer.concat(chunks, size));
    return;
  }
  output({ status: 'ready', protocol: 2 });
  const header = Buffer.alloc(4);
  let headerSize = 0, expected = null, size = 0, chunks = [];
  for await (const chunk of process.stdin) {
    let offset = 0;
    while (offset < chunk.length) {
      if (expected === null) {
        const take = Math.min(4 - headerSize, chunk.length - offset);
        chunk.copy(header, headerSize, offset, offset + take);
        headerSize += take; offset += take;
        if (headerSize < 4) continue;
        expected = header.readUInt32BE();
        if (expected > MAX_INPUT || expected === 0) throw new Error('query_too_large');
      }
      const take = Math.min(expected - size, chunk.length - offset);
      if (take) { chunks.push(chunk.subarray(offset, offset + take)); size += take; offset += take; }
      if (size === expected) {
        handle(alasql, Buffer.concat(chunks, size));
        headerSize = 0; expected = null; size = 0; chunks = [];
      }
    }
  }
  if (headerSize || expected !== null) throw new Error('query_rejected');
}
main().catch(error => { output({ error: error.message === 'query_too_large' ? 'query_too_large' : 'query_rejected' }); process.exitCode = 1; });
