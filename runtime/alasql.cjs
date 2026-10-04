'use strict';
const { compileSql } = require('./sql.cjs');

function output(value) { process.stdout.write(JSON.stringify(value) + '\n'); }
async function main() {
  let alasql;
  try { alasql = require('alasql'); }
  catch { output({ error: 'alasql_unavailable' }); process.exitCode = 1; return; }
  if (process.argv.includes('--check')) { output({ status: 'ready' }); return; }
  const chunks = []; let length = 0;
  for await (const chunk of process.stdin) {
    length += chunk.length;
    if (length > 64 * 1024 * 1024) throw new Error('query_too_large');
    chunks.push(chunk);
  }
  const request = JSON.parse(Buffer.concat(chunks).toString('utf8'));
  if (request.protocol !== 1) throw new Error('query_rejected');
  const query = compileSql(request.sql, request.parameters || [], request.tables);
  const items = alasql(query.sql, query.bindings);
  if (!Array.isArray(items) || items.length > 1000) throw new Error('query_rejected');
  let total = null;
  if (request.include_count) {
    if (!query.countSql) throw new Error('query_rejected');
    total = alasql(query.countSql, query.bindings)[0].total;
  }
  const result = { items, total };
  if (JSON.stringify(result).length > 32 * 1024 * 1024) throw new Error('query_too_large');
  output(result);
}
main().catch((error) => {
  output({ error: error.message === 'query_too_large' ? 'query_too_large' : 'query_rejected' });
  process.exitCode = 1;
});
