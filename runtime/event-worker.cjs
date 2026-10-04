'use strict';
const { stripTypeScriptTypes } = require('node:module');

async function main() {
  if (typeof stripTypeScriptTypes !== 'function') {
    throw new Error('event workers require Node 22 or newer');
  }
  const chunks = [];
  let length = 0;
  for await (const chunk of process.stdin) {
    length += chunk.length;
    if (length > 64 * 1024) throw new Error('worker input too large');
    chunks.push(chunk);
  }
  const { source, event } = JSON.parse(Buffer.concat(chunks).toString('utf8'));
  if (typeof source !== 'string' || source.length > 32 * 1024 || !event || typeof event !== 'object') {
    throw new Error('invalid worker input');
  }
  const javascript = stripTypeScriptTypes(source, { mode: 'transform' });
  const moduleUrl = 'data:text/javascript;base64,' + Buffer.from(javascript).toString('base64');
  const worker = (await import(moduleUrl)).default;
  if (typeof worker !== 'function') throw new Error('worker must export a default function');
  await worker(event);
}

main().catch((error) => {
  process.stderr.write(`event worker failed: ${error.message}\n`);
  process.exitCode = 1;
});
