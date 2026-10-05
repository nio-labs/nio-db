'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const { join } = require('node:path');
const { compileSql } = require('../runtime/sql.cjs');
const tables = { artifacts: [{ id: 'a', status: 'pending' }], sessions: [], projects: [] };

test('compiler binds string values and table data instead of interpolating them', () => {
  const attack = "'); require('node:fs').readFileSync('/etc/passwd'); --";
  const query = compileSql('SELECT id FROM artifacts WHERE status = ? ORDER BY id DESC LIMIT 5', [attack], tables);
  assert.equal(query.sql, 'SELECT [id] FROM ? AS [artifacts] WHERE [status] = ? ORDER BY [id] DESC LIMIT 5 OFFSET 0');
  assert.deepEqual(query.bindings, [tables.artifacts, attack]);
  assert.ok(!query.sql.includes(attack));
  assert.equal(compileSql("SELECT id FROM artifacts WHERE status = 'Bob''s'", [], tables).bindings[1], "Bob's");
});

test('records alias uses the authorized all-records table', () => {
  const query = compileSql('SELECT id, collection, created_at FROM records ORDER BY created_at DESC LIMIT 10', [], tables);
  assert.equal(query.bindings[0], tables.artifacts);
  assert.match(query.sql, /FROM \? AS \[records\]/);
});

test('line comments are ignored without changing quoted text', () => {
  const query = compileSql("-- inspect recent records\nSELECT id FROM records -- keep only ids\nWHERE status = '-- pending' LIMIT 5;", [], tables);
  assert.equal(query.bindings[0], tables.artifacts);
  assert.equal(query.bindings[1], '-- pending');
  assert.match(query.sql, /WHERE \[status\] = \? LIMIT 5/);
  assert.throws(() => compileSql('SELECT id FROM records; -- first query\nDELETE FROM records', [], tables), /query_rejected/);
});

test('joins, aggregates and null checks have a bounded normalized form', () => {
  const query = compileSql('SELECT s.status, COUNT(*) AS total FROM sessions s LEFT JOIN projects p ON s.project_id = p.id WHERE p.status = ? AND s.summary IS NOT NULL GROUP BY s.status ORDER BY total DESC LIMIT 5', ['active'], tables);
  assert.deepEqual(query.bindings, [tables.sessions, tables.projects, 'active']);
  assert.match(query.sql, /LEFT JOIN \? AS \[p\] ON \[s\]\.\[project_id\] = \[p\]\.\[id\]/);
  assert.equal(query.countSql, null);
});

test('compiler rejects host access, writes, multiple statements and prototype access', () => {
  for (const sql of [
    'UPDATE artifacts SET status = 1',
    'SELECT * FROM JSON(\'/etc/passwd\')',
    'SELECT require(\'fs\') FROM artifacts',
    'SELECT constructor FROM artifacts',
    'SELECT a.__proto__ FROM artifacts a',
    'SELECT * INTO JSON(\'out\') FROM artifacts',
    'SELECT * FROM artifacts; SELECT * FROM projects',
    'SELECT * FROM artifacts LIMIT 1001',
    'SELECT `process.exit()` FROM artifacts',
    'SELECT * FROM auth',
    'SELECT * FROM artifacts WHERE status = ?'
  ]) assert.throws(() => compileSql(sql, [], tables), /query_rejected/);
  assert.throws(() => compileSql('SELECT * FROM artifacts WHERE status = ?', [{}], tables), /query_rejected/);
  assert.throws(() => compileSql('SELECT * FROM artifacts', ['extra'], tables), /query_rejected/);
});

let installed = true;
try { require('../runtime/alasql.min.js'); } catch {
  try { require.resolve('alasql'); } catch { installed = false; }
}
test('actual AlaSQL helper filters authorized rows and reports matching count', { skip: installed ? false : 'AlaSQL dependency is unavailable; npm registry network access is required' }, () => {
  const result = spawnSync(process.execPath, [join(__dirname, '..', 'runtime', 'alasql.cjs')], {
    encoding: 'utf8', input: JSON.stringify({ protocol: 1, tables: { artifacts: [{id:'a',status:'pending'}, {id:'b',status:'done'}] }, sql:'SELECT id FROM artifacts WHERE status = ? LIMIT 1', parameters:['pending'], include_count:true })
  });
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(JSON.parse(result.stdout), { items:[{id:'a'}], total:1 });
});
