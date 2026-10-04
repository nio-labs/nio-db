'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const { mkdtempSync, writeFileSync, chmodSync, rmSync } = require('node:fs');
const { tmpdir } = require('node:os');
const { join, resolve } = require('node:path');
const launcher = resolve(__dirname, '..', 'bin', 'niodb.cjs');

test('launcher preserves literal arguments and child exit codes', { skip: process.platform === 'win32' }, () => {
  const directory = mkdtempSync(join(tmpdir(), 'niodb-launcher-'));
  try {
    const binary = join(directory, 'fake-server');
    writeFileSync(binary, '#!/usr/bin/env node\nconsole.log(JSON.stringify(process.argv.slice(2))); process.exit(7);\n');
    chmodSync(binary, 0o755);
    const args = ['--dir', 'path with spaces', '$(touch should-not-run)', '`ignored`'];
    const result = spawnSync(process.execPath, [launcher, ...args], { encoding: 'utf8', env: { ...process.env, NIODB_BIN: binary } });
    assert.equal(result.status, 7);
    assert.deepEqual(JSON.parse(result.stdout), args);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('missing executable produces a failing exit status', () => {
  const result = spawnSync(process.execPath, [launcher], { encoding: 'utf8', env: { ...process.env, NIODB_BIN: join(tmpdir(), 'niodb-nonexistent-executable') } });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /niodb:/);
});
