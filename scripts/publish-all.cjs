#!/usr/bin/env node
'use strict';
const { execSync } = require('node:child_process');
const { resolve, join } = require('node:path');
const { existsSync } = require('node:fs');
const root = resolve(__dirname, '..');

function run(cmd, cwd) {
  console.log(`\n==> [${cwd}] ${cmd}`);
  execSync(cmd, { cwd, stdio: 'inherit' });
}

console.log('=============================================');
console.log('  Publishing NioDB packages to npm');
console.log('=============================================');

// 1. Publish JS SDK
const jsSdkDir = join(root, 'packages', 'nio-db.js');
if (existsSync(jsSdkDir)) {
  console.log('\n--- 1. Publishing @nio-labs/nio-db.js ---');
  try {
    run('npm publish --access public', jsSdkDir);
  } catch (e) {
    console.error(`Failed to publish @nio-labs/nio-db.js: ${e.message}`);
  }
}

// 2. Package current platform binary
console.log('\n--- 2. Packaging platform binary ---');
run('node scripts/package-platform.cjs', root);

// 3. Publish current platform binary
const distPlatform = join(root, 'dist', `nio-db-${process.platform}-${process.arch}`);
if (existsSync(distPlatform)) {
  console.log('\n--- 3. Publishing platform package ---');
  try {
    run('npm publish --access public', distPlatform);
  } catch (e) {
    console.error(`Failed to publish platform package: ${e.message}`);
  }
}

// 4. Publish main launcher package
console.log('\n--- 4. Publishing main package @nio-labs/nio-db ---');
try {
  run('npm publish --access public', root);
} catch (e) {
  console.error(`Failed to publish @nio-labs/nio-db: ${e.message}`);
}

console.log('\nAll publish commands executed.');
