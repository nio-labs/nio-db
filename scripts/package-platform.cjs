'use strict';
const { copyFileSync, mkdirSync, readFileSync, writeFileSync, chmodSync, statSync } = require('node:fs');
const { createHash } = require('node:crypto');
const { resolve, join } = require('node:path');
const root = resolve(__dirname, '..');
const version = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8')).version;
const [binaryArg, platform = process.platform, arch = process.arch] = process.argv.slice(2);
if (!['linux', 'darwin', 'win32'].includes(platform) || !['x64', 'arm64'].includes(arch)) {
  throw new Error(`Unsupported target ${platform}/${arch}`);
}
const binary = resolve(binaryArg || join(root, 'target', 'release', platform === 'win32' ? 'niodb.exe' : 'niodb'));
if (!statSync(binary).isFile()) throw new Error('Expected a compiled server executable');
const directory = join(root, 'dist', `nio-db-${platform}-${arch}`);
mkdirSync(directory, { recursive: true });
const filename = platform === 'win32' ? 'niodb.exe' : 'niodb';
copyFileSync(binary, join(directory, filename));
chmodSync(join(directory, filename), 0o755);
writeFileSync(join(directory, 'package.json'), JSON.stringify({
  name: `@nio-labs/nio-db-${platform}-${arch}`, version,
  description: 'Platform executable for NioDB — The Agentic DB that works.', license: 'UNLICENSED',
  os: [platform], cpu: [arch], files: [filename],
  niodbSha256: createHash('sha256').update(readFileSync(binary)).digest('hex'),
  publishConfig: { access: 'public' }
}, null, 2) + '\n');
console.log(directory);
