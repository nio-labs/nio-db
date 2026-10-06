#!/usr/bin/env node
'use strict';

const { spawn, execFileSync } = require('node:child_process');
const { existsSync, readFileSync, openSync, writeFileSync, fsyncSync, closeSync, lstatSync, renameSync, unlinkSync, mkdirSync, chmodSync } = require('node:fs');
const { createHash, randomBytes } = require('node:crypto');
const { join, resolve, dirname, delimiter, isAbsolute } = require('node:path');
const { homedir } = require('node:os');
const { createInterface } = require('node:readline/promises');
const { isIP } = require('node:net');

const settingsPath = resolve('.nio-db.json');

function validListen(value) {
  const match = /^(?:\[([^\]]+)\]|([^:]+)):(\d+)$/.exec(value);
  return Boolean(match && isIP(match[1] || match[2]) && Number(match[3]) <= 65535);
}

function loadSettings(args) {
  if (!existsSync(settingsPath)) return;
  const metadata = lstatSync(settingsPath);
  if (!metadata.isFile() || metadata.size > 4096) throw new Error('Invalid .nio-db.json settings file.');
  const settings = JSON.parse(readFileSync(settingsPath, 'utf8'));
  if (settings.version !== 1 || typeof settings.dir !== 'string' || !isAbsolute(settings.dir) || !validListen(settings.listen)) {
    throw new Error('Invalid .nio-db.json settings; expected version, absolute dir and IP:port listen address.');
  }
  if (!option(args, '--dir') && !process.env.NIODB_DIR) args.push('--dir', settings.dir);
  if (!option(args, '--listen') && !process.env.NIODB_LISTEN) args.push('--listen', settings.listen);
}

async function firstRun(args, acceptDefaults) {
  const directory = option(args, '--dir') || process.env.NIODB_DIR || './nio-db';
  const customAuth = option(args, '--auth-file') || process.env.NIODB_AUTH_FILE;
  if (customAuth || existsSync(join(resolve(directory), 'auth.json'))) return;
  if (acceptDefaults) return;
  if (!process.stdin.isTTY || !process.stderr.isTTY) {
    throw new Error('First-run setup needs an interactive terminal. Run nio-db init-auth first, or use --yes to create client and secret credentials with defaults.');
  }
  console.error('\n  Welcome to NioDB');
  console.error('  Let\'s set up your database. Press Enter to accept a default.\n');
  const controller = new AbortController();
  const prompt = createInterface({ input: process.stdin, output: process.stderr });
  prompt.once('SIGINT', () => prompt.close());
  prompt.once('close', () => controller.abort());
  const ask = async (label, fallback) => {
    const answer = await prompt.question(`  ${label} [${fallback}]: `, { signal: controller.signal });
    return answer.trim() || fallback;
  };
  try {
    if (!option(args, '--dir') && !process.env.NIODB_DIR) args.push('--dir', resolve(await ask('Data directory', directory)));
  } finally { prompt.close(); }
  return { version: 1, dir: resolve(option(args, '--dir') || directory), listen: option(args, '--listen') || process.env.NIODB_LISTEN || '127.0.0.1:7432' };
}

function executable() {
  if (process.env.NIODB_BIN) return resolve(process.env.NIODB_BIN);
  const platform = process.platform;
  const arch = process.arch;
  if (!['linux', 'darwin', 'win32'].includes(platform) || !['x64', 'arm64'].includes(arch)) {
    throw new Error(`Unsupported platform ${platform}/${arch}. Build NioDB with Cargo and set NIODB_BIN.`);
  }
  const name = `@nio-labs/nio-db-${platform}-${arch}`;
  let manifestPath;
  try { manifestPath = require.resolve(`${name}/package.json`); }
  catch {
    // Development checkout only. Cargo build artifacts are excluded from npm packages.
    const localRelease = join(__dirname, '..', 'target', 'release', platform === 'win32' ? 'niodb.exe' : 'niodb');
    if (existsSync(localRelease)) return localRelease;
    const localDebug = join(__dirname, '..', 'target', 'debug', platform === 'win32' ? 'niodb.exe' : 'niodb');
    const cacheDir = join(homedir(), '.niodb', 'bin');
    const asset = platform === 'win32' ? `niodb-${platform}-${arch}.exe` : `niodb-${platform}-${arch}`;
    const cachedBinary = join(cacheDir, asset);
    if (existsSync(cachedBinary)) {
      try { chmodSync(cachedBinary, 0o755); return cachedBinary; } catch {}
    }
    try {
      mkdirSync(cacheDir, { recursive: true });
      const version = JSON.parse(readFileSync(join(__dirname, '..', 'package.json'), 'utf8')).version;
      const url = `https://github.com/nio-labs/nio-db/releases/download/v${version}/${asset}`;
      console.error(`Downloading NioDB native binary (${asset})...`);
      execFileSync('curl', ['-sSL', '-f', url, '-o', cachedBinary], { stdio: 'inherit' });
      chmodSync(cachedBinary, 0o755);
      if (existsSync(cachedBinary)) return cachedBinary;
    } catch {}
    throw new Error(`Missing ${name}. Install with optional dependencies enabled, or build from source and set NIODB_BIN.`);
  }
  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
  const binary = join(dirname(manifestPath), platform === 'win32' ? 'niodb.exe' : 'niodb');
  const digest = createHash('sha256').update(readFileSync(binary)).digest('hex');
  if (digest !== manifest.niodbSha256) throw new Error('NioDB executable checksum failed. Reinstall the package.');
  return binary;
}

function option(args, name) {
  const index = args.lastIndexOf(name);
  return index < 0 ? undefined : args[index + 1];
}

function startupCommand(args) {
  const values = new Set(['--dir', '--listen', '--auth-file', '--ledger-checkpoint', '--nio-bin', '--nio-timeout', '--node-bin', '--alasql-helper', '--name', '--workspace', '--skill', '--plugin', '--output']);
  let command = 'serve';
  for (let index = 0; index < args.length; index++) {
    const arg = args[index];
    if (values.has(arg)) {
      if (args[++index] === undefined) return false;
    } else if (['--help', '-h', '--version', '-V'].includes(arg)) {
      return false;
    } else if (['--include-files', '--seed-demo', '--no-demo'].includes(arg)) {
      continue;
    } else if (['serve', 'init-auth', 'add-secret', 'backup', 'mcp'].includes(arg)) {
      if (arg !== 'serve') command = arg;
    } else { return false; }
  }
  return command === 'serve';
}

function isNio(binary) {
  try {
    const output = execFileSync(binary, ['--version'], {
      encoding: 'utf8', timeout: 5000, maxBuffer: 4096, stdio: ['ignore', 'pipe', 'ignore']
    });
    return /^nio \d+\.\d+\.\d+\b/i.test(output.trim());
  } catch { return false; }
}

async function ensureNio(args) {
  const explicit = option(args, '--nio-bin') || process.env.NIODB_NIO_BIN;
  if (explicit) {
    if (!isNio(explicit)) throw new Error('Configured Nio executable is unavailable or invalid. Fix --nio-bin / NIODB_NIO_BIN.');
    return explicit;
  }
  const filename = process.platform === 'win32' ? 'nio.exe' : 'nio';
  const candidates = (process.env.PATH || '').split(delimiter)
    .filter(Boolean).map(directory => resolve(directory, filename));
  candidates.push(join(homedir(), '.local', 'bin', filename));
  for (const candidate of candidates) {
    if (existsSync(candidate) && isNio(candidate)) return candidate;
  }
  if (process.env.NIODB_NO_INSTALL_NIO === '1') {
    console.error('\n  Nio is not installed. Automatic installation is disabled.');
    return undefined;
  }
  console.error('\n  Installing Nio for natural-language queries...');
  let installer;
  try { installer = require('@nio-labs/nio-ai/bin/nio.js').ensureBinary; }
  catch { throw new Error('Nio installer dependency is missing. Run npm install --omit=optional in a source checkout, or reinstall @nio-labs/nio-db.'); }
  if (typeof installer !== 'function') throw new Error('The installed Nio launcher does not expose ensureBinary; use --nio-bin with a native Nio installation.');
  const binary = await installer();
  if (!isAbsolute(binary) || !isNio(binary)) throw new Error('Nio installation did not provide a valid native executable.');
  console.error('  Nio installed. Configure its provider and model to enable natural-language queries.');
  console.error(`  Nio executable: ${binary}`);
  return binary;
}

function saveToken(path, token) {
  const descriptor = openSync(path, 'wx', 0o600);
  try { writeFileSync(descriptor, token + '\n'); fsyncSync(descriptor); }
  finally { closeSync(descriptor); }
}

function replaceToken(path, token) {
  const temporary = `${path}.${randomBytes(8).toString('hex')}.tmp`;
  try {
    saveToken(temporary, token);
    renameSync(temporary, path);
  } catch (error) {
    if (existsSync(temporary)) unlinkSync(temporary);
    throw error;
  }
}

function tokenMatchesAuth(authPath, tokenPath) {
  const metadata = lstatSync(tokenPath);
  if (!metadata.isFile() || metadata.size > 128) throw new Error('Invalid existing token file.');
  const token = readFileSync(tokenPath, 'utf8').trim();
  if (!/^niodb_(?:client_|secret_)?[a-f0-9]{64}$/.test(token)) throw new Error('Invalid existing token.');
  const digest = createHash('sha256').update(token).digest('hex');
  const auth = JSON.parse(readFileSync(authPath, 'utf8'));
  if (!Array.isArray(auth.principals) || !auth.principals.some(item => item.token_sha256 === digest)) {
    throw new Error('Existing token does not match auth.json. Restore the matching credentials.');
  }
}

function bootstrapAuth(binary, args) {
  const data = resolve(option(args, '--dir') || process.env.NIODB_DIR || 'nio-db');
  const customAuth = option(args, '--auth-file') || process.env.NIODB_AUTH_FILE;
  const auth = customAuth ? resolve(customAuth) : join(data, 'auth.json');
  const clientPath = join(data, 'client-token');
  const secretPath = join(data, 'secret-token');
  const legacyPath = join(data, 'dev-token');
  if (existsSync(auth)) {
    if (customAuth) return false;
    if (existsSync(legacyPath) && !existsSync(clientPath)) {
      tokenMatchesAuth(auth, legacyPath);
      renameSync(legacyPath, clientPath);
    }
    if (existsSync(clientPath)) tokenMatchesAuth(auth, clientPath);
    if (existsSync(secretPath)) tokenMatchesAuth(auth, secretPath);
    if (existsSync(clientPath) && !existsSync(secretPath)) {
      const secret = execFileSync(binary, ['add-secret', '--dir', data], {
        encoding: 'utf8', timeout: 10000, maxBuffer: 8192, stdio: ['ignore', 'pipe', 'pipe']
      }).trim();
      if (!/^niodb_secret_[a-f0-9]{64}$/.test(secret)) throw new Error('Secret initialization returned an invalid token.');
      saveToken(secretPath, secret);
      return true;
    }
    return false;
  }
  if (customAuth) throw new Error('Configured credential file is missing. Create it with init-auth before serving.');
  if ([clientPath, secretPath, legacyPath].some(existsSync)) throw new Error('A token exists without auth.json; restore credentials or choose a fresh --dir.');
  const initArgs = ['init-auth', '--dir', data, '--name', option(args, '--name') || 'app'];
  for (let index = 0; index < args.length; index++) {
    if (['--workspace', '--skill', '--plugin'].includes(args[index])) initArgs.push(args[index], args[++index]);
  }
  const output = execFileSync(binary, initArgs, {
    encoding: 'utf8', timeout: 10000, maxBuffer: 8192, stdio: ['ignore', 'pipe', 'pipe']
  }).trim();
  let tokens;
  try { tokens = JSON.parse(output); }
  catch { throw new Error('Credential initialization returned invalid data.'); }
  if (!/^niodb_client_[a-f0-9]{64}$/.test(tokens.client_token) || !/^niodb_secret_[a-f0-9]{64}$/.test(tokens.secret_token)) {
    throw new Error('Credential initialization returned invalid tokens.');
  }
  saveToken(clientPath, tokens.client_token);
  saveToken(secretPath, tokens.secret_token);
  return true;
}

async function runMcpStdio(args) {
  loadSettings(args);
  const directory = resolve(option(args, '--dir') || process.env.NIODB_DIR || './nio-db');
  const tokenPath = join(directory, 'client-token');
  const secretPath = join(directory, 'secret-token');
  let token = process.env.NIODB_TOKEN;
  if (!token) {
    if (existsSync(tokenPath)) {
      token = readFileSync(tokenPath, 'utf8').trim();
    } else if (existsSync(secretPath)) {
      token = readFileSync(secretPath, 'utf8').trim();
    }
  }
  const listen = option(args, '--listen') || process.env.NIODB_LISTEN || '127.0.0.1:7432';
  const url = `http://${listen}/mcp`;

  const readline = require('node:readline');
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false,
  });

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    let requestJson;
    try {
      requestJson = JSON.parse(trimmed);
    } catch {
      process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: null, error: { code: -32700, message: 'Parse error' } }) + '\n');
      continue;
    }

    try {
      const headers = { 'Content-Type': 'application/json' };
      if (token) headers['Authorization'] = `Bearer ${token}`;

      const res = await fetch(url, {
        method: 'POST',
        headers,
        body: JSON.stringify(requestJson),
      });

      const data = await res.json();
      process.stdout.write(JSON.stringify(data) + '\n');
    } catch (err) {
      process.stdout.write(JSON.stringify({
        jsonrpc: '2.0',
        id: requestJson.id ?? null,
        error: { code: -32000, message: `Failed to connect to NioDB server at ${url}: ${err.message}` }
      }) + '\n');
    }
  }
}

async function main() {
  const inputArgs = process.argv.slice(2);
  const acceptDefaults = inputArgs.includes('--yes');
  const args = inputArgs.filter(arg => arg !== '--yes');
  if (args.includes('mcp')) {
    await runMcpStdio(args);
    return;
  }
  const binary = executable();
  // Informational and offline commands must not install Nio or create credentials.
  const startup = startupCommand(args);
  const env = { ...process.env, NIODB_NODE_BIN: process.env.NIODB_NODE_BIN || process.execPath,
    NIODB_ALASQL_HELPER: process.env.NIODB_ALASQL_HELPER || join(__dirname, '..', 'runtime', 'alasql.cjs'),
    NIODB_WORKER_RUNNER: process.env.NIODB_WORKER_RUNNER || join(__dirname, '..', 'runtime', 'event-worker.cjs') };
  if (startup) {
    loadSettings(args);
    const settings = await firstRun(args, acceptDefaults);
    const nio = await ensureNio(args);
    if (nio) env.NIODB_NIO_BIN = nio;
    const created = bootstrapAuth(binary, args);
    if (settings && !existsSync(settingsPath)) {
      const descriptor = openSync(settingsPath, 'wx', 0o600);
      try { writeFileSync(descriptor, JSON.stringify(settings, null, 2) + '\n'); fsyncSync(descriptor); }
      finally { closeSync(descriptor); }
    }
    if (created) console.error('\n  Setup complete. Client token and secret saved privately.\n  Starting NioDB...');
  }
  if (args.some(arg => ['--help', '-h'].includes(arg))) {
    console.error('npm CLI: nio-db [OPTIONS]\nFirst launch offers interactive setup and installs Nio if missing.\n  --yes                  Use first-run defaults without prompts\n  NIODB_NO_INSTALL_NIO=1  Disable automatic Nio installation\n');
  }
  if (args.includes('add-secret') && !option(args, '--auth-file') && !process.env.NIODB_AUTH_FILE) {
    const token = execFileSync(binary, args, {
      encoding: 'utf8', timeout: 10000, maxBuffer: 8192, stdio: ['ignore', 'pipe', 'pipe']
    }).trim();
    if (!/^niodb_secret_[a-f0-9]{64}$/.test(token)) throw new Error('Secret rotation returned an invalid token.');
    const data = resolve(option(args, '--dir') || process.env.NIODB_DIR || 'nio-db');
    replaceToken(join(data, 'secret-token'), token);
    process.stdout.write(token + '\n');
    return;
  }
  const child = spawn(binary, args, { stdio: 'inherit', shell: false, env });
  for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => { if (!child.killed) child.kill(signal); });
  child.on('error', (error) => { console.error(`niodb: ${error.message}`); process.exitCode = 1; });
  child.on('exit', (code, signal) => {
    if (signal) { process.exitCode = signal === 'SIGINT' ? 130 : 143; }
    else process.exitCode = code ?? 1;
  });
}

main().catch((error) => {
  // Child stderr can contain sensitive values; report the failure without echoing it.
  console.error(`niodb: ${error.name === 'AbortError' ? 'Setup cancelled.' : error.status !== undefined ? 'Credential initialization failed; inspect the credential path and grants.' : error.message}`);
  process.exitCode = error.name === 'AbortError' ? 130 : 1;
});
