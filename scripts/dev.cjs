#!/usr/bin/env node
'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');

const BACKEND_PORT = 7432;
const DEV_PORT = process.env.DEV_PORT ? parseInt(process.env.DEV_PORT, 10) : 7430;
const ROOT = path.resolve(__dirname, '..');

const LIVE_RELOAD_SCRIPT = `
<!-- NioDB Live Reload -->
<script>
(function() {
  let source = null;
  function connect() {
    source = new EventSource('/__livereload');
    source.onmessage = function(e) {
      if (e.data === 'reload') {
        console.log('[LiveReload] File changed, reloading...');
        window.location.reload();
      }
    };
    source.onerror = function() {
      source.close();
      setTimeout(connect, 1000);
    };
  }
  connect();
})();
</script>
`;

// Connected SSE clients for live reload
const clients = new Set();

function broadcastReload() {
  console.log('\x1b[36m[LiveReload]\x1b[0m Broadcasting reload to browser...');
  for (const res of clients) {
    try {
      res.write('data: reload\n\n');
    } catch {}
  }
}

// 1. Dev HTTP Proxy Server (Instant Frontend Live Reload)
const server = http.createServer((req, res) => {
  // SSE Live Reload endpoint
  if (req.url === '/__livereload') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*'
    });
    res.write('data: connected\n\n');
    clients.add(res);
    req.on('close', () => clients.delete(res));
    return;
  }

  const urlPath = req.url.split('?')[0];

  // Live-serve console.html directly from disk
  if (urlPath === '/' || urlPath === '/console') {
    try {
      const htmlPath = path.join(ROOT, 'src', 'console.html');
      let content = fs.readFileSync(htmlPath, 'utf8');
      if (content.includes('</body>')) {
        content = content.replace('</body>', `${LIVE_RELOAD_SCRIPT}\n</body>`);
      } else {
        content += LIVE_RELOAD_SCRIPT;
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(content);
      return;
    } catch (e) {
      console.error('[LiveReload] Error serving console.html:', e);
    }
  }

  // Live-serve guide.html directly from disk
  if (urlPath === '/docs' || urlPath === '/docs/' || urlPath === '/guide/') {
    res.writeHead(308, { Location: '/guide' });
    res.end();
    return;
  }

  if (urlPath === '/guide') {
    try {
      const htmlPath = path.join(ROOT, 'src', 'guide.html');
      let content = fs.readFileSync(htmlPath, 'utf8');
      if (content.includes('</body>')) {
        content = content.replace('</body>', `${LIVE_RELOAD_SCRIPT}\n</body>`);
      } else {
        content += LIVE_RELOAD_SCRIPT;
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(content);
      return;
    } catch (e) {
      console.error('[LiveReload] Error serving guide.html:', e);
    }
  }

  // Live-serve doc.html (Swagger) directly from disk
  if (urlPath === '/doc' || urlPath === '/doc/') {
    try {
      const htmlPath = path.join(ROOT, 'src', 'doc.html');
      let content = fs.readFileSync(htmlPath, 'utf8');
      if (content.includes('</body>')) {
        content = content.replace('</body>', `${LIVE_RELOAD_SCRIPT}\n</body>`);
      } else {
        content += LIVE_RELOAD_SCRIPT;
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(content);
      return;
    } catch (e) {
      console.error('[LiveReload] Error serving doc.html:', e);
    }
  }

  // Proxy all API and other requests to the running backend
  const proxyReq = http.request({
    hostname: '127.0.0.1',
    port: BACKEND_PORT,
    path: req.url,
    method: req.method,
    headers: req.headers
  }, (proxyRes) => {
    res.writeHead(proxyRes.statusCode, proxyRes.headers);
    proxyRes.pipe(res, { end: true });
  });

  proxyReq.on('error', (err) => {
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      error: 'backend_offline',
      message: `Could not connect to NioDB backend on port ${BACKEND_PORT}. Recompiling or starting up...`
    }));
  });

  req.pipe(proxyReq, { end: true });
});

// Helper: check if backend port is already open
function checkBackend(port) {
  return new Promise((resolve) => {
    const req = http.get(`http://127.0.0.1:${port}/health`, (res) => {
      resolve(true);
    });
    req.on('error', () => resolve(false));
    req.setTimeout(500, () => {
      req.destroy();
      resolve(false);
    });
  });
}

// 2. Manage NioDB Backend Process
let backendProc = null;
let isRebuilding = false;
let ownsBackend = false;
let restartRequested = false;
let restartTimer = null;
let forceStopTimer = null;
let stopping = false;

function stopBackend(child) {
  if (!child || !child.pid) return;
  try { process.kill(-child.pid, 'SIGTERM'); } catch (error) {
    if (error.code !== 'ESRCH') throw error;
  }
  clearTimeout(forceStopTimer);
  forceStopTimer = setTimeout(() => {
    try { process.kill(-child.pid, 'SIGKILL'); } catch (error) {
      if (error.code !== 'ESRCH') console.error('[Backend] Could not force stop:', error);
    }
  }, 3000);
  forceStopTimer.unref();
}

function startBackend() {
  if (backendProc || stopping) return;
  clearTimeout(restartTimer);
  ownsBackend = true;
  const child = spawn(process.execPath, [path.join(ROOT, 'bin', 'niodb.cjs'), '--yes', '--listen', `127.0.0.1:${BACKEND_PORT}`], {
    cwd: ROOT,
    stdio: 'inherit',
    detached: true,
    env: { ...process.env, RUST_LOG: 'info' }
  });
  backendProc = child;
  child.on('exit', (code, signal) => {
    if (backendProc !== child) return;
    backendProc = null;
    clearTimeout(forceStopTimer);
    if (stopping) return;
    if (restartRequested) {
      restartRequested = false;
      restartTimer = setTimeout(startBackend, 200);
    } else if (!isRebuilding) {
      console.log(`\x1b[33m[Backend]\x1b[0m Exited with code ${code ?? signal}. Restarting in 1s...`);
      restartTimer = setTimeout(startBackend, 1000);
    }
  });
}

function restartBackend() {
  if (!ownsBackend || stopping) return;
  restartRequested = true;
  clearTimeout(restartTimer);
  if (backendProc) {
    stopBackend(backendProc);
  } else {
    restartRequested = false;
    startBackend();
  }
}

async function init() {
  const backendRunning = await checkBackend(BACKEND_PORT);

  server.listen(DEV_PORT, '127.0.0.1', () => {
    console.log(`\n\x1b[32m✔ NioDB Live Reload Dev Server ready!\x1b[0m`);
    console.log(`  \x1b[1m➜ Console:\x1b[0m   \x1b[36mhttp://127.0.0.1:${DEV_PORT}/console\x1b[0m`);
    console.log(`  \x1b[1m➜ Guides:\x1b[0m    \x1b[36mhttp://127.0.0.1:${DEV_PORT}/guide\x1b[0m`);
    console.log(`  \x1b[1m➜ Swagger:\x1b[0m   \x1b[36mhttp://127.0.0.1:${DEV_PORT}/doc\x1b[0m`);
    if (backendRunning) {
      console.log(`  \x1b[1m➜ Backend:\x1b[0m   \x1b[32mhttp://127.0.0.1:${BACKEND_PORT} (connected to running server)\x1b[0m\n`);
    } else {
      console.log(`  \x1b[1m➜ Backend:\x1b[0m   \x1b[33mhttp://127.0.0.1:${BACKEND_PORT} (starting launcher...)\x1b[0m\n`);
      startBackend();
    }
    console.log(`  \x1b[90m• Open port ${DEV_PORT} for frontend live reload; port ${BACKEND_PORT} serves compiled HTML.\x1b[0m`);
    console.log(`  \x1b[90m• Editing HTML files (console.html, guide.html, doc.html) triggers instant browser reload.\x1b[0m`);
    console.log(`  \x1b[90m• Editing Rust files (*.rs) recompiles and restarts the backend.\x1b[0m\n`);
  });

  // 3. File Watcher
  let reloadTimer = null;
  let rustRebuildTimer = null;

  const srcDir = path.join(ROOT, 'src');
  const runtimeDir = path.join(ROOT, 'runtime');

  // Watch HTML changes for instantaneous browser reload (no Rust recompile needed)
  fs.watch(srcDir, { recursive: true }, (event, filename) => {
    if (!filename) return;
    if (filename.endsWith('.html')) {
      clearTimeout(reloadTimer);
      reloadTimer = setTimeout(() => {
        console.log(`\x1b[32m[File Changed]\x1b[0m ${filename}`);
        broadcastReload();
      }, 150);
    } else if (filename.endsWith('.rs')) {
      clearTimeout(rustRebuildTimer);
      rustRebuildTimer = setTimeout(() => {
        console.log(`\x1b[33m[Rust Changed]\x1b[0m ${filename}. Recompiling release binary...`);
        isRebuilding = true;
        const build = spawn('cargo', ['build', '--release', '--locked'], { cwd: ROOT, stdio: 'inherit' });
        build.on('close', (code) => {
          isRebuilding = false;
          if (code === 0) {
            console.log('\x1b[32m[Rust Build]\x1b[0m Build successful, restarting server...');
            restartBackend();
            broadcastReload();
          } else {
            console.error(`\x1b[31m[Rust Build]\x1b[0m Compilation failed with code ${code}`);
          }
        });
      }, 400);
    }
  });

  // Watch runtime JS changes
  fs.watch(runtimeDir, (event, filename) => {
    if (!filename) return;
    console.log(`\x1b[33m[Runtime Changed]\x1b[0m ${filename}.`);
    restartBackend();
    broadcastReload();
  });
}

init();

// Clean exit handling
function cleanup() {
  if (stopping) return;
  stopping = true;
  console.log('\nShutting down dev servers...');
  clearTimeout(restartTimer);
  if (backendProc) {
    const child = backendProc;
    child.once('exit', () => process.exit(0));
    stopBackend(child);
    setTimeout(() => process.exit(0), 5000).unref();
  } else {
    process.exit(0);
  }
  server.close();
}

process.on('SIGINT', cleanup);
process.on('SIGTERM', cleanup);
