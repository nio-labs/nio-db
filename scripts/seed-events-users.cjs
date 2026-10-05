#!/usr/bin/env node
'use strict';

const { readFileSync, writeFileSync, existsSync, chmodSync } = require('node:fs');
const { randomBytes } = require('node:crypto');
const { join, resolve } = require('node:path');

const DATA_DIR = process.env.NIODB_DATA_DIR || resolve(__dirname, '../nio-db');
const BASE_URL = process.env.NIODB_BASE_URL || 'http://127.0.0.1:7432';
const TOKEN = readFileSync(join(DATA_DIR, 'client-token'), 'utf8').trim();
const CREDENTIALS_FILE = join(DATA_DIR, 'seed-users.json');
const USERNAMES = ['demo_alice', 'demo_bob', 'demo_charlie'];
const EVENT_NAMES = ['user.signup', 'task.dispatched', 'order.fulfilled', 'agent.checkpoint', 'backup.completed'];
const WATCH = process.argv.includes('--watch');

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function request(path, options = {}) {
  const response = await fetch(`${BASE_URL}${path}`, {
    ...options,
    headers: { Authorization: `Bearer ${TOKEN}`, ...options.headers }
  });
  const body = await response.json();
  if (!response.ok) {
    throw new Error(`${path} returned ${response.status}: ${body.error?.message || JSON.stringify(body)}`);
  }
  return body;
}

async function seedUsers() {
  const existing = new Set((await request('/api/v1/auth/users')).users.map(user => user.username));
  const credentials = existsSync(CREDENTIALS_FILE)
    ? JSON.parse(readFileSync(CREDENTIALS_FILE, 'utf8'))
    : {};
  let created = 0;
  for (const username of USERNAMES) {
    if (existing.has(username)) continue;
    const password = credentials[username] || randomBytes(18).toString('base64url');
    credentials[username] = password;
    writeFileSync(CREDENTIALS_FILE, JSON.stringify(credentials, null, 2) + '\n', { mode: 0o600 });
    chmodSync(CREDENTIALS_FILE, 0o600);
    await request('/api/v1/auth/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password })
    });
    created++;
  }
  console.log(`Users: ${created} created, ${USERNAMES.length - created} already present`);
  console.log(`Sample login credentials: ${CREDENTIALS_FILE} (private file)`);
}

async function simulateEvents() {
  const controller = new AbortController();
  const stream = await fetch(`${BASE_URL}/api/v1/events/stream`, {
    headers: { Authorization: `Bearer ${TOKEN}` },
    signal: controller.signal
  });
  if (!stream.ok || !stream.body) throw new Error(`Event stream returned ${stream.status}`);
  let received = 0;
  const reader = stream.body.getReader();
  const receive = (async () => {
    const decoder = new TextDecoder();
    let buffer = '';
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, '\n');
        let boundary;
        while ((boundary = buffer.indexOf('\n\n')) !== -1) {
          const block = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          if (block.split('\n').some(line => line === 'event: message')) received++;
        }
      }
    } catch (error) {
      if (!controller.signal.aborted) throw error;
    }
  })();

  let published = 0;
  try {
    await sleep(100);
    while (published < 30) {
      await publishSample(published);
      published++;
      await sleep(100);
    }
    await sleep(200);
  } finally {
    controller.abort();
    await receive;
  }
  console.log(`Events: ${published} published, ${received} received through the authenticated stream`);
  if (received !== published) throw new Error('Event stream missed published sample events');
}

async function publishSample(index) {
  await request('/api/v1/events', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: EVENT_NAMES[index % EVENT_NAMES.length],
          data: {
            source: 'niodb-demo-simulator',
            sequence: index + 1,
            timestamp: new Date().toISOString(),
            trace_id: randomBytes(8).toString('hex')
          }
        })
  });
}

async function main() {
  await seedUsers();
  if (WATCH) {
    console.log('Publishing sample events every second; press Ctrl+C to stop.');
    let published = 0;
    while (true) {
      try {
        await publishSample(published);
        published++;
        if (published % 10 === 0) console.log(`Published ${published} events...`);
      } catch (error) {
        console.log(`Waiting for backend: ${error.message}`);
      }
      await sleep(1000);
    }
  } else {
    console.log('Publishing 30 sample events...');
    await simulateEvents();
  }
}

main().catch(error => {
  console.error(`Seed failed: ${error.message}`);
  process.exitCode = 1;
});
