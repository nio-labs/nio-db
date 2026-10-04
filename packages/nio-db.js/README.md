# nio-db.js

Ultra-lightweight JavaScript and TypeScript client SDK for **NioDB** — the Agentic Database.

- **Zero dependencies** (uses native `fetch`)
- **Works everywhere**: Node.js, Bun, Deno, Next.js, Vite, and React Native
- **Lightweight**: < 8 KB

---

## Installation

```bash
npm install nio-db.js
```

---

## Quickstart

```typescript
import { createNioDB } from "nio-db.js";

const db = createNioDB({
  url: "http://127.0.0.1:7432",
  token: process.env.NIODB_TOKEN,
});

// 1. Documents & Collections
await db.collection("tasks").insert({
  title: "Process payments",
  status: "pending",
  ttl: 3600 // Auto-expires in 1 hour
});

const tasks = await db.collection("tasks").find({ limit: 10 });

// 2. Pure-Rust In-Memory Vector Search
const searchResults = await db.collection("knowledge").searchVector({
  vector: [0.012, -0.045, 0.089, ...],
  topK: 5,
  minScore: 0.75
});

// 3. NioBridge Multi-Agent Sessions
const session = db.session("session_104");
await session.appendTurn({
  agent: "claude",
  model: "claude-3-7-sonnet",
  summary: "Refactored auth middleware",
  costUsd: 0.012
});

const manifest = await session.getManifest();
console.log(manifest.manifest_text);

// 4. Safe SQL Queries
const sqlResults = await db.query("SELECT * FROM tasks WHERE status = 'pending'");
```
