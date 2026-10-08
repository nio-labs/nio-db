# @nio-labs/nio-db.js

Ultra-lightweight JavaScript and TypeScript client SDK for **NioDB** — the Agentic Database.

- **Zero dependencies** (uses native `fetch`)
- **Works everywhere**: Node.js, Bun, Deno, Next.js, Vite, and React Native
- **Lightweight**: < 8 KB

---

## Installation

```bash
npm install @nio-labs/nio-db.js
```

---

## Quickstart

```typescript
import { createNioDB } from "@nio-labs/nio-db.js";

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

// 3. Multi-Agent Sessions
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


## Application persistence and ESM

Both `import { NioDB } from '@nio-labs/nio-db.js'` and CommonJS `require` are supported.
`query(sql, parameters, options)` now transmits bound values; one-argument calls still work.

New methods: `configureCollection`, `getCollectionConstraints`, `mutate`,
`startDeletionJob`, `getDeletionJob`, `getPersistenceStatus`, `getLease`,
`acquireLease`, `renewLease`, and `releaseLease`. Mutations accept an `idempotency_key`,
ordered create/update/delete operations, and optional fenced lease proofs. Updates/deletes
require `expected_revision`; existing bulk methods retain their original semantics.

Requests default to a 30-second deadline and an 8-MiB response bound. Set constructor
`timeoutMs`, `maxResponseBytes`, or injected `fetch`; pass per-request `timeoutMs` and
`signal` to query, mutation, policy, lease, and collection CRUD methods. HTTP errors expose
`status`, `code`, `requestId`, and `data`. No automatic retry is performed.

See [the complete storage contract](https://github.com/nio-labs/nio-db/blob/main/docs/APPLICATION_STORAGE.md)
for configuration order, transaction limits, permanent retry receipts, and lease lifecycle.
NioJS must implement the fetch/abort/stream primitives used by the SDK before runtime parity
can be claimed. `npm run build` generates the native ESM entry; `prepack` rebuilds it.
