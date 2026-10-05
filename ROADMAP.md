# NioDB Next-Gen Architecture & Feature Roadmap

## Vision: The All-in-One Edge & AI Engine

Modern AI applications and edge systems suffer from severe **infrastructure sprawl**. A production agent typically requires:
* PostgreSQL / SQLite (Documents & Relational data)
* Pinecone / Qdrant (Vector embeddings)
* Redis (Key-value cache, sessions, rate limits)
* RabbitMQ / BullMQ (Asynchronous agent tasks, background workers)
* InstantDB / PowerSync (Offline-first client sync)

NioDB unifies all of these capabilities into a **single, ultra-lightweight ~8 MB Rust executable** with an **under 15 KB client SDK** and **~12 MB idle RAM footprint**.

```
                              ┌──────────────────────────────────────────────┐
                              │                 NioDB Engine                 │
                              │           (Single ~8MB Rust Binary)          │
                              └──────────────────────┬───────────────────────┘
                                                     │
         ┌───────────────────┬───────────────────────┼───────────────────────┬───────────────────┬───────────────────┐
         │                   │                       │                       │                   │                   │
         ▼                   ▼                       ▼                       ▼                   ▼                   ▼
   ┌───────────┐       ┌───────────┐           ┌───────────┐           ┌───────────┐       ┌───────────┐       ┌───────────┐
   │ Multimodal│       │ 1. KV     │           │ 2. Task   │           │ 3. Offline│       │ 4. pgwire │       │ 5. Agent  │
   │ Core & SQL│       │   Module  │           │    Queue  │           │    Sync   │       │ Connector │       │ Branching │
   │ + Vectors │       │ (Redis)   │           │(RabbitMQ) │           │(Local-1st)│       │ (DBeaver) │       │ (Git-data)│
   └───────────┘       └───────────┘           └───────────┘           └───────────┘       └───────────┘       └───────────┘
                                                     │
                                                     ▼
                                            ┌─────────────────┐
                                            │ 6. Cryptographic│
                                            │  Merkle Ledger  │
                                            │  (Tamper-Proof) │
                                            └─────────────────┘
```

---

## The Big 6 Next-Gen Modules

### 1. ⚡ High-Performance KV Module *(Redis-like)*
A dedicated, multi-core, lock-free key-value store optimized for high-throughput caching, rate limiting, and session state.

* **Multi-Core Throughput (Beats Redis)**: Lock-free partitioned sharding across CPU cores (`dashmap`), bypassing Redis's single-thread command execution bottleneck.
* **Atomic Operations**: Atomic `INCR`, `DECR`, and Compare-And-Swap (`CAS`).
* **High-Precision TTL**: In-memory timer wheel for expiration without polling overhead.
* **Storage Modes**:
  * `ephemeral`: In-memory only (100k+ ops/sec, zero disk I/O).
  * `durable`: Persisted to `journal.jsonl` for persistent configuration and sessions.

```typescript
// KV API
await db.kv.set("session:usr_99", { role: "admin" }, { ttl: 3600 });
await db.kv.incr("rate_limit:ip_1.2.3.4", 1);
const val = await db.kv.get("session:usr_99");
```

---

### 2. 📨 Distributed Task Queue *(RabbitMQ / BullMQ-like)*
A reliable task queue designed for asynchronous AI agent workflows, background jobs, and worker pools.

* **Work-Stealing Producer-Consumer**: Multiple workers pull tasks concurrently from named queues.
* **Guaranteed Reliability**:
  * **Visibility Timeouts**: Tasks leased by a worker become temporarily invisible to others while being processed.
  * **Explicit `ACK` / `NACK`**: Unacknowledged or failed tasks are automatically re-queued.
  * **Dead-Letter Queue (DLQ)**: Poison tasks exceeding `max_retries` are routed to a DLQ for inspection.
* **Delayed / Scheduled Execution**: Native support for retry backoff and delayed jobs.

```typescript
// Producer
await db.queue.push("agent-jobs", { action: "summarize_document", docId: "42" });

// Consumer
const task = await db.queue.pull("agent-jobs", { visibilityTimeout: 30 });
try {
  await processJob(task.payload);
  await task.ack();
} catch (err) {
  await task.nack(); // Automatically returns to queue
}
```

---

### 3. 🔄 Offline-First Bidirectional Sync *(InstantDB / Local-First)*
Seamless local-first data synchronization for web (IndexedDB), mobile (React Native / SQLite), and desktop (Tauri/Electron).

* **Zero-Latency (0ms) Optimistic UI**: Writes hit the client’s local storage immediately with zero network delay.
* **Sequence-Based Delta Sync**: The server streams only mutation diffs since the client’s last sequence checkpoint (`since_seq`), minimizing bandwidth.
* **Deterministic Conflict Resolution**: Hybrid Logical Clock / Last-Write-Wins and field-level 3-way merge.
* **Reactive Live Queries**: Client UI automatically re-renders when local mutations occur or when remote changes arrive via Server-Sent Events / WebSockets.

```typescript
const db = createNioDB({
  url: "https://db.myapp.com",
  offline: { storage: "indexeddb", autoSync: true }
});

// Works 100% offline with 0ms latency:
await db.collection("notes").create({ title: "Drafting while offline" });
```

---

### 4. 🔌 Universal `pgwire` Connector *(DBeaver / DataGrip / BI)*
A native PostgreSQL wire-protocol server listening on port `7432` / `5432`.

* **0% Extra Disk Space**: Queries execute directly against NioDB’s in-memory collections and journal. **No duplicate `.sqlite` file is ever written.**
* **Instant GUI Compatibility**:
  * Users click **New Connection $\rightarrow$ PostgreSQL** in DBeaver, TablePlus, or DataGrip.
  * Host: `localhost`, Port: `7432`.
  * Visually explore collections as tables, execute SQL queries, inspect schemas, and export data.
* **BI & Dashboard Ready**: Out-of-the-box compatibility with Metabase, Grafana, and Superset.
* **Zero Custom Drivers**: Works out of the box with standard Postgres drivers (`psycopg2`, `pg`, `libpq`) across any programming language.

---

### 5. 🌿 Agent "Sandboxed Branching" *(Git-for-Data for AI Agents)*
Zero-copy database branching for autonomous AI agents to test multi-step operations without risking production state.

* **Zero-Copy Instant Branches**: Create an isolated sandbox environment in 1 ms without duplicating storage.
* **Safe Agent Experimentation**: Autonomous agents run simulations, speculative planning, and tool calls in a private sandbox.
* **Merge or Discard**:
  * **`branch.merge()`**: Fast-forwards validated changes to production.
  * **`branch.discard()`**: Vanishes completely with zero side-effects if the agent hallucinates or encounters an error.

```typescript
// 1. Agent creates an isolated sandbox branch
const branch = await db.branch("agent-simulation-run-42");

// 2. Perform tentative mutations
await branch.collection("balances").update("user_1", { amount: 50 });
await branch.collection("orders").create({ item: "A1", status: "pending" });

// 3. Verify outcome before committing
if (await agentValidator.verify(branch)) {
  await branch.merge(); // Safely applies to production!
} else {
  await branch.discard(); // Zero trace left behind
}
```

---

### 6. 🛡️ Cryptographic Merkle Provenance *(Tamper-Proof Ledger)*
An immutable, mathematically verifiable audit trail for legal, financial, and autonomous agent accountability.

* **SHA-256 Hash Chaining**: Every journal frame includes a cryptographic hash of the preceding frame, creating an unbreakable Merkle chain.
* **Tamper-Evident Integrity**: Any unauthorized manual modification, deletion, or reordering of journal files is detected immediately (`db.ledger.verify()`).
* **Authorized Deletes Preserved**: Standard deletions via SDK or REST API append auditable deletion tombstones without breaking the cryptographic chain.
* **Zero Performance Cost**: Hardware-accelerated SHA-256 takes ~200 nanoseconds per frame in Rust, preserving 50,000+ rec/s ingestion speed.

```typescript
// Verify entire database integrity
const report = await db.ledger.verify();
// { verified: true, totalFrames: 142500, rootHash: "9f8a3c2e...", tamperingDetected: false }

// Generate an audit proof for a single record
const proof = await db.ledger.getProof("record_user_123");
```

---

## Resource & Footprint Targets

| Dimension | Current NioDB | With All 6 Next-Gen Modules | Traditional 5-Container Stack |
| :--- | :--- | :--- | :--- |
| **Compiled Binary** | **6.1 MB** | **~7.5 – 8.5 MB** 🏆 | ~260+ MB |
| **Idle Memory (RSS)** | **~9.3 MB** | **~12 – 15 MB** 🏆 | ~250 – 350 MB |
| **Startup Time** | **< 15 ms** | **< 25 ms** 🏆 | 10 – 30 seconds |
| **Client SDK Size** | **< 8 KB** | **< 15 KB** 🏆 | 100 – 300 KB |
| **Daemons / Containers**| **1 process** | **1 process** 🏆 | 4 – 5 containers |
