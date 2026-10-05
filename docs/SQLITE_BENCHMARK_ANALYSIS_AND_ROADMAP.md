# NioDB vs SQLite: Benchmark Analysis and Optimization Strategy

## 1. Executive Summary

This document analyzes the current benchmark comparison between **NioDB** and the matched **SQLite HTTP adapter** on identical 10,000-record workloads, identifies root architectural bottlenecks, and outlines a concrete engineering roadmap for NioDB to beat SQLite across all key metrics.

### Performance Summary Table

*Measured on 2026-10-05 on WSL2 Linux (ext4, Intel Core Ultra 5 135H, 18 logical cores, 7.22 GiB RAM).*

| Benchmark Stage / Operation | NioDB Current | SQLite Adapter | Delta / Ratio | Winner |
| :--- | :--- | :--- | :--- | :--- |
| **Bulk Ingestion (10,000 records)** | **13,311 rec/s** (P50: 37.6 ms) | **29,808 rec/s** (P50: 16.0 ms) | SQLite is **~2.2× faster** | 🥈 SQLite |
| **Point Lookup (by Record ID)** | **1.92 ms** P50 | **1.88 ms** P50 | **Virtually tied** (~1.5 ms loopback HTTP) | 🤝 Tie |
| **Collection Scan (5,000 records, limit 50)** | **4.15 ms** P50 | **2.83 ms** P50 | SQLite is **~1.5× faster** | 🥈 SQLite |
| **Vector Search (2,000 × 8D, top-10 cosine)** | **3.33 ms** P50 | **7.09 ms** P50 | NioDB is **~2.1× faster** | 🏆 **NioDB** |
| **SQL Queries (Grouped aggregations & sort)** | **31.41 ms** P50 | **2.67 ms** P50 | SQLite is **~11.8× faster** | 🥈 SQLite |
| **Combined Server Memory (RSS)** | **~138.5 MiB** (Rust ~40 + Node ~98) | **12.9 MiB** (Rust + SQLite) | SQLite uses **~10.7× less RAM** | 🥈 SQLite |
| **Process Footprint** | Rust main process + 1-2 Node workers | Single native Rust process | SQLite has **zero subprocesses** | 🥈 SQLite |

---

## 2. Root Cause Analysis

### 2.1 Why SQLite Wins on SQL (11.8× faster)
1. **Out-of-Process Serialization & IPC Overhead**:
   - In [`src/query.rs`](../src/query.rs), NioDB executes SQL by serializing candidate records into a JSON payload, piping them over `stdin` to an external Node.js helper running AlaSQL.
   - Node.js parses the JSON in V8, constructs internal tables, runs the query, and formats the result back as JSON across `stdout`.
   - Even with warm worker pooling, JSON serialization, pipe IPC, and V8 engine execution impose an unavoidable ~30 ms floor on 10k datasets.
2. **Memory Footprint**:
   - The Node.js child worker consumes ~85–105 MiB RSS simply to initialize V8 and AlaSQL.
   - In contrast, SQLite runs inside the Rust process memory space via C FFI, operating directly on b-tree pages in ~12 MiB total RSS.

### 2.2 Why SQLite Wins on Ingestion (2.2× faster)
1. **Per-Record Projection Filesystem Thrashing**:
   - While NioDB batches journal commits to `journal.jsonl` into a single checksummed frame, [`Store::bulk_insert`](../src/storage.rs) invokes `write_projection` for **every single record**.
   - For 10,000 records, this results in **10,000 temporary file creations, writes, and `rename()` calls** in the `artifacts/` folder.
   - Synchronous filesystem metadata manipulation dominates the ingestion time.
2. **SQLite WAL Efficiency**:
   - SQLite writes transactions sequentially to a single write-ahead log (`comparison.sqlite-wal`) using `synchronous=FULL`, without per-record filesystem metadata operations.

### 2.3 Why NioDB Wins on Vector Search (2.1× faster)
1. **Zero-Disk In-Memory Evaluation**:
   - NioDB stores all records in memory as `Arc<Artifact>` inside a `BTreeMap`.
   - [`Store::search_vectors`](../src/storage.rs) iterates directly over these in-memory records and computes cosine similarity in Rust without disk page faults or deserialization.
2. **SQLite Disk & Serialization Penalty**:
   - Without native vector indexing, the SQLite benchmark must query rows from disk/page cache, extract JSON strings, deserialize them in Rust, and calculate similarities.

---

## 3. Architecture & Optimization Strategy to Outperform SQLite

To surpass SQLite across both throughput and latency while preserving NioDB's agentic AI advantages:

### 3.1 Pure Rust Structured Query Engine (Eliminate AlaSQL / Node.js)
* **Strategy**: Implement the proposed [Structured Query API](STRUCTURED_QUERY_API.md) (`POST /api/v1/records/query`) directly in Rust.
* **Mechanism**:
  - Filter, sort, and aggregate directly over in-memory `Arc<Artifact>` data without JSON serialization or IPC.
  - Rust-native predicate evaluation (`eq`, `in`, `gt`, `contains`) and aggregations (`sum`, `avg`, `count`).
* **Expected Outcome**:
  - Query latency drops from **31.41 ms to < 1.0 ms** (sub-millisecond), beating SQLite's 2.67 ms.
  - Server memory drops from **~140 MiB to ~15–20 MiB** by removing the Node.js worker entirely.

### 3.2 Asynchronous / Deferred Projections (`.toon`)
* **Strategy**: Remove synchronous projection writing from the ingestion path.
* **Mechanism**:
  - `journal.jsonl` is already the single source of truth and recovery authority.
  - Push projected `.toon` file generation to an asynchronous background worker channel or make it an on-demand/export feature.
* **Expected Outcome**:
  - Bulk ingestion throughput will jump from **13,311 rec/s to > 35,000–45,000 rec/s**, outperforming SQLite's 29,808 rec/s.

### 3.3 In-Memory Secondary & Collection Indexing
* **Strategy**: Avoid linear scans over the primary `BTreeMap<String, Arc<Artifact>>`.
* **Mechanism**:
  - Add an in-memory collection index:
    ```rust
    collection_index: BTreeMap<String, Vec<Arc<Artifact>>>
    ```
  - Optional secondary hash/b-tree indexes for tagged filter fields (e.g., `status`, `plan`, `user_id`).
* **Expected Outcome**:
  - Collection scans and paginated lists drop from **4.15 ms to < 0.8 ms** (sub-millisecond), beating SQLite's 2.83 ms.

### 3.4 SIMD-Accelerated Vector Search & Compact Embedding Storage
* **Strategy**: Capitalize on NioDB's architectural advantage as an agentic/vector-native database.
* **Mechanism**:
  - Store vector embeddings as contiguous `Vec<f32>` slices instead of nested `serde_json::Value` objects.
  - Implement SIMD-accelerated dot product and Euclidean norm calculation (via AVX2 / AVX-512 / ARM NEON).
  - For larger datasets (> 10,000 vectors), support an in-memory HNSW (Hierarchical Navigable Small World) index.
* **Expected Outcome**:
  - Vector search latency drops from **3.33 ms to < 0.5 ms**, widening the lead over SQLite by **> 10×**.

---

## 4. Implementation Roadmap

### Phase 1: Ingestion & Filesystem Throughput (Immediate)
- [ ] Move `write_projection` in `bulk_insert` to an asynchronous channel / background batch queue.
- [ ] Buffer artifact sync operations and eliminate per-item temporary file renames during bulk operations.
- [ ] Verify ingestion throughput exceeds 30,000 records/sec.

### Phase 2: In-Process Query Engine & Node.js Removal (Core Performance)
- [ ] Implement `POST /api/v1/records/query` in Rust according to [`STRUCTURED_QUERY_API.md`](STRUCTURED_QUERY_API.md).
- [ ] Support native grouping, counting, filtering, and sorting directly in `src/storage.rs`.
- [ ] Deprecate the mandatory Node.js AlaSQL worker for standard queries.
- [ ] Verify query latency drops under 1.5 ms and total server RSS stays below 20 MiB.

### Phase 3: Secondary Indexing & SIMD Vector Engine (Scale)
- [ ] Maintain an in-memory `collection -> [Arc<Artifact>]` index.
- [ ] Vector SIMD acceleration for cosine similarity.
- [ ] Memory compaction for stored embeddings.
