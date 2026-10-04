# NioDB Plan

## Agreed direction and API contract

NioDB will be a standalone Rust server distributed through `npx @nio-labs/niodb`. Nio, NioBridge, Nio0, NioOS, and other web/mobile clients connect through its API. TOON is the preferred storage format, with JSON interoperability at the HTTP boundary.

Nio is the intelligence layer, invoked through the installed Nio CLI. Server startup checks CLI availability, protocol compatibility, and provider readiness. Assistance uses the caller's workspace permissions. Database operations remain available when Nio assistance is unavailable.

The small [OpenAPI 3.1 contract](openapi.yaml) defines the proposed initial HTTP surface: health, Nio readiness, artifact creation/retrieval, and natural-language assistance. It includes bearer authentication, pagination, shared errors, and grounded answer references. The initial assistance contract covers reads and clarification; mutation previews, approval, streaming, and jobs will need explicit contracts as they are designed.

This is a design document; the endpoints are not implemented. The original vision below still needs revision to match these decisions, including Rust packaging, replacement of AlaSQL, and the Nio CLI integration.

**Product:** NioDB  
**Purpose:** An AI-native, file-based runtime and server that gives coding agents persistent memory, executable skills, and a shared context layer — packaged as a lightweight Node.js server.

## Core concept

NioDB is not a traditional database. It is a **directory-based artifact graph** where:
- Every file is a typed collection (think "table")
- Data and logic live together as first-class artifacts
- Nio is the primary admin agent
- Other agents participate via a standardized skill/tool interface

The entire runtime is designed around how **AI agents think**, not how humans write SQL.

## Design principles

- **One runtime, zero native dependencies.** Run with `npx @nio-labs/niodb`. No compiled extensions, no external database server.
- **Files are the database.** Collections are flat files (JSONL, JSON, TOON). Human-readable, editable, version-control-friendly.
- **Agent-first API.** Nio talks to NioDB via intent routing. Other agents talk via OpenAI-compatible tool definitions.
- **Skills as first-class artifacts.** Executable JS functions are stored, versioned, discoverable, and shareable.
- **Portable intelligence.** Copy the `niodb/` directory and you copy the agent's memory + learned capabilities.

## Architecture

```text
+-----------+     +-----------+     +-----------+
|  Nio      |     |  Mobile   |     |  Web      |
|  (admin)  |     |  App      |     |  Dashboard|
+----+------+     +----+------+     +----+------+
     |                  |                  |
     | tool calls       | REST / GQL / WS  | REST / GQL / WS
     v                  v                  v
+-------------------------------------------------+
|                  niodb server                   |
|  - Auth / agent identity                        |
|  - File-per-collection I/O                      |
|  - Blob / file storage (buckets & CAS)          |
|  - GraphQL query, mutation & subscriptions      |
|  - SQL-like query (AlaSQL)                      |
|  - Natural-language proxy (via Nio)             |
|  - Skill discovery & execution                  |
|  - Live dashboard + GraphiQL playground         |
+-------------------------------------------------+
     |
     v
+-------------------+
|  niodb/ directory  |
|  - config.json     |
|  - auth.json       |
|  - sessions.jsonl  |
|  - projects.json   |
|  - todos.json      |
|  - files.jsonl     |
|  - storage/        |
|  - functions/      |
|  - skills.json     |
+-------------------+
```

## Data model

### Collections = Files
Each collection file is a typed, independently addressable dataset:

| File | Content | Format |
|---|---|---|
| `sessions.jsonl` | Session logs, summaries, tags | JSONL (append-only) |
| `projects.json` | Project metadata, status | JSON |
| `todos.json` | Tasks, priorities, assignments | JSON |
| `artifacts/` | Code snippets, configs, diagrams | JSON/TOON |
| `files.jsonl` | File metadata, hashes, MIME types, links | JSONL (append-only) |
| `storage/` | Raw file blobs (images, audio, PDFs, archives) | Binary (CAS / bucket) |
| `functions/*.js` | Executable logic stored as skills | JS |
| `skills.json` | Skill registry with metadata | JSON |
| `auth.json` | Users, password hashes, tokens | JSON |
| `config.json` | Server settings, admin token | JSON |

### Artifacts
Every stored item is an artifact with a stable ID, type, timestamps, and optional links to other artifacts.

### File storage & Blobs
Agents work with non-text assets: voice recordings (`:voice`), screenshots, PDFs, documents, images, and codebase tarballs.
- **Bucket-based organization**: `storage/<bucket>/` (e.g., `storage/attachments/`, `storage/voice/`, `storage/documents/`).
- **Content-Addressable Storage (CAS)**: Files are stored by SHA256 checksum with extension preservation (e.g. `storage/voice/a3f1c...89.wav`), preventing duplicate disk storage.
- **Metadata index**: `files.jsonl` tracks file metadata:
  ```json
  {
    "id": "file_01jb2...",
    "bucket": "voice",
    "filename": "meeting_notes.wav",
    "hash": "a3f1c4...",
    "size": 491520,
    "mime": "audio/wav",
    "created_at": "2026-10-04T00:30:00Z",
    "created_by": "nio",
    "artifact_id": "art_9918...",
    "tags": ["voice", "transcription"]
  }
  ```
- **Chunked & Streaming I/O**: Direct stream upload/download with HTTP 206 Range requests (enabling native audio/video seek and partial document reads) with zero external storage dependencies.

### Functions / Skills
JS functions with embedded metadata. They are:
- Executable at runtime
- Discoverable via `/skills`
- Shareable across agents
- Versioned by filename or manifest entry

## API surface

```yaml
# Auth
POST /api/v1/auth/register
POST /api/v1/auth/login
POST /api/v1/auth/logout
GET  /api/v1/auth/me

# Artifacts
POST /api/v1/artifacts           # Create
GET  /api/v1/artifacts/:id       # Read
PUT  /api/v1/artifacts/:id       # Update
DELETE /api/v1/artifacts/:id     # Delete

# File Storage / Blobs
POST   /api/v1/storage/:bucket/upload       # Multipart or raw stream upload
GET    /api/v1/storage/:bucket/:id          # Stream download (supports HTTP 206 Range)
GET    /api/v1/storage/:bucket/:id/metadata # Read file metadata
DELETE /api/v1/storage/:bucket/:id          # Delete file & release blob
GET    /api/v1/storage/:bucket              # List files in bucket (query & paginate)

# Querying
GET /api/v1/query?sql=...        # SQL-like via AlaSQL
GET /api/v1/query?nl=...         # Human-like, routed through Nio

# GraphQL
POST /graphql                    # GraphQL queries & mutations
GET  /graphql                    # Interactive GraphiQL playground
WS   /graphql                    # GraphQL subscriptions over WebSocket

# Skills
GET  /api/v1/skills              # Discover skills
POST /api/v1/skills/:id/invoke   # Execute skill

# Tools
GET /api/v1/tools                # OpenAI-compatible tool definitions

# WebSocket
WS  /api/v1/ws                   # Live updates on file changes
```

## Agent model

### Nio (primary admin)
- Built-in admin identity
- Full read/write across all namespaces
- Special skills: `nio_admin_audit`, `niodb_route`
- Receives human-like queries from other clients and translates them into operations

### Other agents (via skills)
- Self-register to get an agent-scoped namespace
- Discover tools via `/tools` or `/skills`
- Sandboxed to their own artifacts unless explicitly granted access
- Can share skills privately or publicly

## Query modes

### SQL-like
Structured, predictable queries using AlaSQL across in-memory collections loaded from files.

```sql
SELECT s.*, p.name as project_name
FROM sessions s
LEFT JOIN projects p ON s.project_id = p.id
WHERE s.tags LIKE '%nioz%'
ORDER BY s.ts DESC
LIMIT 5
```

### Human-like
Natural language is forwarded to Nio (admin proxy), which interprets intent and executes the appropriate query or skill.

```
GET /api/v1/query?nl=show my latest nioz sessions
```

### GraphQL
Declarative, strongly typed graph querying for mobile clients, web dashboards, and multi-agent coordination. Eliminates over-fetching and replaces multiple REST roundtrips with a single nested query.

```graphql
# Query: Fetch project, its latest sessions, pending todos, and attached files
query GetProjectDashboard($id: ID!) {
  project(id: $id) {
    id
    name
    status
    sessions(limit: 5) {
      id
      summary
      ts
    }
    todos(status: PENDING) {
      id
      title
      priority
    }
    files(bucket: "attachments") {
      id
      filename
      mime
      size
      url
    }
  }
}
```

```graphql
# Subscription: Live stream updates as sessions or files are added
subscription OnProjectActivity($projectId: ID!) {
  sessionAppended(projectId: $projectId) {
    id
    summary
    tags
  }
  fileUploaded(projectId: $projectId) {
    id
    filename
    mime
    size
  }
}
```

**Key benefits:**
- **Auto-generated schema**: Derived automatically from the collection JSON schemas and `files.jsonl`.
- **Graph traversal**: Traverse links across collections (`Project` ➔ `Sessions` ➔ `Artifacts` ➔ `Files`).
- **Real-time subscriptions**: Live reactive updates over WebSocket for UI & background agents.
- **Embedded GraphiQL playground**: Test and inspect queries directly from `niodb web`.

## Skills system

Skills are the **primary interface for agent extensibility**.

```js
// niodb/functions/semantic_search.js

/**
 * @skill
 * @name semantic_search
 * @description Search artifacts using natural language similarity
 * @version 1.0.0
 * @agent nio,claude,cursor
 * @param {string} query - Natural language search query
 * @param {string} [collection] - Optional collection filter
 * @param {number} [limit=10] - Max results
 */

module.exports = async (db, args) => {
  // Implementation
  return results;
};
```

Skills can:
- Cross-reference multiple collections
- Perform joins, aggregations, and transformations
- Be shared across agents
- Be versioned and audited

## Ecosystem Integration: Nio & NioBridge

NioDB serves as the underlying persistence, file storage, and GraphQL engine for:
1. **Nio** — The primary coding agent and administrative interface.
2. **NioBridge** — The universal AI agent multiplexer and shell (`@nio-labs/niobridge`).

### NioBridge Integration
External coding agents (Claude Code, AGY, OpenCode, Kilo, Codex, Aider) communicate with NioDB via:
- **`sessions.jsonl`**: Universal timeline of cross-agent turns, diffs, and prompts.
- **`artifacts/` & `storage/`**: Code snippets, configs, diffs, and binary blobs (audio, images, PDFs).
- **Embedded MCP Server**: Real-time context discovery and retrieval.
- **Token Ledger**: Consolidated cost and token tracking across all providers.

*(For full architecture, adapters, and CLI specifications of the agent wrapper, see [NIOBRIDGE_PLAN.md](file:///Users/mn/Documents/NIOBRIDGE_PLAN.md)).*

### NioDB Plugin for Nio CLI
NioDB will be integrated directly into Nio as a first-class plugin (`src/plugins.rs`):
- **Plugin Name**: `niodb`
- **Capabilities**: Local storage engine, GraphQL/REST server management, live synchronization.
- **Interactive Commands**:
  - `:niodb status` — Display connected NioDB status, collections count, and storage size.
  - `:niodb query <sql>` — Execute quick AlaSQL queries over sessions and artifacts.
  - `:niodb web` — Launch the embedded web dashboard and GraphiQL playground.

## CLI modes

```bash
# Start NioDB server (default)
npx @nio-labs/niodb

# Start with custom directory
npx @nio-labs/niodb --dir ~/.niodb

# Web dashboard & GraphiQL
niodb web

# Show version
niodb --version
```

## Roadmap

### Phase 1: Foundation
- File-per-collection storage engine
- Blob & file storage manager (CAS, buckets, range streaming)
- HTTP server with auth middleware
- Basic CRUD and file upload/download endpoints
- `niodb web` minimal dashboard
- Local-only operation

### Phase 2: Querying & Graphs
- AlaSQL integration for SQL-like queries
- `/query?sql=` endpoint
- Nio admin proxy for `/query?nl=`
- GraphQL engine with auto-generated schema (`/graphql` & GraphiQL playground)
- Collection-level indexes (JSON sidecar files)

### Phase 3: Skills runtime
- JS function execution sandbox
- `skills.json` manifest
- `/skills` discovery endpoint
- `/skills/:id/invoke` execution
- Built-in starter skills (search, summarize, analyze, export)

### Phase 4: Multi-Agent & NioBridge Integration
- Agent self-registration & namespacing
- NioBridge integration hooks (`sessions.jsonl` cross-agent sync)
- NioDB Plugin for Nio CLI (`:niodb`, background daemon management)
- Cross-agent skill sharing and audit logging

### Phase 5: Polish
- WebSocket live updates & GraphQL subscriptions
- Backup / export / import
- Compaction for append-only collections and blob garbage collection
- Mobile client examples (React Native / Flutter with GraphQL & file sync)
- Documentation and contribution guides

## License

NioDB is released under the **Business Source License 1.1 (BSL 1.1)**.

- Source code is publicly available and usable
- Modification is permitted for internal and personal use
- You may not offer NioDB as a competing managed service or product without a commercial license
- After four years, the license automatically converts to a popular open-source license (target: Apache 2.0 or MIT)

This license protects the project from being wrapped into competing hosted services while keeping the code transparent and accessible to the community.

## Project structure

```
packages/niodb/
├── package.json
├── LICENSE.txt                 # BSL 1.1 full text
├── LICENSE                     # BSL 1.1 summary / reference
├── README.md
├── bin/
│   └── niodb.js                # CLI entry point
├── src/
│   ├── server.js               # HTTP server
│   ├── auth.js                 # Password hashing + tokens
│   ├── storage.js              # File I/O, watchers, compaction
│   ├── blobs.js                # File storage engine (CAS, buckets, stream I/O)
│   ├── graphql/                # GraphQL engine
│   │   ├── schema.js           # Auto-generated GraphQL schema
│   │   ├── resolvers.js        # Resolvers (collections, files, skills)
│   │   └── playground.js       # Embedded GraphiQL playground
│   ├── query.js                # AlaSQL + NL proxy
│   ├── runtime.js              # JS function execution
│   ├── skills.js               # Skill registry + discovery
│   └── ws.js                   # WebSocket live updates & GQL subscriptions
├── dashboard/
│   ├── index.html              # Single-page dashboard + GraphiQL tab
│   └── app.js                  # Dashboard logic
└── functions/                  # Built-in skills
    ├── semantic_search.js
    ├── summarize.js
    ├── analyze_trends.js
    ├── export.js
    └── nio_admin_audit.js
```

## Success criteria

- A developer can run `npx @nio-labs/niodb` and have a working server in under 10 seconds
- Nio can save, query, and retrieve memory across sessions without custom glue code
- Any OpenAI-compatible agent can discover and invoke NioDB skills via standard tool definitions
- The `niodb/` directory is human-readable, version-controllable, and portable
- The web dashboard provides immediate visibility into agent memory and activity
