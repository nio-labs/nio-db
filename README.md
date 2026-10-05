# NioDB — The Agentic DB that works.

_A lightweight database with natural-language queries, powered by Nio._

NioDB is a standalone Rust server. Nio and web/mobile applications connect over HTTP. Nio CLI supplies natural-language interpretation and grounded answers. AlaSQL is required for SQL and natural-language query execution, using a bounded Node subprocess.

## Current implementation

- One workspace per server, backend bearer credentials, username/password users, record creation/retrieval, pagination and durable idempotency.
- Checksummed, append-only recovery journal with fsync before acknowledgement and exclusive server locking.
- Readable TOON record projections and JSON HTTP interoperability.
- Optional file ownership by user ID, user deletion, live events, TypeScript event workers and outgoing webhooks.
- Restricted read-only AlaSQL SELECT, bound parameters, joins and aggregates.
- Nio-assisted queries, clarification, owned conversations and validated record references.
- Permission-filtered Nio skill/plugin discovery; approved small skills can guide assistance.
- Built-in Shadcn + Vue 3 Web Console Dashboard (accessible in your browser at [http://localhost:7432](http://localhost:7432) or [http://localhost:7432/console](http://localhost:7432/console)) to manage Data & Queries, Files & Buckets, Live Events & Webhooks, Auth Users, and Server Config.
- [OpenAPI 3.1 contract](openapi.yaml), readiness endpoints, offline backup and npm binary launcher.

---

**Contents**

- [Current implementation](#current-implementation)
- [Cloud deployment](#cloud-deployment)
  - [1-Click Deploy to Railway](#1-click-deploy-to-railway)
  - [1-Click Deploy to Koyeb](#1-click-deploy-to-koyeb)
- [1-Command Self-Hosting with Docker](#1-command-self-hosting-with-docker)
  - [Direct LLM Fallback (Zero-Daemon AI)](#direct-llm-fallback-zero-daemon-ai)
- [CLI and first-run setup](#cli-and-first-run-setup)
  - [Run from source now](#run-from-source-now)
- [API examples](#api-examples)
  - [Guides](#guides)
  - [Interactive documentation](#interactive-documentation)
  - [SQL surface](#sql-surface)
  - [Row-Level Security (RLS)](#row-level-security-rls)
  - [Model Context Protocol (MCP) for Coding Agents](#model-context-protocol-mcp-for-coding-agents)
  - [Starter demos](#starter-demos)
- [App authentication](#app-authentication)
- [Files and buckets](#files-and-buckets)
- [Live events and webhooks](#live-events-and-webhooks)
- [Chat with Nio](#chat-with-nio)
- [Nio skills and plugins](#nio-skills-and-plugins)
- [Storage and backup](#storage-and-backup)
- [Web Console Dashboard](#web-console-dashboard)
- [Client SDKs](#client-sdks)
- [Benchmark](#benchmark)
- [Initial limits](#initial-limits)
- [Packaging and release](#packaging-and-release)
- [License](#license)

---

## Cloud deployment

Deploy your personal, 100% agentic NioDB instance to the cloud in one click:

### 1-Click Deploy to Railway

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/new?template=https%3A%2F%2Fgithub.com%2Fnio-labs%2Fnio-db)

- **Persistent Volume:** Automatically mounts persistent storage at `/data` so your records, files, journal, and users survive redeploys.
- **Direct LLM Fallback:** Add `OPENAI_API_KEY`, `GEMINI_API_KEY`, or custom OpenAI endpoint in the Railway dashboard environment variables.
- **Free Automatic SSL:** Connect your web/mobile apps and coding agents directly via `https://<your-project>.up.railway.app`.

### 1-Click Deploy to Koyeb

[![Deploy to Koyeb](https://www.koyeb.com/static/images/deploy/button.svg)](https://app.koyeb.com/deploy?type=git&repository=github.com/nio-labs/nio-db&branch=main&name=niodb)

- **Native Healthchecks:** Automated health monitoring at `/api/v1/health` with zero configuration.
- **Global Edge Routing:** Built-in SSL and low-latency HTTP routing on port `7432`.

## 1-Command Self-Hosting with Docker

NioDB is 100% self-contained with zero external database dependencies:

```sh
docker compose up -d
```

Or run directly with Docker:

```sh
docker run -d \
  --name niodb \
  -p 7432:7432 \
  -v $(pwd)/data:/data \
  -e OPENAI_API_KEY=your_key_here \
  nio-labs/nio-db:latest
```

On first boot, NioDB automatically initializes authentication, creates the workspace, and logs the admin credentials:
```sh
docker logs niodb
```

### Direct LLM Fallback (Zero-Daemon AI)
If native Nio CLI is uninstalled or unconfigured, NioDB automatically activates direct LLM fallback when any standard LLM environment variable is set:
- **OpenAI**: `OPENAI_API_KEY=...` (optional: `OPENAI_MODEL=gpt-4o-mini`, `OPENAI_BASE_URL=...`)
- **Gemini**: `GEMINI_API_KEY=...` (optional: `GEMINI_MODEL=gemini-1.5-flash`)
- **Ollama (Local)**: `OLLAMA_HOST=http://localhost:11434` (optional: `OLLAMA_MODEL=llama3.2`)
- **Custom OpenAI-compatible API**: `OPENAI_BASE_URL=https://...` with optional `OPENAI_API_KEY=...`

## CLI and first-run setup

The npm CLI entry point is:

```sh
npx @nio-labs/nio-db
```

On the first launch in a folder, the CLI asks only for a data directory. Press Enter to accept `./nio-db`. Each server has one workspace; there is no workspace ID to enter or pass to the API. The server uses `127.0.0.1:7432` automatically; use `--listen` or `NIODB_LISTEN` to override it. It detects an installed native Nio on PATH or in `~/.local/bin`. If Nio is missing, the pinned `@nio-labs/nio-ai` launcher downloads its checksummed native binary into its versioned user cache. No global npm install is needed.

Setup creates two bearer credentials, a **client token** and a **secret**, and writes them privately to `DATA_DIR/client-token` and `DATA_DIR/secret-token`. Only their SHA-256 hashes are stored in `auth.json`. Both currently have full backend access and use `Authorization: Bearer TOKEN`. The names describe intended use, not separate permissions: neither token is safe to embed in a public web or mobile app. Use an app-user session there instead. The CLI saves the directory and address in `.nio-db.json` in the current folder. Existing `dev-token` credentials are preserved as the client token and receive a new secret on the next launch. Provider/model configuration remains a Nio task.

The package exposes `nio-db` and the compatibility alias `niodb`. Flags such as `--dir`, `--listen`, and `--nio-bin` override detection or saved settings. Explicit offline commands (`init-auth`, `add-secret`, `backup`, `--help`, `--version`) do not install Nio or run first-launch setup.

For scripts, provision credentials with `init-auth`, or pass `--yes` to accept first-run defaults and create both tokens without prompts. Set `NIODB_NO_INSTALL_NIO=1` to keep core storage available without automatically installing Nio. A custom `--auth-file` must already exist or be provisioned using `init-auth`.

### Run from source now

Requires Rust 1.89+ and Node 18+.

```sh
npm install --omit=optional
cargo build --release --locked
node bin/niodb.cjs
```

This launches the same first-run flow. If this folder already contains `nio-db/auth.json`, it starts directly with those credentials. `--omit=optional` skips the platform binary packages; the launcher uses the locally built release binary.

For explicit credential provisioning:

```sh
node bin/niodb.cjs init-auth --dir ./nio-db --name app
```

`init-auth` prints JSON with `client_token` and `secret_token` once and refuses to overwrite an existing credential file. Store both securely; the server stores only their SHA-256 hashes. First-run setup captures them into private token files instead of printing them. Use `node bin/niodb.cjs add-secret --dir ./nio-db` to add or rotate the secret on an existing credential file; the launcher updates `secret-token` and prints the new secret once. Restart a running server after changing credentials.

Run `node bin/niodb.cjs --help` for configuration flags.

NioDB checks CLI compatibility, configured model and enabled capability catalogs at startup. Configure Nio through its CLI and restart NioDB after changes. If installed only in the managed cache, the startup output shows its executable path; `npx @nio-labs/nio-ai` also uses that versioned cache. `NIO_CONFIG` selects the source Nio configuration. Ready means compatible and configured, rather than a successful provider request. Core record operations remain available when Nio or AlaSQL is unavailable.

## API examples

For a local shell, load either stored token without printing it: `NIODB_TOKEN=$(cat ./nio-db/client-token)`. The secret is in `./nio-db/secret-token`. Both have full backend access.

```sh
curl http://127.0.0.1:7432/health
curl http://127.0.0.1:7432/api/v1/status \
  -H "Authorization: Bearer $NIODB_TOKEN"

curl 'http://127.0.0.1:7432/api/v1/records' \
  -H "Authorization: Bearer $NIODB_TOKEN" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: task-1' \
  -d '{"collection":"tasks","data":{"title":"Review API","status":"pending"}}'

curl http://127.0.0.1:7432/api/v1/query \
  -H "Authorization: Bearer $NIODB_TOKEN" -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT id, title FROM tasks WHERE status = ? LIMIT 20","parameters":["pending"]}'

curl http://127.0.0.1:7432/api/v1/assist \
  -H "Authorization: Bearer $NIODB_TOKEN" -H 'Content-Type: application/json' \
  -d '{"message":"What tasks are pending?"}'
```

Use the returned `conversation_id` for follow-up questions. Conversations belong to the authenticated principal and workspace. Each response has `X-Request-ID`; errors contain the same identifier. Public routes are `/health`, `/guide`, `/doc`, `/openapi.yaml`, and POST `/auth/login` (also `/api/v1/auth/login`). All other routes require a bearer token. Remote deployments should use an HTTPS reverse proxy and persistent server storage.

### Starter demos

A new database includes one welcome record, one `niodb_demo` user, one `welcome.txt` file, one TypeScript worker using `fetch()` against `/health`, and one `demo.ping` event. File storage also creates a file metadata record. The random demo password is saved privately in `<data-directory>/demo-user.json`.

Demos are created once and are not duplicated or recreated after deletion. The starter event appears on SSE connection; other events remain live without replay. Publish `demo.ping` to run the worker again. Workers require Node 22+ with TypeScript stripping support.

Existing databases are preserved. Start with `--seed-demo` to add examples once, or run `NIODB_SEED_DEMO=1 node scripts/dev.cjs` during development. For a new database without demos, use `--no-demo` or `NIODB_NO_DEMO=1`.

### Guides

Open [http://127.0.0.1:7432/guide](http://127.0.0.1:7432/guide) for detailed setup, token and user flows, records and collections, SQL, Nio assistance, files, chat, live events, workers, webhooks, capabilities, backup and current limits. It includes copyable requests, response shapes and permission rules. The page is embedded in the Rust executable and needs no separate documentation process. Google Sans Code loads from Google Fonts when the browser has internet access, with a system font fallback. `/docs` and `/docs/` redirect to `/guide`; the guide links to the interactive API reference at `/doc`.

### Interactive documentation

Open [http://127.0.0.1:7432/doc](http://127.0.0.1:7432/doc) for a single Swagger UI page with endpoint details and **Try it out**. Click **Authorize** and paste your bearer token (without the `Bearer ` prefix) to call protected endpoints. Authorization is kept in memory and cleared on reload. Requests go to the same NioDB server.

The page is embedded in the Rust executable; its version-pinned Swagger UI JavaScript and CSS load from a CDN, so your browser needs internet access. There is no additional server dependency or frontend build. Swagger's remote validator is disabled. Configuration follows the [official Swagger UI documentation](https://swagger.io/docs/open-source-tools/swagger-ui/usage/configuration/).

### SQL surface

The main API uses **records** grouped by **collection**. For example, create a record in the `tasks` collection with `POST /api/v1/records` and `{ "collection": "tasks", "data": { "title": "Review API" } }`. List with `GET /api/v1/records?collection=tasks` and read with `GET /api/v1/records/{id}`. Existing `/api/v1/artifacts` routes remain as compatible aliases; they use `type` instead of `collection` in responses.

Safe collection names become SQL tables. The `records` and legacy `artifacts` SQL tables contain every authorized record. The `files` collection is managed exclusively by the storage API. Top-level data fields are flattened. Reserved server fields `id`, `collection`, `type`, `revision`, `created_at`, and `updated_at` override colliding data fields.

Record CRUD supports complete **Create** (`POST /api/v1/records`), **Read** (`GET /api/v1/records` to list, or `GET /api/v1/records/{id}` to fetch one), **Update** (`PATCH` or `PUT /api/v1/records/{id}`), **Delete** (`DELETE /api/v1/records/{id}`), **Bulk** operations (`POST /api/v1/records/bulk`), **Vector Similarity Search** (`POST /api/v1/records/search`), and **Real-time Live Watch** (`GET /api/v1/records/watch?collection=...` via SSE).

### Row-Level Security (RLS)
When called by an App User session (via `POST /auth/login`), created records are automatically stamped with `owner_id = user.id`. Users can read, update, and delete only their own records, plus any records explicitly marked `is_public: true`. Non-owners cannot mutate or access private records. Backend bearer tokens (client token and secret) maintain full admin visibility and mutation rights across all records.

### Model Context Protocol (MCP) for Coding Agents
NioDB includes native MCP support for AI coding assistants (Claude Desktop, Cursor, Windsurf, Zed, Aider):
- **Stdio Transport**: Run `npx @nio-labs/nio-db mcp` (or `node bin/niodb.cjs mcp`). Configure it directly in your agent's MCP config:
  ```json
  {
    "mcpServers": {
      "niodb": {
        "command": "node",
        "args": ["/path/to/nio-db/bin/niodb.cjs", "mcp"]
      }
    }
  }
  ```
- **HTTP Endpoint**: `POST /mcp` (and `/api/v1/mcp`) supporting JSON-RPC 2.0 with tools:
  - `niodb_create_record`: Store documents, memory, or tasks with optional TTL.
  - `niodb_search_records`: Semantic vector similarity search.
  - `niodb_query_sql`: Safe read-only SQL queries with filters, joins, and aggregates.
  - `niodb_get_session_manifest`: Distilled context manifest for multi-agent handoffs.
  - `niodb_log_dead_end`: Log failed hypotheses to prevent agents from repeating loops.

The restricted compiler accepts SELECT, DISTINCT, columns, aliases, up to two INNER/LEFT JOINs, WHERE comparisons/LIKE/IS NULL with AND/OR, GROUP BY, ORDER BY, LIMIT and OFFSET. Functions are COUNT, SUM, AVG, MIN, MAX, LOWER, UPPER, and LEN. Identifiers use ASCII letters, digits and underscores, beginning with a letter or underscore; unsafe prototype names are rejected. Use single-quoted strings or scalar `?` parameters. Raw user SQL is validated and reconstructed before AlaSQL receives it. File/network sources, arbitrary functions, JavaScript, writes and multiple statements are rejected.

Natural-language queries use `POST /api/v1/assist` with a `message` and optional `conversation_id`. Plans currently support collection, literal text search, scalar equality filters, recency and up to 20 results. Nio receives a bounded schema and retrieved context, then returns a structured answer with validated record references. Aggregate and join requests use the SQL API for now. Natural-language assistance is read-only.

## App authentication

Your backend creates users with its secret or client token. Browser/mobile clients log in using username and password; they receive their own seven-day bearer session. Sessions and logout revocation persist across restart. Users can access their own records and public records; file ownership rules apply to buckets. Chat conversations belong to their individual user identity. Keep both backend tokens off public clients; either can register and delete users, while user sessions cannot.

```sh
# Backend provisions an account using NIODB_TOKEN.
curl http://127.0.0.1:7432/auth/register \
  -H "Authorization: Bearer $NIODB_TOKEN" -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"example-long-password"}'

# App signs in; save access_token from the response.
curl http://127.0.0.1:7432/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"example-long-password"}'

curl http://127.0.0.1:7432/auth/me -H "Authorization: Bearer $USER_TOKEN"
curl -X POST http://127.0.0.1:7432/auth/logout -H "Authorization: Bearer $USER_TOKEN"

# Backend deletes a user by the id returned from registration.
curl -X DELETE http://127.0.0.1:7432/api/v1/auth/users/USER_ID \
  -H "Authorization: Bearer $NIODB_TOKEN"
```

The versioned equivalents are `/api/v1/auth/register`, `/login`, `/me`, and `/logout`. Usernames are trimmed and lowercased, with 3–64 ASCII letters, digits, dots, hyphens or underscores. Registration passwords must be 12–1024 UTF-8 bytes. Password storage uses the cached RustCrypto PBKDF2 implementation with HMAC-SHA256, 600,000 rounds, random 16-byte salts and constant-time comparisons, following the PBKDF2 work factor in [OWASP's password storage guidance](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html). Argon2 is not currently available in the offline build environment. Tokens are random opaque values; only their SHA-256 hashes are journaled. New databases include a demo user with a randomly generated password saved privately in `demo-user.json`. There are no shared default passwords, cookie sessions, password reset, email verification, refresh tokens or general record ACLs in this initial contract. Deleting a user revokes their sessions and removes their linked files and chat histories; shared files and general records remain.

For a web app on another origin, set an exact comma-separated allowlist before starting:

```sh
NIODB_CORS_ORIGINS=http://localhost:5173 node bin/niodb.cjs
```

Use bearer Authorization headers. The default has no cross-origin allowlist. This is one shared app database; clients never send `workspace_id`. Existing single-workspace credentials and records are reused internally. Legacy credentials and data must each resolve to a single scope. Multiple distinct scopes require export/migration before startup; data is not deleted or merged automatically. Fresh credentials for a restored single-workspace database automatically use its existing internal scope.

## Files and buckets

Buckets are created explicitly or on the first upload. File content is streamed to a private temporary file, hashed, synced and stored under a bucket-specific content hash before its journal metadata is committed. Identical bytes deduplicate within a bucket; each upload still has a separate metadata record. Logical references count toward quota even when bytes deduplicate. Downloads are attachments and support a single HTTP bytes Range, suffix ranges and If-Range ETags. Upload with optional `user_id=USER_ID` to link a file to a user. An unlinked file is accessible to every authenticated caller. Only the linked user's session can list, read or delete a linked file; this also applies to its record, SQL queries and Nio context. A user session may link a new file only to itself; the backend may link it to any existing user.

```sh
curl http://127.0.0.1:7432/api/v1/storage \
  -H "Authorization: Bearer $USER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"documents"}'

curl http://127.0.0.1:7432/api/v1/storage/documents/upload \
  -H "Authorization: Bearer $USER_TOKEN" -F 'file=@./notes.txt'

# Private upload using the id from /auth/me or registration:
curl 'http://127.0.0.1:7432/api/v1/storage/documents/upload?user_id=USER_ID' \
  -H "Authorization: Bearer $USER_TOKEN" -F 'file=@./private.txt'

# Raw upload alternative:
curl 'http://127.0.0.1:7432/api/v1/storage/documents/upload?filename=notes.txt' \
  -H "Authorization: Bearer $USER_TOKEN" -H 'Content-Type: text/plain' \
  --data-binary @./notes.txt

curl http://127.0.0.1:7432/api/v1/storage/documents \
  -H "Authorization: Bearer $USER_TOKEN"

# Use the uploaded record's id:
curl http://127.0.0.1:7432/api/v1/storage/documents/FILE_ID \
  -H "Authorization: Bearer $USER_TOKEN" -o notes.txt
curl http://127.0.0.1:7432/api/v1/storage/documents/FILE_ID/metadata \
  -H "Authorization: Bearer $USER_TOKEN"
curl -X DELETE http://127.0.0.1:7432/api/v1/storage/documents/FILE_ID \
  -H "Authorization: Bearer $USER_TOKEN"
```

Uploads allow one multipart `file` field or a raw body with `filename`. Files are limited to 16 MiB, uploads to 60 seconds and four concurrent operations. Bucket names use 1–63 lowercase ASCII letters, digits, hyphens or underscores; reserved filesystem device names are rejected. The maximum is 10,000 file metadata records. Filenames are metadata and cannot contain path separators or control characters. File metadata becomes a record in the `files` collection for AlaSQL/Nio; query `file_id` for the download ID and `id` for the record reference. File responses expose `record_id`; `artifact_id` remains as a compatibility alias. Binary contents are not automatically sent to Nio. Startup verifies referenced blob hashes and removes orphaned blobs or unfinished uploads.

## Live events and webhooks

Authenticated clients can publish events and subscribe to the live stream. `POST /api/v1/events` accepts an event `name` and object `data`, then returns `202` with an event ID. Publishing is limited to 100 events per second per server. `GET /api/v1/events/stream?name=orders.created` subscribes to one name; omit `name` or use `*` for every event. This is an authenticated Server-Sent Events stream. Use a fetch client or `curl -N` that can send an Authorization header. The `message` event contains the full JSON event; a `gap` event reports missed messages when a slow subscriber falls behind. Events are live only: the stream has no history or replay after disconnect or restart. Any authenticated user may broadcast to connected users, so send only shared event data. A future `channel` field can group events into rooms.

```sh
curl -N 'http://127.0.0.1:7432/api/v1/events/stream?name=orders.created' \
  -H "Authorization: Bearer $USER_TOKEN"

curl http://127.0.0.1:7432/api/v1/events \
  -H "Authorization: Bearer $USER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"orders.created","data":{"order_id":"123"}}'
```

In a browser, use `fetch` because the native `EventSource` API cannot attach a bearer Authorization header:

```js
const response = await fetch('/api/v1/events/stream?name=orders.created', {
  headers: { Authorization: `Bearer ${userToken}` }
});
if (!response.ok) throw new Error(`Subscription failed: ${response.status}`);
const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
let pending = '';
for (;;) {
  const { value = '', done } = await reader.read();
  if (done) break;
  pending += value;
  let end;
  while ((end = pending.indexOf('\n\n')) !== -1) {
    const frame = pending.slice(0, end);
    pending = pending.slice(end + 2);
    const data = frame.split('\n').find(line => line.startsWith('data:'));
    if (data) console.log(JSON.parse(data.slice(5)));
  }
}
```

The backend credential can register up to 64 TypeScript workers and 64 webhooks. Each registration chooses an exact event name or `*`. Workers run asynchronously in a separate Node process with the published event as their only function argument. They require Node 22+ and an `export default` function using TypeScript syntax supported by Node's built-in type stripping. Worker source is trusted administrator code with the server process's privileges, so only a trusted backend should register it. A worker has a five-second time limit, a 64 MiB Node heap limit and two execution slots. Event publishing does not wait for it. Worker stdout and stderr are discarded; failures appear as a short server log entry.

```sh
curl http://127.0.0.1:7432/api/v1/events/workers \
  -H "Authorization: Bearer $NIODB_TOKEN" -H 'Content-Type: application/json' \
  -d '{"event_name":"orders.created","source":"export default async function(event: { name: string; data: { order_id: string } }) { if (!event.data.order_id) throw new Error(\"order_id required\"); }"}'

curl http://127.0.0.1:7432/api/v1/webhooks \
  -H "Authorization: Bearer $NIODB_TOKEN" -H 'Content-Type: application/json' \
  -d '{"event_name":"orders.created","url":"https://example.com/niodb-events"}'
```

Webhook creation returns a random `secret` once. Keep it on the receiving server. Each POST sends the event JSON with `X-NioDB-Event`, `X-NioDB-Delivery` and `X-NioDB-Signature: sha256=<hex HMAC-SHA256>`; the HMAC key is the returned secret string, applied to the exact request body. Delivery has a five-second timeout, no redirects, and up to three attempts while the server runs. Only HTTPS targets are accepted, except HTTP loopback for local development. A published event is accepted before worker/webhook completion, and failures are logged; delivery is not durable. Use `GET` and `DELETE` on `/api/v1/events/workers` or `/api/v1/webhooks` (with `/:id` for deletion) to manage registrations. Workers also support `GET /api/v1/events/workers/:id` for source details and `PATCH /api/v1/events/workers/:id` to update the event name or source. See the [worker examples in the guide](http://127.0.0.1:7432/guide#workers) for REST `fetch()` integrations. Worker source and webhook secrets are stored in the private journal and omitted from list responses.

## Chat with Nio

`POST /chat` (also `/api/v1/chat`) supports ordinary conversation, general knowledge, app context and database retrieval:

```sh
curl http://127.0.0.1:7432/chat \
  -H "Authorization: Bearer $USER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"message":"Summarize the pending tasks for this screen","fields":{"screen":"task overview"},"queries":[{"sql":"SELECT id, title FROM tasks WHERE status = ? LIMIT 10","parameters":["pending"]}]}'
```

Only `message` is required. `fields` is an optional JSON object with app context; `queries` is up to three restricted SQL queries executed by NioDB. Nio can plan a read query itself when additional stored data is needed. The response includes status, message, conversation_id and validated record references. Each reference exposes `record_id`; `artifact_id` remains as a compatibility alias. Send conversation_id to continue. General conversation can work without AlaSQL; database queries require it. App fields and query results are treated as untrusted content rather than tool instructions. No host-file reads, model mutations or plugin execution are enabled.

## Nio skills and plugins

Grant capability names when provisioning credentials:

```sh
node bin/niodb.cjs init-auth --dir ./nio-db \
  --name app --skill research --plugin pdf
```

Use actual installed capability names. `/api/v1/skills` and `/api/v1/plugins` list only enabled, granted entries. Catalogs refresh on restart. Paths, executables and provider credentials are excluded from responses.

Small granted `SKILL.md` files inside Nio's configured skills directory can guide answers. Files above 8 KiB and guidance above the combined 2 KiB prompt budget are omitted. These skills cannot execute tools or expand data access. App-user sessions currently have no skill/plugin grants; backend credentials retain explicit capability grants. Plugins currently provide discovery metadata only; executing them still requires a controlled process and file-handle contract on top of the implemented blob storage.

Each Nio invocation uses an isolated directory and configuration, `--mode ask --no-tools`, bounded output and a timeout. Prompt attachment syntax is escaped to avoid Nio's automatic host-file attachment behavior. Database records and historical messages are treated as untrusted input.

## Storage and backup

`journal.jsonl` is the recovery authority. TOON files in `artifacts/` are derived, human-readable projections using a basic encoder. They are not an editable database interface; TOON decoding/import and full codec conformance are future work. Missing or mismatched projections are regenerated from the journal at startup.

One server owns a data directory. Stop it before taking an offline backup:

```sh
node bin/niodb.cjs backup --dir ./nio-db --output ./backup.jsonl
```

The journal-only command acquires the same exclusive lock and refuses to overwrite the destination. **If you have uploaded files, use the full backup below**; a journal alone cannot restore blobs. To restore a journal-only database with no files, stop the server, create a fresh private directory, copy the backup to `journal.jsonl`, and copy the separately backed-up `auth.json` or provision fresh credentials. Start the server against that directory. Do not overwrite a running or existing database journal. Nio provider configuration is managed separately from database backups.

An incomplete final journal record is truncated on recovery. Complete corrupt records stop startup. Back up credentials separately; removing a principal and restarting revokes its access. Backend credentials have no automatic expiry. App-user sessions expire after seven days and can be revoked with logout.

### Full backup with files

Stop the server, then run:

```sh
node bin/niodb.cjs backup --dir ./nio-db --include-files --output ./nio-db-backup
```

The destination must be a new directory. It contains the consistent journal and every referenced blob; the source directory's exclusive lock stays held during the copy. Copy your separately backed-up backend `auth.json` into a restored directory, or provision fresh backend credentials. To restore, copy the backup directory to a new private data directory and start against it. It recreates TOON projections and verifies blob hashes. Password hashes and app sessions are in the journal, so treat backups as private. Provider configuration is separate. Do not restore while a server is running.

## Web Console Dashboard

NioDB includes a built-in administration web console served directly by the server. Open your browser and navigate to [http://localhost:7432](http://localhost:7432) (or [http://localhost:7432/console](http://localhost:7432/console)) to access the dashboard.
It is styled with Vue 3, Shadcn design tokens (unified teal theme, light mode by default with dark toggle), and Google Sans Code font.

- **Data & Query Explorer**: Interactive data grid, collection filters, search, pagination, detailed JSON drawer, new record creation, JSON export, AlaSQL Query Studio (`>_`), and Natural Language Intelligence planning.
- **SQLite Import & Export**: Directly import `.sqlite`, `.db`, or `.sqlite3` database files in-browser via the official SQLite WebAssembly engine (`sql.js`), inspect table schemas with row counts, customize collection mapping, and batch-import rows. Export records to a binary `.sqlite` database file with one click.
- **Files & Storage Buckets**: Browse storage buckets, inspect file sizes/content types/owners, drag & drop file uploads, downloads, and deletion.
- **Live Events & Webhooks**: Real-time SSE event stream viewer (`/api/v1/events/stream`), event publishing, webhook subscriptions, and TypeScript worker registration.
- **Auth & User Directory**: List database users, create accounts, test user logins, and manage bearer tokens.
- **Config & System Status**: Real-time health monitoring (`/health`), AlaSQL query engine status, Nio CLI intelligence status, principal skills, and discovered plugins.

## Client SDKs

NioDB provides zero-dependency, ultra-lightweight client SDKs:

- **TypeScript / JavaScript**: [`@nio-labs/nio-db.js`](packages/nio-db.js) (< 8 KB, browser, Node.js, Bun, Deno, React Native).
- **Python**: [`nio-db-py`](packages/nio-db-py) (zero dependencies, typed, Python 3.8+).

## Benchmark

Measured on **2026-10-05** with release builds on WSL2 Linux, an Intel Core Ultra 5 135H, 18 logical CPUs, 7.22 GiB RAM, Node 24.18.1, and an ext4 filesystem. The same authenticated loopback HTTP runner sent one request at a time to each server. It parsed every response and verified record counts, SQL results, and the SHA-256 hashes of four file downloads. The fixture contains 10,000 records: 5,000 telemetry, 3,000 customers, and 2,000 with 8-dimensional vectors, plus four file metadata records. Inserts use 20 transactions of 500 records. SQL uses two grouped aggregates and one sorted query. Reported read latencies include 20, 5, 5, and 3 warm-up requests for lookups, scans, vectors, and SQL, respectively.

| Operation | NioDB before changes | NioDB current | SQLite adapter |
| --- | ---: | ---: | ---: |
| Bulk ingestion, records/s | 324 | 13,311 | 29,808 |
| Bulk batch P50, 500 records | 1,535 ms | 37.6 ms | 16.0 ms |
| Record lookup by ID P50 | 2.15 ms | 1.92 ms | 1.88 ms |
| Collection scan P50, 5,000 records, return 50 | 11.53 ms | 4.15 ms | 2.83 ms |
| Vector search P50, 2,000 × 8D, top 10 | 5.67 ms | 3.33 ms | 7.09 ms |
| SQL P50, grouped aggregates and sorted query | 274.28 ms | 31.41 ms | 2.67 ms |

The **before** and **current** NioDB columns ran the same benchmark script and fixture. The SQLite column uses a small Rust/Axum HTTP adapter in [src/bin/sqlite-benchmark.rs](src/bin/sqlite-benchmark.rs), with SQLite 3.46.1. It stores records in SQLite tables, uses `WAL` and `synchronous=FULL`, and commits each 500-record batch in one transaction. [SQLite documents](https://sqlite.org/pragma.html#pragma_synchronous) that this setting syncs the WAL after each transaction commit. The adapter computes exact vector similarity in Rust after scanning the SQLite rows; SQLite itself has no vector extension in this test. The adapter executes only the three measured SQL queries, using typed SQLite tables. These are **HTTP application comparisons**, not raw embedded SQLite timings or a claim that every feature has equivalent implementation. Event publishing is excluded from the comparison because the adapter only acknowledges an in-memory event and does not implement NioDB's worker, webhook, or SSE behavior. Neither server had event handlers or subscribers during the measured workload.

NioDB's current ingestion rate is about **41×** its previous rate in this run. The journal now commits each validated bulk batch as one checksummed frame and syncs once. One warm AlaSQL worker serves repeated SQL requests; a second starts on concurrent demand. Paged reads and vector search copy selected results rather than the entire workspace. NioDB's server RSS after SQL was **38.26 MiB**, plus **93.79 MiB** for its warm SQL worker; combined proportional set size after two idle seconds was **96.01 MiB**. SQLite adapter RSS after SQL was **12.30 MiB**, with **9.85 MiB** proportional set size. RSS snapshots are not peak memory, and process memory accounting varies by environment.

Natural-language/LLM requests, password hashing, concurrent clients, and remote network latency are outside this benchmark. Hardware, filesystem, cache state, and background activity can change the figures. See the complete [NioDB report](benchmarks/latest.json), [previous NioDB report](benchmarks/baseline-profile.json), and [SQLite report](benchmarks/sqlite.json).

### Reproduce

```sh
cargo build --release --locked
npm run benchmark
cargo build --release --locked --features sqlite-benchmark --bin sqlite-benchmark
npm run benchmark:sqlite
```

The runner creates and removes a separate temporary database on the repository filesystem; it does not modify your normal `nio-db` directory. It uses port 7488; set `NIODB_BENCHMARK_PORT` to use another port. Override either report path with `NIODB_BENCHMARK_OUTPUT`.

## Initial limits

| Resource | Limit |
| --- | --- |
| Journal | 64 GiB; vacuum compaction supported |
| Record data / HTTP body | 256 KiB / 300 KiB |
| SQL input / workspace records / serialized input | 64 KiB / 500,000 / 64 MiB |
| SQL results / timeout / JS heap | 200 rows / 5 seconds / 64 MiB |
| SQL processes / assistance requests | 2 / 2 concurrent |
| Nio invocation | 60 seconds by default; configurable 1–300 |
| Assistance schema / retrieved context | 8 KiB / 10 KiB |
| Conversation supplied to Nio | Latest four pairs within 4 KiB |
| Chat fields / queries / query result context | 4 KiB / 3 queries / 8 KiB |
| File size / upload time / upload concurrency | 16 MiB / 60 seconds / 4 |
| File references in this server’s workspace | 128 MiB / 10,000 metadata records |
| Users / active sessions | 5,000 / 10,000 |
| Login attempts / concurrent password operations | 30 per minute per server / 2 |
| Event data / live stream buffer | 16 KiB / 256 messages |
| Event publish rate | 100 per second per server |
| Worker / webhook registrations | 64 each |
| Worker / webhook concurrency | 2 / 8 |

Journal state is loaded into memory; collection scans sort visible record references and copy the returned page. The Node runtime needed by AlaSQL contributes to the deployment footprint. Mutations through Nio, plugin execution, automatic file-content extraction, durable event replay, GraphQL, chat streaming, and secondary indexes are planned work. Vacuum compaction and the administration console are implemented.

## Packaging and release

`npm run package:platform` packages a compiled host binary and SHA-256 metadata in `dist/`. Build each supported OS/architecture before publishing the matching optional packages and launcher.

## License

NioDB is open-source software licensed under the [MIT License](LICENSE).  
Copyright (c) 2026 Nio Labs.
