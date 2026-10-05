# NioDB — The Agentic DB that works.

_A lightweight database with natural-language queries, powered by Nio._

## Agreed direction

NioDB is a standalone Rust server distributed through a small npm launcher, ultimately `npx @nio-labs/nio-db`. It serves Nio and independent web/mobile applications over HTTP. There is no embedded client store.

Nio CLI is the intelligence layer: human-language interpretation, clarification, retrieval planning, summaries and grounded answers. AlaSQL is mandatory and runs through a bounded Node helper. JSON provides HTTP interoperability. TOON provides readable artifact projections; the initial recovery authority is a checksummed JSONL journal. A full TOON codec and import contract remain future work.

The historical vision is preserved in [NIODB_ORIGINAL_VISION.md](docs/NIODB_ORIGINAL_VISION.md). It contains superseded Node-server assumptions and proposed features. This document and [README](README.md) describe the current direction; [OpenAPI](openapi.yaml) defines the initial HTTP contract.

## Architecture

```text
Nio / AI agents / web and mobile apps
                         |
                   authenticated HTTP
                         |
                  Rust NioDB server
                 /        |         \
        durable journal   |     scoped AlaSQL helper
        TOON projections  |           (Node)
                          |
                    installed Nio CLI
                  approved skill guidance
                  plugin catalog discovery
```

Rust owns authentication, shared server scope, durable commits, conversation ownership, bounded context and API contracts. Nio proposes a bounded read plan; the server validates and executes it, then gives Nio authorized records for the answer. Model output never grants database access. Assistance is optional for core storage availability, while remaining central to the product experience.

## Implemented initial slice

- Record create/read/list through `/api/v1/records`, IDs, timestamps, pagination and persistent scoped idempotency. The older `/api/v1/artifacts` contract remains available.
- Two backend bearer credentials (client token and secret) with equal full access and capability grants; backend-managed username/password app users and seven-day revocable bearer sessions.
- Exclusive data-directory lock, checksummed append-only journal, crash-tail recovery, offline backup and TOON projections.
- Restricted AlaSQL SELECT compiler with scalar parameters, joins and aggregates; no external sources, arbitrary JavaScript or SQL writes.
- Nio startup checks, isolated read-only invocations, bounded natural-language plans, owned conversations, timeouts and validated references.
- Enabled skill/plugin catalog discovery filtered by principal grants; bounded approved skill guidance in prompts.
- Bucket storage with streaming raw/multipart uploads, byte-range downloads, metadata, listing, deletion, CAS deduplication and consistent offline file backups.
- Optional user-owned files enforced across storage routes, artifacts, SQL and Nio; backend user deletion revokes sessions and removes linked files and chat histories.
- Authenticated live event broadcast over SSE, persistent backend-managed TypeScript worker/webhook registrations and asynchronous delivery.
- `/chat` and `/api/v1/chat` with optional app fields, up to three SQL queries, general conversation and owned history.
- Configurable origin allowlist for web app bearer requests.
- Self-contained `/docs/` guides, interactive `/doc` OpenAPI page, status/errors, graceful shutdown, npm CLI and host binary packaging script.
- `@nio-labs/nio-db` first-run prompts, saved directory/address settings (only the directory is prompted), private client-token and secret provisioning, installed-Nio detection and automatic native installation through the Nio npm launcher.

AlaSQL is installed and reports ready; full engine behavior remains unverified. The pinned Nio npm installer and missing-Nio download path need registry verification because shell registry access is blocked. Core implementation and fixtures must not be presented as verification of the real engine. npm publication and cross-platform binaries are outstanding.

## Skills and plugins

Nio's installed capabilities are part of the design. A capability must be enabled in Nio and granted in the NioDB credential before it appears to a caller. Discovery reveals safe metadata only. Small approved skill documents may guide interpretation and answers; permission checks and structured output remain enforced by Rust.

Plugin execution requires explicit invocation schemas, managed file handles, output/size/time limits, cancellation, audit records and a process isolation model. It builds on the implemented managed blob/file storage. Merely installing a plugin does not authorize execution. A skill that needs shell tools cannot execute them through the initial assistance endpoint.

Future Nio mutations require a concrete preview, explicit approval, revision checks, idempotency and an audit trail. Do not expose raw CLI command strings or an unrestricted model-generated SQL/tool interface.

## Integration contracts

All Nio products use the same authenticated server API and this server’s shared database. Each server has one workspace. APIs and setup prompts do not expose or require workspace IDs. Existing internal scopes are retained for journal compatibility; startup rejects credentials spanning multiple distinct workspaces. Conversations and idempotency are scoped to principal/workspace. Product-specific records use collection names and JSON data fields; storage internals are not client APIs.

Integrations will use retry/idempotency and event contracts. File and media use cases depend on managed blobs and capability execution contracts. Nio CLI compatibility is checked at startup; provider connectivity is established by actual assistance calls rather than inferred from configuration.

## Next phases

The proposed [structured query API](docs/STRUCTURED_QUERY_API.md) adds bounded filters, sorting, pagination, and aggregates executed directly in Rust alongside SQL. The proposal includes the HTTP contract, SDK example, authorization rules, indexing plan, and benchmark checks; implementation remains future work.

1. Install and lock npm dependencies; verify real AlaSQL behavior, joins, aggregates and rejection boundaries. Build the host release and remaining platform matrix. Finalize licensing before publishing.
2. Add journal compaction, indexes, storage quotas, migration/version policy, complete TOON conformance and explicit import/export.
3. Add controlled blob extraction and bounded plugin execution using managed file handles. Bucket storage and backups are now implemented.
4. Add preview/approval contracts for agent mutations, jobs, streaming and auditable capability invocations.
5. Add durable event replay and webhook delivery queues, and optional GraphQL. The administration web console dashboard (Vue 3 + Shadcn + SQLite import/export) is implemented at `/console` and `/`.

See README for operational limits and commands. This initial implementation is not the complete historical product vision.

## App auth and shared data

The backend registers and deletes users using either client token or secret; both are full-access bearer credentials and must stay off public clients. Public signup is not enabled. Login accepts username/password only and returns a seven-day session token. me/logout require a user session. Users share this one server’s general artifacts and buckets; conversations are user-owned. Files may have a user owner and are then accessible only to that user's session, including their queryable metadata. Deleting a user revokes sessions and removes linked files and conversations. Password hashing uses PBKDF2-HMAC-SHA256 with 600,000 rounds and random salts. Backend credentials remain separate and do not expire automatically. General record ACLs, password reset, refresh tokens and email verification need separate contracts.

## Files and chat

Buckets are server-scoped. Raw and single-file multipart uploads stream through private staging into content-addressed bucket storage. Optional user IDs link private files to an existing user; unlinked files remain shared among authenticated callers. File metadata is queryable as files artifacts only by callers allowed to see that file; content extraction is not automatic. Full backup copies a consistent journal and referenced blobs under the data lock. Chat supports general conversation, fields, supplied read SQL queries and Nio-planned reads with validated references. No user-supplied workspace IDs, agent writes or arbitrary host-file access are allowed.

## Events and webhooks

Authenticated callers publish object data under an event `name`. Subscribers use an authenticated SSE stream, optionally filtered by `name`, and receive live messages or gap notices; events are not journaled or replayed. `channel` is reserved for future rooms. Registrations for up to 32 TypeScript workers and 32 webhooks are journaled and managed only by backend credentials, using `event_name` or `*`. Workers are trusted administrator code executed asynchronously in a bounded Node 22+ subprocess. Webhooks sign delivery bodies with a generated secret and retry three times while the server runs. Publishing acknowledges dispatch, not completion. A durable delivery queue, worker isolation from host privileges and event ACLs remain future work.

Current limits and examples are in README. These changes were compiled. Integration tests have not been run for this task.
