# Application storage contracts

These additive APIs support stable application identities, atomic coupled writes,
reference validation, and shared run ownership. Legacy `/records/bulk` and
`/records/batch` retain their existing behavior: mixed operations, updates, and
deletes can partially commit. Use `/api/v1/mutations` for atomic work.

All new endpoints require backend bearer credentials authorized for the server's
workspace. App user sessions receive `403 backend_only`. Credentials stay in the
application backend. New endpoints do not accept client workspace IDs.

## Collection policies

`PUT /api/v1/collections/{collection}/constraints` installs a durable policy:

```json
{
  "unique": ["appId"],
  "references": [
    {"field":"guruId","collection":"nioguru_gurus","target_field":"appId"}
  ],
  "lease": {"field":"appId","prefix":"conversation:"}
}
```

`GET` on that path returns `{"constraints": policy}` or `{"constraints": null}`.
Installing the same policy again succeeds without writing. Replacing or removing
an installed policy returns `409 constraint_conflict`. Install parent policies
before child policies: reference targets must have the specified unique field.
Existing records are validated before installation; failures change nothing.

Unique fields are required nonempty strings, at most 256 bytes, within each
workspace/collection. References are required strings pointing to those keys,
not NioDB record IDs. Policies have at most 16 unique fields and 16 references;
field/collection/prefix names are at most 128 bytes and exclude NUL. Reserved
metadata, `ttl`, and `expires_at` cannot be constraint field names. At most 1,000
policies may be installed per server.

Constraints apply to legacy creates, updates, deletes, and bulk operations too.
References are checked against the final transaction state, so a transaction may
create a child before its parent or delete a parent before its children. Deleting
or changing a referenced key alone fails. There is no automatic cascade.

TTL/expiry is prohibited on constrained records, because expiry must not bypass
reference or ownership checks. Installing a policy on existing TTL records fails.
Policies and data rebuild from the journal and survive vacuum and restart.
Configured unique, reference and lease fields have rebuildable in-memory equality
indexes. Native SQL uses them when a WHERE equality is required by every branch of
the predicate. Results still pass through the SQL evaluator, including visibility
and cursor checks. Indexes rebuild from the journal and vacuum snapshots. Queries
without a supported equality use the existing collection scan. Constraint
validation still scans the final state under the storage lock; benchmark write
throughput before choosing production history sizes.

## Atomic mutations

`POST /api/v1/mutations` accepts:

```json
{
  "idempotency_key":"run-123:final",
  "operations":[
    {"action":"create","collection":"nioguru_messages",
     "data":{"appId":"message-123","conversationId":"conversation-123","content":"Hello"}},
    {"action":"update","id":"art_existing","expected_revision":4,
     "data":{"updatedAt":1791432000000}}
  ],
  "leases":[{"resource":"conversation:conversation-123","owner":"run-123","fence":7}]
}
```

Actions are `create`, merging `update`, and `delete`. Updates and deletes require
`expected_revision`; a stale revision returns `409 revision_conflict`. Missing
records return `404 mutation_not_found`. Each existing record may occur once.
Creates receive server IDs; references within a batch use application keys.
There must be 1–1,000 operations and no more than 1,000 proofs. The JSON request
and serialized result limit are each 8 MiB (including merged record data). This API rejects TTL/expiry and deletion of storage-linked file
records; use the storage file API for blobs.

The response is `{"records":[Artifact...],"deleted":["art_id",...]}`. Each
Artifact has `id`, `workspace_id`, `kind`, `data`, `revision`, `created_at`, and
`updated_at`. Keep application `appId` and numeric timestamps inside `data`;
server identity/timestamps remain separate. Updates return merged records.

All operations and constraints validate before one durable journal frame commits.
A failed validation writes neither records nor a receipt. Recovery replays the
whole committed frame or discards a truncated final frame. A disk failure can
leave the acknowledgement uncertain; restart an unhealthy server and retry the
same request/key to determine its outcome. TOON projections are derived, repaired
on restart, and not the transaction's commit point. Once new policy, lease, or mutation
events are written, older binaries cannot read that journal; pin a compatible server
release and retain a backup before upgrading.

Keys are nonempty, at most 128 bytes, exclude NUL, and scoped by workspace/backend
principal. A receipt contains the ordered operations' fingerprint and original
response. The same key/operations returns that response without writes, even
when records have since changed or been deleted. Different operations conflict
with `409 mutation_conflict`. Lease proofs may change on retry; they are excluded
from the fingerprint. Replaying a completed receipt needs no active lease.
Receipt responses are retained for 30 days by default, adjustable with
`NIODB_RECEIPT_RETENTION_DAYS` (1–365). Active receipt capacity defaults to
100,000, adjustable with `NIODB_RECEIPT_LIMIT` (1–1,000,000). When full, the next
successful mutation atomically converts expired receipts to durable spent-key
hashes and commits its own receipt. A retry of an expired key returns
`409 mutation_expired` forever and never repeats the mutation. Expired keys lose
their response body, so callers must resolve older outcomes from stable application
IDs. If all active receipts fill capacity, new keys fail with `503 receipt_limit`.
Legacy receipts written before timestamps were added remain retriable indefinitely.
Compaction preserves active receipts and spent-key hashes. Spent-key hashes consume
journal and memory until a separate archival design is introduced; the 64 GiB
journal limit remains the ultimate bound. `GET /api/v1/persistence/status` reports
active receipts, spent keys, limits, active leases, pending jobs and index keys.

For small cascades, submit every dependent record and its expected revision in
one mutation. Larger cascades use the deletion job API below.
Record watch notifications publish after commit; receipt retries do not republish them.
Notifications use the existing best-effort in-memory transport, not a durable outbox.

## Fenced resource leases

`POST /api/v1/leases` accepts `resource`, `owner`, `action`, optional `fence`, and
optional `ttl_ms`. Actions are `acquire`, `renew`, and `release`.

- Resource: nonempty, at most 512 bytes, excludes NUL.
- Owner: stable unique run ID, nonempty, at most 128 bytes, excludes NUL.
- Duration: 1,000–300,000 milliseconds; default 30,000.
- Renew/release require the current owner's fence.

Response: `{"lease":{"resource":"conversation:c","owner":"run-1",
"fence":1,"expires_at":1791432000000}}`. Expiry is server UTC epoch milliseconds.
`GET /api/v1/leases?resource=...` returns `{"lease": lease_or_null, "active": true_or_false}`.

Acquisition is serialized with all writes. A competing active owner conflicts.
A retry with the same active owner returns the original lease without extending
it. Renew extends an active matching lease; release removes it. A new fence uses
the globally increasing durable journal sequence, so an old fence is never reused
even after inactive lease records are reclaimed or vacuumed. A released resource
returns null from `GET`. Active leases are limited to 100,000 by default, adjustable
with `NIODB_ACTIVE_LEASE_LIMIT` (1–1,000,000). When the map is full, expired leases
are removed in the same journal frame as a new acquisition; `503 lease_limit`
means the active capacity is exhausted. Restart preserves unexpired leases;
callers must resume them or wait for expiry.

A collection lease policy derives its required resource by concatenating `prefix`
and the record's field value. Every create/update/delete in that collection
requires a matching unexpired proof in `/mutations`. Updates check both previous
and new resources, preventing a record from moving out of an owned conversation.
Legacy write endpoints cannot supply proofs, so writes to guarded records fail
with `409 lease_required`. Reads remain available.

For NioGuru, guard conversations by `appId` and messages by `conversationId`, both
with prefix `conversation:`. Acquire before preparing a turn and renew while it
runs. Stable owner IDs identify runs, not server instances. Database fencing
prevents stale persistence; the execution bridge must separately stop a native
Nio process when ownership is lost, preventing overlapping native session writes.

Leases use the server wall clock. Operators must keep it synchronized. Backend
principals are trusted operators; proofs are ownership assertions, not standalone
credentials or authorization for another workspace.

## Restartable cascade deletion

`POST /api/v1/deletion-jobs` accepts
`{"root_id":"art_guru","idempotency_key":"delete-custom-guru"}`. The root must
belong to a configured collection. NioDB follows all installed reference rules
from that root and records a durable child-before-parent deletion plan. The plan
supports 1–100,000 records and must fit 8 MiB; larger plans return
`413 deletion_job_too_large`. Cyclic references and storage-linked file artifacts
are rejected before creating a job. A second call with the same key/root returns
the existing job; a different root conflicts. Up to 32 jobs may be pending.

Creating the job atomically marks the root and every planned descendant inaccessible
to record reads, lists, and SQL. New children and mutations of marked records fail
with `409 deletion_in_progress`; new leases for affected conversations also fail.
An active lease on any planned record prevents job creation. The server then deletes
up to 1,000 records per durable journal frame. It resumes pending jobs when the
server restarts. Progress is returned as
`{"job":{"id":"del_...","root_id":"art_guru","status":"pending",
"deleted":1000,"total":1207,"created_at_ms":1791432000000}}`.
`GET /api/v1/deletion-jobs/{id}` reports `pending` or `complete` and exact counts.
Completed job metadata remains durable for idempotent retries; its deletion plan
is released. If a storage failure pauses cleanup, restart the unhealthy server;
normal recovery resumes the job. A pending job can delay returning the affected
records, but other collections remain accessible.

This job follows configured references only. The application must configure all
relevant parent/child rules before accepting records. Its hidden parent remains
physically present until its descendants are removed; use job status to confirm
completion. This is a durable deletion protocol, not one atomic physical deletion.
Watch notifications for job chunks are not published; clients should follow job
status and refresh lists after completion.

## JavaScript SDK

ESM: `import { NioDB } from '@nio-labs/nio-db.js'`. CommonJS consumers may keep
`require('@nio-labs/nio-db.js')`. The published ESM file contains no Node imports;
`npm run build` generates it, and `prepack` ensures it matches the CommonJS source.

```js
const db = new NioDB({ url: process.env.NIODB_URL, token: process.env.NIODB_TOKEN });
await db.configureCollection('nioguru_gurus', { unique: ['appId'] });
const { lease } = await db.acquireLease('conversation:c', 'run-1');
const { resource, owner, fence } = lease;
const proof = { resource, owner, fence };
await db.mutate({ idempotency_key: 'run-1:final', operations, leases: [proof] });
const page = await db.query(
  'SELECT appId, createdAt FROM nioguru_messages WHERE conversationId = $1 ORDER BY createdAt DESC, appId DESC LIMIT 31',
  ['c'],
  { timeoutMs: 5000, signal: abortController.signal },
);
await db.releaseLease(lease);
```

For mutation proofs, pass only `{resource, owner, fence}`; the server rejects unknown
fields such as `expires_at`. SDK renew/release methods accept a full lease response
and transmit only the proof fields.

Methods: `configureCollection`, `getCollectionConstraints`, `mutate`,
`startDeletionJob`, `getDeletionJob`, `getPersistenceStatus`, `getLease`,
`acquireLease`, `renewLease`, `releaseLease`, and `query(sql, parameters, options)`.
One-argument `query(sql)` continues to work. Collection insert/find/get/update/delete
also accept request options. Constructor defaults: 30-second deadline and 8-MiB
response limit (`timeoutMs`/`maxResponseBytes`); `fetch` may be injected. Deadlines
include streamed response reads. Aborts propagate to fetch; response readers are
cancelled/released on exit. HTTP errors expose `status`, `code`, `requestId`, and
`data`. No automatic retries occur: callers select stable mutation keys.

NioJS adoption still requires its authenticated fetch, AbortController, readable
streams, encoding, and timer support. Node/ESM checks do not establish NioJS parity.
