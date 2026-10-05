# Structured query API proposal

Status: proposed. This document describes the next implementation; the endpoint and SDK methods below are not available yet.

## Purpose

Add a structured, read-only query API alongside SQL. Execute common filters, sorting, pagination, and aggregates directly in Rust so applications and Nio can query records without transferring the dataset to Node/AlaSQL.

The current benchmark measured SQL P50 at 31.41 ms for NioDB and 2.67 ms for the SQLite HTTP adapter. NioDB currently serializes all authorized records for each SQL request, transfers them to Node, and rebuilds collection tables. These figures describe the measured workload; they do not establish a latency target for the proposed API. See [the benchmark methodology](../README.md#benchmark).

## HTTP request

Proposed endpoint: `POST /api/v1/records/query`, with the same bearer authentication as the existing records API.

```json
{
  "collection": "customers",
  "where": {
    "all": [
      { "field": "plan", "op": "eq", "value": "pro" },
      { "field": "credits", "op": "gte", "value": 500 }
    ]
  },
  "select": ["id", "name", "credits"],
  "order_by": [{ "field": "credits", "direction": "desc" }],
  "limit": 20
}
```

The collection identifies the candidate records. Data fields use their top-level names. Reserved fields such as `id`, `collection`, `revision`, `created_at`, and `updated_at` refer to server metadata and cannot be overridden by record data.

The response includes only selected fields when `select` is supplied; otherwise, it returns the existing record representation. A continuation cursor supports pagination.

```json
{
  "items": [
    { "id": "art_example", "name": "Example customer", "credits": 750 }
  ],
  "has_more": false,
  "next_cursor": null
}
```

Do not compute a full matching count automatically. Fetch at most `limit + 1` results to determine `has_more`; provide exact counts through the aggregate operation. Apply explicit sorting before selecting the page, with `id` as a stable tie breaker. Without a requested order, use `created_at` descending and `id` ascending. Sorting can still require scanning and sorting candidates until an appropriate index exists.

## SDK example

```js
const result = await db.collection('customers').find({
  where: {
    all: [
      { field: 'plan', op: 'eq', value: 'pro' },
      { field: 'credits', op: 'gte', value: 500 }
    ]
  },
  select: ['id', 'name', 'credits'],
  orderBy: [{ field: 'credits', direction: 'desc' }],
  limit: 20
});
```

The SDK translates its camelCase options into the HTTP contract. Queries are JSON descriptions rather than JavaScript callbacks or executable function source. Python should expose equivalent options using its naming conventions.

## Initial semantics and bounds

- Require one collection per query. Keep joins in the SQL API initially.
- Support `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `contains`, and `exists`.
- Support bounded `all` and `any` condition groups: at most four nesting levels and 32 conditions in total.
- Compare numbers numerically and strings case-sensitively. Do not coerce strings into numbers. Ordering comparisons require matching scalar types.
- Treat missing fields separately from explicit JSON `null`. `eq` with `null` matches explicit null only; `exists` tests field presence. `ne` requires field presence. Conditions on incompatible types do not match.
- Define `contains` as a literal substring search on strings. Keep regular expressions and arbitrary functions outside the initial contract.
- Limit `in` to 100 scalar values, selected fields to 32, and sort fields to four.
- Default to 25 returned rows; permit a maximum of 200. Reuse the existing HTTP body limit and validate the query before execution.
- Sort missing and null values last in either direction, and define a fixed type ordering for mixed values. Document that ordering before release.
- Bind opaque pagination cursors to the authenticated principal and normalized query. Reject changed query options. Document that pages across concurrent writes are not a persistent snapshot.

These proposed bounds should be checked against existing server limits during implementation and added to OpenAPI and the guide.

## Rust execution

1. Authenticate the caller and validate the complete request.
2. Obtain candidate record references for the requested collection.
3. Enforce existing visibility, ownership, file-access, and expiration rules before filtering, aggregation, or projection. A requested field or cursor cannot expand access.
4. Evaluate conditions directly against immutable records, without serializing candidates to Node.
5. Apply ordering and pagination. Use a bounded selection algorithm where possible rather than sorting every match.
6. Copy and serialize only the returned fields or records.

Start with the existing in-memory record references. This removes SQL transport overhead but still scans the collection. Add a collection index next, then selected field indexes for demonstrated workloads. Derived indexes must rebuild from the journal on startup and update on insert, update, delete, expiration, and compaction. The journal remains the recovery authority.

## Counts and aggregates

After the initial find API, add a structured aggregate operation using the same authorization and filter evaluator. Support exact `count`, `sum`, `avg`, `min`, and `max`, with optional bounded grouping. Define numeric, null, empty-set, and group-limit behavior explicitly; report exceeded limits instead of returning silently incomplete aggregates.

Example proposed aggregate request to the same endpoint:

```json
{
  "collection": "customers",
  "group_by": ["plan"],
  "aggregates": [
    { "op": "count", "as": "user_count" },
    { "op": "avg", "field": "credits", "as": "avg_credits" }
  ]
}
```

Validate find and aggregate requests as separate request variants. Aggregate requests must not silently ignore find-only options such as record projection or a pagination cursor.

## Nio integration

Extend Nio's existing bounded read-plan format to map onto the structured query evaluator. Rust validates and authorizes every model-generated plan. Keep SQL available for supported joins and queries outside the initial structured surface. Natural-language response time also includes model calls, so improving database execution alone does not establish a target for total Nio latency.

## Implementation order and verification

1. Implement the validated find endpoint and Rust evaluator, then add OpenAPI, guide, and SDK examples.
2. Add exact counts and bounded aggregates using the same evaluator.
3. Route compatible Nio plans through the evaluator.
4. Add collection and selected field indexes after measuring the remaining scan cost.

Verify ownership and file visibility, expiration, metadata collisions, mixed types, missing/null values, stable ordering, cursor isolation, and bounded requests. Benchmark equivalent filters, sorts, and aggregates through SQL and the structured API using the same fixtures and returned fields. Report P50/P95, throughput, and memory for 10,000 and larger datasets. Measure scan, sort, and serialization time separately before claiming which stage dominates.

## Related SQL improvement

Continue improving SQL independently: validate referenced tables and fields, then transfer only required authorized data to AlaSQL. Preserve joins, aliases, wildcard selection, and authorization when narrowing input. Profile first; the structured endpoint does not change SQL latency by itself.
