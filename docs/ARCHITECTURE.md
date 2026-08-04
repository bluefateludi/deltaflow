# DeltaFlow architecture

DeltaFlow is a lightweight incremental synchronization core for teams that need
reliable scheduled data movement without operating a full CDC platform.

The reference use case copies an operational `orders` table into a local
analytics database. The first run is a backfill; later runs read only inserted
or updated rows.

## Data flow

```text
SQLite orders source
  -> ordered keyset page
  -> connector-independent pipeline
  -> transactional SQLite upsert + checkpoint
```

The core deliberately knows nothing about SQL or order fields. A source returns
`Record` values in stable cursor order. A sink commits a batch and its next
cursor atomically.

## Incremental cursor

`updated_at` alone is not a safe cursor: multiple rows can have the same update
time. DeltaFlow uses the tuple `(updated_at, id)` and reads with keyset
pagination:

```sql
SELECT ...
FROM orders
WHERE (updated_at, id) > (:updated_at, :id)
ORDER BY updated_at, id
LIMIT :batch_size;
```

This avoids both offset-scan cost and missing rows at timestamp boundaries.

## Delivery semantics

The local reference implementation provides effectively-once results:

1. The source may return a batch more than once after a crash.
2. The sink upserts by business key, so replay is safe.
3. The sink advances its checkpoint in the same transaction as the upserts.
4. Older `updated_at` values cannot overwrite a newer target row.

It is therefore at-least-once processing plus idempotent application, rather
than a claim of distributed exactly-once delivery.

## MVP boundaries

- One source and one sink per command.
- SQLite reference connectors; connector protocols allow MySQL/PostgreSQL later.
- Inserts and updates; hard deletes require tombstones or a CDC log.
- ISO-8601 UTC timestamps whose textual ordering matches chronological ordering.
- Schema mapping, DLQ, secrets management and metrics exporters are future work.

## Acceptance scenarios

- Initial backfill writes every source order.
- A second run reads only new or updated source rows.
- Rows sharing the same timestamp are not skipped across batch boundaries.
- Replaying a committed batch does not duplicate target rows.
- A simulated interruption resumes at the last committed cursor.
- A stale source version does not overwrite a newer target version.
