# DeltaFlow reliability

DeltaFlow's SQLite reference path provides recoverable, effectively-once
results for ordered snapshot synchronization. It combines at-least-once batch
processing with idempotent target writes; it does not claim distributed
exactly-once delivery.

## Commit and recovery model

Each source page is ordered by the composite cursor `(updated_at, id)`. The
SQLite sink starts a write transaction with `BEGIN IMMEDIATE`, upserts the
page, advances that source's checkpoint, and then commits. The pipeline only
advances its in-memory cursor after the sink returns successfully.

This creates one durable boundary per batch:

- Failure before a batch commit leaves both its rows and checkpoint absent.
- Failure after a commit may cause that batch to be read again on restart.
- Replaying a batch is safe because rows are upserted by order ID and only a
  strictly newer `updated_at` replaces an existing target row.
- The checkpoint only moves forward, including when concurrent instances
  finish overlapping pages in a different order.

An interrupted run therefore resumes strictly after the last committed
composite cursor. Batch size affects the amount of replay work, not correctness.

## Timestamp boundaries and stale versions

The ID tie-breaker prevents records with the same timestamp from being skipped
when a page ends in the middle of that timestamp. A stale version delivered
after a newer target version cannot overwrite it, and a stale batch cannot
move the checkpoint backward.

This guarantee assumes the source's cursor fields are monotonic for newly
discoverable changes. The snapshot connector cannot discover a row inserted or
updated after a run if its `(updated_at, id)` sorts at or before the already
committed checkpoint. Sources that allow such late events need a lookback and
deduplication policy, a monotonic sequence column, or a CDC log.

## SQLite contention

Multiple DeltaFlow processes may target the same SQLite file. SQLite serializes
writers; connections use a 30-second busy timeout, so a transient write lock is
waited out instead of failing immediately. Overlapping synchronization runs can
replay work, but idempotent upserts and monotonic checkpoints make them converge
on the same target state.

If a write lock lasts longer than the busy timeout, SQLite raises an
`OperationalError`. The transaction is rolled back and the checkpoint remains
at the prior committed batch. Operators should retry the run with backoff and
investigate long-lived transactions. DeltaFlow does not provide leader
election, cross-database atomicity, or an internal retry scheduler.

## Fault coverage

The reliability suite exercises:

- interruption before the Nth batch and restart from the previous checkpoint;
- whole-batch rollback on a sink write failure;
- row rollback when checkpoint persistence fails;
- committed-batch replay and same-timestamp page boundaries;
- stale version rejection and checkpoint monotonicity;
- two synchronization instances contending for one target; and
- waiting for, then recovering from, a transient SQLite write lock.

Run the full suite with:

```console
PYTHONPATH=src python3 -m unittest discover -s tests -v
```
