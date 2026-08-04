# DeltaFlow performance

`scripts/benchmark.py` measures the public SQLite source, sink, and incremental
pipeline together. Its default matrix covers 100,000 and 1,000,000 initial rows
at batch sizes 100, 1,000, 5,000, and 10,000. Every case then appends 10,000
rows, runs an incremental sync, and measures a no-op sync.

The generated source schema includes `idx_orders_updated_at_id` on
`(updated_at, id)`. This is fixture setup only: the source connector never
creates indexes or otherwise changes a source database.

Run the complete matrix from the repository root:

```bash
PYTHONPATH=src python3 scripts/benchmark.py > benchmark.json
```

For a quick smoke run, override both axes:

```bash
PYTHONPATH=src python3 scripts/benchmark.py \
  --rows 1000 --batch-sizes 100 500 --incremental-rows 100
```

The program writes one JSON document to stdout and diagnostics to stderr. The
top-level `schema_version`, benchmark name, ordered cases, and per-case fields
form the stable machine-readable output contract. Temporary filesystem paths
and source IDs are deliberately excluded. Timing and throughput values are
measurements, not stable assertions.

## Results on the current machine

Measured on 2026-08-04 on arm64 macOS 26.5.1 (build 25F80), using CPython
3.11.12 and SQLite 3.51.0. These values are a local reference only: CPU,
storage, filesystem, Python and SQLite builds, power state, background load,
and virtualization can materially change them.

<!-- BENCHMARK_RESULTS_START -->
| Initial rows | Batch size | Initial seconds | Initial rows/s | Incremental seconds | Incremental rows/s | No-op ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | 100 | 5.852694 | 17,086.15 | 0.630751 | 15,854.13 | 5.510 |
| 100,000 | 1,000 | 0.751405 | 133,084.06 | 0.076587 | 130,570.82 | 5.217 |
| 100,000 | 5,000 | 0.282999 | 353,358.19 | 0.032004 | 312,463.38 | 5.639 |
| 100,000 | 10,000 | 0.223465 | 447,497.45 | 0.026825 | 372,786.58 | 4.804 |
| 1,000,000 | 100 | 559.768430 | 1,786.45 | 4.790613 | 2,087.42 | 43.562 |
| 1,000,000 | 1,000 | 58.116941 | 17,206.69 | 0.560258 | 17,848.93 | 48.949 |
| 1,000,000 | 5,000 | 13.227692 | 75,598.98 | 0.158860 | 62,948.48 | 46.432 |
| 1,000,000 | 10,000 | 7.978534 | 125,336.31 | 0.125402 | 79,743.47 | 51.400 |
<!-- BENCHMARK_RESULTS_END -->

Fixture generation time is reported in JSON but excluded from sync throughput.
No-op throughput is not meaningful because it reads zero rows; its latency is
the useful value. Compare results from the same machine and software stack when
evaluating a change. On this run, larger batches were substantially faster,
especially at one million rows, because they required fewer source pages and
transaction commits. That relationship is an observation from this environment,
not a cross-platform guarantee.
