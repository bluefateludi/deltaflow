# DeltaFlow one-command demo

From the repository root, run:

```bash
./scripts/demo.sh
```

The script creates a temporary SQLite source and target, then prints each CLI
command's JSON result while it demonstrates:

1. an initial full synchronization;
2. an incremental run containing both appended and updated orders;
3. a no-op run with zero rows read and written;
4. a run stopped by `--max-batches`, followed by `status` showing the partial
   target; and
5. a resumed run from the cursor printed by the interrupted sync, followed by
   final `status` showing all source rows in the target.

The temporary databases are deleted when the script exits. Pass an absent or
empty workspace directory to keep them for inspection:

```bash
./scripts/demo.sh /tmp/deltaflow-demo
```

Set `PYTHON` to select a Python 3.10+ interpreter. The script uses the checkout's
`src` directory directly, so an editable install is not required:

```bash
PYTHON=.venv/bin/python ./scripts/demo.sh
```

The output is intentionally CLI-shaped rather than parsed by the shell. This
makes cursor movement, row counts, batch counts, and no-op behavior visible in
the same JSON users receive from `qsync`.
