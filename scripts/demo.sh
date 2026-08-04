#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python_bin="${PYTHON:-python3}"
cleanup_workspace=false

if [[ $# -gt 1 ]]; then
  echo "usage: $0 [workspace]" >&2
  exit 2
fi

if [[ $# -eq 1 ]]; then
  workspace="$1"
  if [[ -e "$workspace" ]] && [[ -n "$(ls -A "$workspace" 2>/dev/null)" ]]; then
    echo "demo: workspace must be absent or empty: $workspace" >&2
    exit 2
  fi
  mkdir -p "$workspace"
else
  workspace="$(mktemp -d "${TMPDIR:-/tmp}/deltaflow-demo.XXXXXX")"
  cleanup_workspace=true
fi

cleanup() {
  if [[ "$cleanup_workspace" == true ]]; then
    rm -rf "$workspace"
  fi
}
trap cleanup EXIT

source_db="$workspace/commerce.db"
target_db="$workspace/analytics.db"

qsync() {
  PYTHONPATH="$repo_root/src${PYTHONPATH:+:$PYTHONPATH}" "$python_bin" -m qsync "$@"
}

step() {
  printf '\n== %s ==\n' "$1"
}

echo "DeltaFlow one-command demo"
echo "workspace: $workspace"

step "1. Create 12 source orders and run the initial full sync"
qsync init-demo "$source_db" --count 12
qsync sync "$source_db" "$target_db" --batch-size 5

step "2. Append 3 orders, update 2 existing orders, and sync only those changes"
qsync init-demo "$source_db" --count 3 --append --updates 2
qsync sync "$source_db" "$target_db" --batch-size 5

step "3. Run again with no source changes (no-op)"
qsync sync "$source_db" "$target_db" --batch-size 5

step "4. Add 7 orders, stop after 2 batches, and inspect partial target status"
qsync init-demo "$source_db" --count 7 --append
qsync sync "$source_db" "$target_db" --batch-size 2 --max-batches 2
qsync status "$target_db"

step "5. Resume from the checkpoint and show final status"
qsync sync "$source_db" "$target_db" --batch-size 2
qsync status "$target_db"

if [[ "$cleanup_workspace" == false ]]; then
  echo
  echo "Demo databases kept in: $workspace"
fi
