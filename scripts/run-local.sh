#!/usr/bin/env bash
# Pipe local etl-output into target-rillet using sandbox creds + flow snapshots.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ ! -f .secrets/config.json ]]; then
  echo "Missing .secrets/config.json" >&2
  exit 1
fi

if [[ ! -d .venv ]]; then
  python3 -m venv .venv
  .venv/bin/pip install -e .
fi

INPUT="${1:-.secrets/data.singer}"
if [[ ! -f "$INPUT" ]]; then
  echo "Missing singer input: $INPUT" >&2
  exit 1
fi

export FLOW="${FLOW:-KQhsz4w0j}"
export SNAPSHOT_DIR="${SNAPSHOT_DIR:-$ROOT/snapshots}"

# cwd under .secrets so SDK signal listener can write ../.secrets/...
cd "$ROOT/.secrets"
if [[ "$INPUT" = /* ]]; then
  INPUT_PATH="$INPUT"
elif [[ "$INPUT" == .secrets/* ]]; then
  INPUT_PATH="$ROOT/$INPUT"
else
  INPUT_PATH="$ROOT/$INPUT"
fi

"$ROOT/.venv/bin/target-rillet" --config config.json --input "$INPUT_PATH"
