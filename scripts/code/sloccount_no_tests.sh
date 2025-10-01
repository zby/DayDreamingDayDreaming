#!/usr/bin/env bash
# Run sloccount while excluding colocated Python tests (files named test_*.py).
# Usage: scripts/code/sloccount_no_tests.sh [path ...]
# If no paths are provided, defaults to the project src/ directory.

set -euo pipefail

if ! command -v sloccount >/dev/null 2>&1; then
  echo "Error: sloccount is not installed or not on PATH." >&2
  exit 1
fi

if [ $# -eq 0 ]; then
  roots=("src")
else
  roots=("$@")
fi

# Create a temporary workspace containing only non-test Python files.
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

# Package filtered files and extract them under the temporary directory.
find "${roots[@]}" -type f -name '*.py' ! -name 'test_*.py' -print0 \
  | tar --null -T - -cf - \
  | (cd "$tmpdir" && tar -xf -)

# Run sloccount against the staged copy.
sloccount "$tmpdir"
