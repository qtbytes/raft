#!/bin/sh

DEFAULT_RUNS=10

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  exit 1
fi

TEST_PATTERN="$1"
TEST_RUNS="${2:-$DEFAULT_RUNS}"

if ! [[ "$TEST_RUNS" =~ ^[0-9]+$ ]]; then
  exit 1
fi

for ((run=1; run<=TEST_RUNS; run++)); do
  echo "===== TESTING: $TEST_PATTERN $run/$TEST_RUNS ====="
  go test -run "$TEST_PATTERN" -failfast || exit 1
done
