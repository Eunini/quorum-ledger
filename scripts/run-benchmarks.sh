#!/usr/bin/env bash
# Runs every benchmark used in the README and prints one key=value line each.
# usage: scripts/run-benchmarks.sh [seconds-per-cluster-run]
set -euo pipefail
cd "$(dirname "$0")/.."
SECS=${1:-15}
cargo build --release -p server -p bench >/dev/null
echo "# $(date -u +%FT%TZ) $(nproc) vCPU, $(uname -r), load: $(cut -d' ' -f1-3 /proc/loadavg)"
./target/release/bench fsync
./target/release/bench sm
for cfg in "1 1" "32 1" "8 128" "32 128" "16 1000"; do
  set -- $cfg
  echo "# load: $(cut -d' ' -f1-3 /proc/loadavg)"
  ./target/release/bench cluster --clients "$1" --batch "$2" --seconds "$SECS"
done
echo "# load: $(cut -d' ' -f1-3 /proc/loadavg)"
if command -v pgbench >/dev/null || ls /usr/lib/postgresql/*/bin/pgbench >/dev/null 2>&1; then
  scripts/postgres-baseline.sh "$SECS"
else
  echo "# postgres/pgbench not found; skipping SQL baseline"
fi
