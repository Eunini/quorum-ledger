#!/usr/bin/env bash
# Baseline: the same random-transfer workload against a typical SQL ledger in
# PostgreSQL (row locks + balance updates + transfer insert per transaction),
# driven by pgbench. Uses a throwaway local cluster under .bench/pgdata with
# fsync and synchronous_commit ON (same durability promise as the WAL).
#
# usage: scripts/postgres-baseline.sh [seconds]   (needs postgres + pgbench binaries)
set -euo pipefail
SECONDS_PER_RUN=${1:-15}
PGBIN=${PGBIN:-$(ls -d /usr/lib/postgresql/*/bin 2>/dev/null | sort -V | tail -1)}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DATA=$ROOT/.bench/pgdata
SOCK=$ROOT/.bench/pgsock
PORT=55432
ACCOUNTS=10000

rm -rf "$DATA" "$SOCK"
mkdir -p "$SOCK"
"$PGBIN/initdb" -D "$DATA" -U bench --auth=trust >/dev/null
"$PGBIN/pg_ctl" -D "$DATA" -l "$ROOT/.bench/pg.log" -w start \
  -o "-p $PORT -k $SOCK -c listen_addresses='' -c fsync=on -c synchronous_commit=on -c max_connections=100 -c shared_buffers=512MB" >/dev/null
trap '"$PGBIN/pg_ctl" -D "$DATA" -m fast stop >/dev/null; rm -rf "$DATA" "$SOCK"' EXIT
PSQL=("$PGBIN/psql" -h "$SOCK" -p $PORT -U bench -d postgres -q -v ON_ERROR_STOP=1)

"${PSQL[@]}" <<SQL
CREATE TABLE accounts (
  id bigint PRIMARY KEY,
  debits_posted numeric(39,0) NOT NULL DEFAULT 0,
  credits_posted numeric(39,0) NOT NULL DEFAULT 0
);
CREATE TABLE transfers (
  id bigint PRIMARY KEY,
  debit_account_id bigint NOT NULL REFERENCES accounts(id),
  credit_account_id bigint NOT NULL REFERENCES accounts(id),
  amount numeric(39,0) NOT NULL CHECK (amount > 0),
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE SEQUENCE transfer_ids;
INSERT INTO accounts (id) SELECT g FROM generate_series(1, $ACCOUNTS) g;
-- One transfer: lock both rows in id order (no deadlocks), update, insert.
CREATE FUNCTION post_transfer(dr bigint, cr bigint, amt bigint) RETURNS void LANGUAGE plpgsql AS \$\$
BEGIN
  PERFORM 1 FROM accounts WHERE id IN (dr, cr) ORDER BY id FOR UPDATE;
  UPDATE accounts SET debits_posted = debits_posted + amt WHERE id = dr;
  UPDATE accounts SET credits_posted = credits_posted + amt WHERE id = cr;
  INSERT INTO transfers (id, debit_account_id, credit_account_id, amount)
  VALUES (nextval('transfer_ids'), dr, cr, amt);
END \$\$;
-- n transfers in one transaction (the batched variant). All touched rows are
-- locked up front in id order, as a batching SQL ledger must to avoid deadlocks.
CREATE FUNCTION post_batch(n int) RETURNS void LANGUAGE plpgsql AS \$\$
DECLARE drs bigint[]; crs bigint[]; amts bigint[];
BEGIN
  SELECT array_agg(d), array_agg(CASE WHEN c = d THEN d % $ACCOUNTS + 1 ELSE c END), array_agg(a)
    INTO drs, crs, amts
    FROM (SELECT 1 + floor(random() * $ACCOUNTS)::bigint AS d,
                 1 + floor(random() * $ACCOUNTS)::bigint AS c,
                 1 + floor(random() * 1000)::bigint AS a
          FROM generate_series(1, n)) x;
  PERFORM 1 FROM accounts WHERE id = ANY (drs || crs) ORDER BY id FOR UPDATE;
  FOR i IN 1..n LOOP
    UPDATE accounts SET debits_posted = debits_posted + amts[i] WHERE id = drs[i];
    UPDATE accounts SET credits_posted = credits_posted + amts[i] WHERE id = crs[i];
    INSERT INTO transfers (id, debit_account_id, credit_account_id, amount)
    VALUES (nextval('transfer_ids'), drs[i], crs[i], amts[i]);
  END LOOP;
END \$\$;
SQL

cat > "$ROOT/.bench/single.sql" <<SQL
\set dr random(1, $ACCOUNTS)
\set cr random(1, $ACCOUNTS)
\set amt random(1, 1000)
BEGIN;
SELECT 1 FROM accounts WHERE id IN (:dr, :cr) ORDER BY id FOR UPDATE;
UPDATE accounts SET debits_posted = debits_posted + :amt WHERE id = :dr;
UPDATE accounts SET credits_posted = credits_posted + :amt WHERE id = :cr;
INSERT INTO transfers (id, debit_account_id, credit_account_id, amount) VALUES (nextval('transfer_ids'), :dr, :cr, :amt);
COMMIT;
SQL

run() { # name clients script transfers_per_txn
  local name=$1 clients=$2 script=$3 per=$4
  local logdir=$ROOT/.bench/pglog-$name
  rm -rf "$logdir"; mkdir -p "$logdir"
  (cd "$logdir" && "$PGBIN/pgbench" -h "$SOCK" -p $PORT -U bench -n -c "$clients" -j "$(( clients < 4 ? clients : 4 ))" \
      -T "$SECONDS_PER_RUN" -f "$script" -l postgres >/dev/null 2>"$logdir/err.txt") || { cat "$logdir/err.txt"; exit 1; }
  # Per-transaction latency (us) is column 3 of pgbench's log.
  cat "$logdir"/pgbench_log.* | awk '{print $3}' | sort -n > "$logdir/lat.txt"
  local n p50 p99
  n=$(wc -l < "$logdir/lat.txt")
  p50=$(awk -v n="$n" 'NR==int(n*0.50+0.5){print; exit}' "$logdir/lat.txt")
  p99=$(awk -v n="$n" 'NR==int(n*0.99+0.5){print; exit}' "$logdir/lat.txt")
  echo "bench=postgres variant=$name clients=$clients transfers_per_txn=$per seconds=$SECONDS_PER_RUN txns=$n transfers_per_sec=$(( n * per / SECONDS_PER_RUN )) txn_p50_us=$p50 txn_p99_us=$p99"
  rm -rf "$logdir"
}

echo "SELECT post_batch(128);" > "$ROOT/.bench/batch.sql"
run single-c1 1 "$ROOT/.bench/single.sql" 1
run single-c32 32 "$ROOT/.bench/single.sql" 1
run batch128-c8 8 "$ROOT/.bench/batch.sql" 128
run batch128-c32 32 "$ROOT/.bench/batch.sql" 128
"${PSQL[@]}" -t -c "SELECT 'consistency check: sum(debits)=sum(credits): ' || (sum(debits_posted) = sum(credits_posted)) FROM accounts;"
rm -f "$ROOT/.bench/single.sql" "$ROOT/.bench/batch.sql"
