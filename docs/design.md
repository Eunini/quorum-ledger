# Design notes and trade-offs

Written so that every decision can be defended in a design review. Each
section states the decision, why, and what it costs.

## 1. Shape of the system

The ledger is a deterministic state machine replicated with Raft. Everything
that decides anything (Raft, the session table, the ledger) is a single
**sans-IO** Rust value, `Replica<S: Storage>`, driven by explicit events
(`step`, `submit`, `tick`, `prepare`, `sync`). It never reads a clock, spawns a
thread or touches a socket.

Two drivers exist:

* the **TCP server** (`crates/server`): threads for sockets, one event-loop
  thread that owns the replica, a file-backed WAL;
* the **simulator** (`crates/sim`): one thread, a virtual clock, a simulated
  network and a simulated disk.

Because both drive the same code through the same interface, a bug the
simulator finds is a bug in the production code, and a fix is tested by
replaying the seed. This is the main architectural decision; the rest follows
from it.

## 2. Why Raft (and not Viewstamped Replication)

Both are leader-based state machine replication protocols with equivalent
guarantees. Raft was chosen because:

* **Durable term and vote are part of the protocol.** VSR Revisited assumes
  replicas may lose their disk state and adds a separate recovery protocol;
  this project wanted the crash model "process dies, unsynced writes are lost,
  synced writes survive" and Raft's persistent `currentTerm`/`votedFor`/log map
  directly onto a WAL.
* **Log repair is local and incremental** (AppendEntries consistency check with
  fast backtracking), whereas a VSR view change ships log suffixes in
  DoViewChange messages.
* It is the protocol most reviewers know, so the code can be checked against
  the paper.

Implemented: leader election with randomized timeouts, log replication with
the prev-index/prev-term check, commit only of current-term entries counted
by replicas (indirect commit of older ones), a no-op entry per new leader, fast
backtracking (`conflict_term`/`conflict_index`), pipelined replication with a
probe state for lagging followers, check-quorum, and the §6 rule that replicas
ignore vote requests while they believe a leader is alive.

Not implemented: membership changes, snapshots/log compaction, pre-vote,
leader leases for local reads. See the README limitations.

## 3. Durability: when may a replica say "yes"?

Rule: **nothing that asserts local durable state leaves the process before the
WAL is fsynced.** Votes, AppendEntries acknowledgements and anything sent while
term/vote are unsynced are *held* and released by `sync()`.

The one exception is the leader's own AppendEntries: they are sent before the
leader's fsync so followers write in parallel with the leader. This is safe
because the leader counts itself toward a quorum only up to its *durable*
index (`durable_index`), not its in-memory log.

Two subtleties that are easy to get wrong:

* Held acknowledgements are dropped if the log is truncated or the term
  changes before the fsync completes; otherwise an "I have entry 5" could be
  sent for an entry 5 that was replaced.
* `fsync` errors are fatal (`expect`): after a failed fsync the page cache
  state is unknown (the "fsyncgate" problem), so fail-stop is the only safe
  choice.

`sim --inject-bug ack-before-sync` removes the holding and the simulator
reports lost commits and divergent replicas within a few thousand seeds.

## 4. The write-ahead log

An append-only file of records `len | crc32 | kind | body`. Kinds: hard state,
entry, truncate. Truncation is logged as a record instead of rewriting the
file, so every write is a sequential append and a crash at any point leaves a
valid prefix plus possibly a torn tail.

Recovery replays records until the first one that is incomplete, fails its
checksum or does not decode, and cuts the file there. Cutting is only correct
because of the durability rule above: everything after the last fsync was
never acknowledged to anyone.

The server buffers records in memory and issues one `write` + `fdatasync` per
event-loop iteration: **group commit**. Under load one fsync covers hundreds of
client requests.

What the checksum does *not* give: recovery from corruption of data that was
already fsynced (bit rot, misdirected writes). The replica would silently cut
its log at the damaged record and could lose an entry it had acknowledged.
Handling that safely needs protocol-aware recovery (asking peers for the
damaged entries before voting again, as in the PAR paper / TigerBeetle), which
is out of scope; the simulator therefore only corrupts unsynced data.

## 5. Batching

Two levels:

1. **Client batching:** one request carries up to 8,190 transfers (the
   TigerBeetle-style API). Per-request costs (network round trip, session
   lookup, reply) are paid once per batch.
2. **Log batching:** the leader queues incoming requests and `prepare()` packs
   everything that arrived during one event-loop iteration into log entries
   (capped by request count and event count). One AppendEntries and one fsync
   then cover many client requests.

The benchmarks show why: a single client sending single transfers is bounded
by one fsync round trip (~1 ms here), while 8 clients sending 128-transfer
batches move ~100k transfers/s on the same disk.

The cost is latency under load: requests wait for the current iteration's
fsync. The `max_entries_per_message` cap keeps AppendEntries around 3 MB so a
lagging follower cannot be sent an arbitrarily large message.

## 6. Why `u128`

* **Amounts:** `u64` minor units overflow for some real use cases (high
  precision crypto assets, 18-decimal tokens, aggregate accounts summing many
  balances). `u128` makes overflow practically unreachable, and every
  addition is still `checked_add`: overflow is a result code
  (`Overflow`), never a wrap.
* **Ids:** 128 bits lets clients generate ids (random or derived, e.g. from an
  idempotency key) with negligible collision probability, without asking the
  ledger for a sequence number first. That is what makes idempotent retries
  possible across process restarts.
* Cost: 16 bytes per field, and Java needs `BigInteger` at the API boundary.

## 7. Idempotency (three layers)

1. **Object ids.** A transfer is created at most once per id. Same id + same
   body returns `Exists` (treated as success); same id + different body returns
   `ExistsWithDifferentFields`. The comparison is against the request exactly as
   submitted (reconstructed from the stored transfer plus a zero-field mask, see
   bug 4 in `bugs-found.md`), so a post with `amount = 0` (meaning "full") is a
   different request than a post with the explicit amount.
2. **Client sessions.** `(client_id, request_number)` makes each *request*
   at-most-once even if the client resends it to a different replica after a
   leader change. Because the same request may be appended to the log twice
   (by two leaders), deduplication happens **at apply time** in the replicated
   session table, where every replica makes the same decision. The leader also
   skips requests it already has in flight, but that is only an optimisation.
3. **HTTP Idempotency-Key** (payments API). The transfer id is
   `SHA-256(endpoint scope || 0 || key)[..16]`. No idempotency table is needed:
   the ledger's id semantics do the work, across API instances and restarts.
   The scope keeps a hold and its capture from colliding if a client reuses a
   key.

Known gap: a transfer that *fails* (e.g. insufficient funds) is not recorded,
so retrying the same id later can succeed once funds arrive. TigerBeetle added
an explicit "id already failed" result for this; here it is documented
behaviour.

Session table entries are never evicted (unbounded memory in the number of
distinct client ids); a production system would expire sessions
deterministically as part of the log.

## 8. Two-phase transfers and timeouts

* `PENDING` reserves the amount: `debits_pending` / `credits_pending` grow and
  balance limits are checked against `pending + posted`.
* `POST_PENDING` captures all or part of the hold (amount 0 = full); the
  remainder is released. `VOID_PENDING` releases everything.
* A hold may carry `timeout` seconds.

**Where does time come from?** Never from the replica's clock while applying.
The leader stamps each log entry with
`timestamp = max(last_entry.timestamp + 1, wall_clock)`, so log timestamps are
strictly increasing even if the leader's clock jumps backwards or a new leader's
clock is behind. Before applying an entry, every replica expires all holds with
`expires_at <= entry.timestamp` (a `BTreeSet` ordered by expiry). All replicas
therefore expire exactly the same holds at exactly the same log position.

Consequences worth knowing:

* Expiry happens when the next entry is applied, not at the wall-clock moment.
  An idle cluster would not release a hold until the next write (a production
  system would have the leader append a no-op periodically; reads go through
  the log here, so they never observe an expired-but-unreleased hold).
* A leader with a clock far in the future pushes timestamps forward for
  everyone; the monotonic rule cannot pull them back. Clock skew in the
  simulator includes jumps of up to ±10 s to exercise this.

## 9. Reads

Lookups go through the log like writes, so every read is linearizable at the
cost of a replication round trip. Leader leases or ReadIndex would make reads
cheaper; neither is implemented.

## 10. The simulator

* **Determinism:** one thread, one seeded PRNG (own xoshiro256**, so seeds stay
  stable across dependency upgrades), an event queue ordered by
  `(virtual time, sequence number)`, and no iteration over randomly-seeded hash
  maps in any decision path. A test runs seeds twice and compares a hash of
  the whole event trace.
* **Swarm testing:** each seed draws its own cluster size (1, 3 or 5),
  drop/duplicate rates, latency range, partition and crash frequency, torn-write
  and corruption probabilities, fsync latency, clock offsets/jumps, batch sizes
  and workload. A fixed "average" fault mix tends to miss bugs that need one
  fault to be extreme and the others absent.
* **Crash model:** a crash discards all in-memory state; for each write issued
  since the last fsync the simulated disk keeps a random prefix, may tear the
  next write and may flip a bit in the surviving unsynced bytes. Crashes are
  also injected *immediately after* a replica issues writes, which is when they
  are most interesting.
* **Checks:** see the README. The strongest one replays the canonical committed
  log into a fresh state machine and requires every reply a client ever
  received to equal the reply of that request's single execution in the log,
  and checks real-time order (a request acknowledged before another was sent
  must be ordered before it).
* **Two phases:** fault phase, then heal (faults stop, all replicas restart,
  network becomes reliable). Liveness is only demanded after healing; during
  faults the cluster is allowed to be unavailable.

### What the simulator cannot catch

* Anything outside the replica: the TCP server's threading, framing and
  reconnect logic, the Java client, real fsync semantics of the OS and disk.
  These are covered only by ordinary tests and the cluster integration tests.
* Performance pathologies that depend on real CPU time: the simulator charges
  zero time for computation, so it could not see the HashMap resize pauses or
  the election storms in `bugs-found.md` (found with the real cluster).
* Faults outside the model: corruption of already-fsynced data, misdirected
  writes, Byzantine behaviour, clock-driven timeouts beyond the tick model.
* Rare interleavings. Random search found the injected Figure-8 bug
  (`commit-old-term`) in only a handful of 10,000 seeds. A clean run of N
  seeds is evidence, not proof.
* Bugs in the checker itself, or invariants nobody wrote down.

## 11. Server threading

Blocking `std::net` with a thread per connection and one event-loop thread.
Chosen over async (tokio) to keep the replica single-threaded and the code
small; at 3 replicas and tens of client connections thread count is not a
problem. The trade-off is that fsync runs inline on the event loop, so a slow
fsync delays heartbeats; this is why the server's election timeout is
0.5-1 s and why the loop processes at most one logical tick per iteration.
Moving fsync to its own thread (and acknowledging asynchronously, as the
simulator already models) is the next step.

## 12. Java side

* `LedgerClient` is one session (one request in flight), retries the same
  request number across replicas, follows `NotLeader` hints, ignores stale
  replies and gives up after a request timeout with `LedgerUnavailableException`.
  `LedgerClientPool` provides concurrency with N sessions.
* The payments API maps result codes to HTTP: 201 created, 200 replay,
  409 key reused with a different body or hold already captured/voided,
  422 insufficient funds / validation, 404 unknown account or hold,
  503 cluster unavailable (safe to retry with the same key).
* IDs are decimal strings in JSON (128-bit values do not fit JavaScript
  numbers); amounts are JSON integers in minor units.
