# Bugs found while building

A log of real defects hit during development, how they surfaced and how they
were fixed. Entries are in the order they were found. Nothing here is
hypothetical; where a bug was in test scaffolding rather than in the system,
it says so.

Honest summary: the deterministic simulator did **not** find a safety bug
(lost commit, divergent replicas, double-applied transfer) in the Raft core
during development. It found one liveness/performance bug in the core and one
in its own client model. The most consequential bugs (election storms under
load) were found by running the real 3-process cluster under benchmark load,
which is exactly the class of problem a single-threaded simulator with an
idealised CPU cannot see. To show the checker is able to catch safety bugs,
the replica has four switchable, deliberately wrong behaviours; their
detection rates are listed at the end.

---

## 1. Simulated clients never reached the leader (simulator harness bug)

* **Found by:** first VOPR run, `sim --seeds 300`: 203 of 300 seeds failed
  the liveness check, e.g. seed 1:
  `liveness: cluster did not converge within 60s of healing; 4 clients unfinished`
  while every replica reported the same commit index.
* **Symptom:** the cluster was healthy and idle, but clients never finished.
* **Root cause:** in the simulated client, a `NotLeader { hint }` reply set
  the target to the hinted leader and then scheduled the *timeout* event to
  resend. The timeout handler rotates to the next replica before resending, so
  every redirect landed one replica past the leader. With 3 replicas this
  cycles forever.
* **Fix:** a separate `ClientRetry` event that resends without rotating
  (`crates/sim/src/lib.rs`).
* **Lesson:** the liveness check catches harness bugs too; the same pattern
  (redirect then rotate) was avoided in the Rust and Java clients.

## 2. Duplicated rejections caused an unbounded AppendEntries storm (core)

* **Found by:** VOPR with the injected `ack-before-sync` bug. Seeds 24, 35,
  49, 57, 59, 60 and 92 did not terminate within 10 s of wall time (healthy
  seeds take milliseconds). The verbose trace of seed 24 showed thousands of
  identical `AppendEntriesResponse { success: false, conflict_index: 187 }`
  messages in a few simulated milliseconds.
* **Root cause:** on every rejection the leader immediately sent a new probe.
  The network duplicates messages, so one rejection could produce two probes,
  each producing a rejection, each duplicated again: the number of in-flight
  messages grew geometrically. (The injected bug made the follower reject
  forever, which exposed it, but the amplification exists whenever a follower
  rejects repeatedly, e.g. while being repaired after a long partition.)
* **Fix:** a leader in probe state only sends a new probe when a rejection
  changes `next_index`; duplicate/stale rejections are ignored and a lost probe
  is retransmitted by the heartbeat (`on_append_response` in
  `crates/consensus/src/replica.rs`). The simulator also got an event budget
  (5 million events) so a message storm is reported as a failure with its seed
  instead of hanging the runner.
* **After the fix:** the same seeds fail fast with the expected safety or
  liveness violation caused by the injected bug.

## 3. Election storms under benchmark load (core + server)

* **Found by:** `bench cluster --clients 32 --batch 128` against the real
  3-process cluster. One run died with `request timed out on all replicas`
  after 30 s; another completed but the replica logs (added for this
  investigation) showed term numbers climbing past 390 in 15 seconds.
* **Root causes (three, compounding):**
  1. **Catch-up tick bursts.** The server event loop called `tick()` once for
     every 10 ms that had elapsed. After one slow iteration the replica fired
     a burst of ticks *before* reading the AppendEntries already waiting in its
     queue, and started an election against a healthy leader.
  2. **No protection against disruptive candidates.** Any replica that timed
     out bumped the term, and every other replica (including the leader)
     adopted the higher term and stepped down, so one slow follower could
     depose a healthy leader.
  3. **Election timeout too aggressive for an inline-fsync event loop.**
     150–300 ms (a common LAN default) is shorter than the stalls described in
     bug 4 on a shared VPS.
* **Fixes:**
  1. At most one logical tick per loop iteration; a stalled process stalls its
     own timers (`crates/server/src/node.rs`).
  2. Raft §6 "disruptive servers" rule plus check-quorum: replicas that have
     heard from a live leader within the minimum election timeout ignore
     RequestVote entirely, a leader ignores them while it has quorum contact,
     and a leader that has not heard from a majority for an election timeout
     steps down (`believes_leader_alive`, `quorum_active` in `replica.rs`).
     Both changes are safety-neutral (ignoring a vote request or stepping down
     is always safe) and the simulator re-ran clean afterwards.
  3. Server defaults: heartbeat 50 ms, election timeout 0.5–1 s, at most 4
     entries per AppendEntries (bounded message size).

## 4. Multi-second HashMap resize pauses in the state machine (core)

* **Found by:** the slow-iteration log added while chasing bug 3:
  `replica 2: slow loop iteration 2.368026033s (157 inputs in 2.367981178s, fsync 293ns)`
  — almost all of the time was spent applying a handful of entries, not in
  fsync.
* **Root cause:** transfers lived in one `HashMap`. At ~3 million transfers a
  resize copies the entire table in one step; each stored transfer also kept a
  full second copy of the original request for idempotency checks (~200 bytes
  per transfer in total), so a resize moved hundreds of megabytes while the
  event loop was blocked. The same effect showed up as a 210 ms p99 in the
  state-machine-only benchmark with 8,190-transfer batches.
* **Fix:** `ShardedMap` (64 independently growing shards, so a resize moves
  1/64 of the data) and a compact representation that reconstructs the
  original request from the stored transfer plus a 5-bit "which optional
  fields were zero" mask (`crates/ledger/src/sharded.rs`, `StoredTransfer` in
  `ledger.rs`). A `debug_assert` checks the reconstruction on every insert and
  the property tests exercise it.
* **After the fix:** the same benchmark ran with no elections; the worst
  logged iteration was ~265 ms (on a host with load average above 8) instead
  of 2.4 s. Remaining pauses are listed as a limitation in the README.

## 5. Spring integration test could not start the cluster (test scaffolding)

* **Found by:** `./mvnw verify`.
* **Symptoms and causes:**
  * `Unable to find a @SpringBootConfiguration`: failsafe ran against the
    repackaged Spring Boot jar; fixed by pointing failsafe at
    `target/classes`.
  * Every request returned 503: `@BeforeAll` started the Rust processes before
    `@DynamicPropertySource` had chosen the ports, so the servers got an empty
    `--cluster` argument and exited with a usage error. Fixed by choosing the
    ports in a static initializer.

---

## Injected bugs: does the checker have teeth?

`sim --inject-bug NAME` switches on a deliberately wrong behaviour in the
replica. Results with the final code are in the README ("Simulator results").
In short: `vote-ignores-log` and `ack-before-sync` are caught in a large
fraction of seeds; `commit-old-term` (Raft Figure 8) and
`unbounded-follower-commit` are caught only rarely, because the leader's
no-op entry and the pipelined, in-order replication make the required
interleavings very unlikely under random fault injection. That is a real
limitation of random simulation, recorded here rather than tuned away.
