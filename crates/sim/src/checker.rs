//! Safety and correctness checks run by the simulator.
//!
//! Continuously (after every replica event):
//! * **Election safety** – at most one leader per term.
//! * **State machine safety** – every replica commits the same entry at each
//!   index; a committed entry is never replaced or reordered. The first
//!   replica to commit an index defines the canonical entry and every later
//!   commit of that index on any replica (including after restarts) must
//!   match it byte for byte.
//! * Commit index never exceeds the log, never moves backwards within a
//!   replica's lifetime, and applied <= committed.
//! * **Client agreement** – a request is never acknowledged with two
//!   different replies.
//!
//! At the end of the run:
//! * The canonical committed log is replayed into a fresh state machine. Every
//!   acknowledged client reply must equal the reply produced by the first
//!   (and only) execution of that request in the log: no acknowledged write is
//!   lost, none is applied twice, and none observed a different state.
//! * Every replica's state digest equals the reference digest.
//! * Ledger invariants (debits == credits per ledger, balances equal the sum
//!   of their transfers, balance flags respected) hold on every replica.

use std::collections::BTreeMap;

use consensus::state_machine::Applied;
use consensus::{Entry, Replica, Reply, Request, Role, StateMachine, Wal};
use ledger::codec::fnv64;

use crate::disk::SimDevice;

type SimReplica = Replica<Wal<SimDevice>>;

pub struct Checker {
    committed: Vec<Entry>,
    hashes: Vec<u64>,
    checked: Vec<u64>,
    leaders: BTreeMap<u64, u8>,
    acks: BTreeMap<(u128, u64), Reply>,
    sent_at: BTreeMap<(u128, u64), u64>,
    acked_at: BTreeMap<(u128, u64), u64>,
}

fn entry_hash(e: &Entry) -> u64 {
    let mut buf = Vec::new();
    e.encode(&mut buf);
    fnv64(&buf)
}

impl Checker {
    pub fn new(replicas: usize) -> Self {
        Checker {
            committed: Vec::new(),
            hashes: Vec::new(),
            checked: vec![0; replicas],
            leaders: BTreeMap::new(),
            acks: BTreeMap::new(),
            sent_at: BTreeMap::new(),
            acked_at: BTreeMap::new(),
        }
    }

    pub fn committed_len(&self) -> u64 {
        self.committed.len() as u64
    }

    pub fn on_restart(&mut self, r: u8) {
        // Commit index is volatile; it restarts from 0 and is re-learned.
        self.checked[r as usize] = 0;
    }

    pub fn check_replica(&mut self, r: &SimReplica) -> Result<(), String> {
        let id = r.id();
        if r.role() == Role::Leader {
            match self.leaders.get(&r.term()) {
                Some(l) if *l != id => {
                    return Err(format!(
                        "election safety: replicas {l} and {id} both leader in term {}",
                        r.term()
                    ))
                }
                Some(_) => {}
                None => {
                    self.leaders.insert(r.term(), id);
                }
            }
        }
        let commit = r.commit_index();
        if commit > r.last_index() {
            return Err(format!(
                "replica {id} commit index {commit} beyond log end {}",
                r.last_index()
            ));
        }
        if r.last_applied() > commit {
            return Err(format!(
                "replica {id} applied {} beyond commit {commit}",
                r.last_applied()
            ));
        }
        let checked = self.checked[id as usize];
        if commit < checked {
            return Err(format!(
                "replica {id} commit index went backwards {checked} -> {commit}"
            ));
        }
        for i in checked + 1..=commit {
            let e = &r.log()[i as usize - 1];
            if e.index != i {
                return Err(format!(
                    "replica {id} log slot {i} holds entry with index {}",
                    e.index
                ));
            }
            let h = entry_hash(e);
            if (i as usize) <= self.committed.len() {
                let canonical = &self.committed[i as usize - 1];
                if self.hashes[i as usize - 1] != h {
                    return Err(format!(
                        "state machine safety: replica {id} committed a different entry at index {i}: \
                         term {} ts {} ({} requests) vs canonical term {} ts {} ({} requests)",
                        e.term,
                        e.timestamp,
                        e.request_count(),
                        canonical.term,
                        canonical.timestamp,
                        canonical.request_count()
                    ));
                }
            } else {
                self.committed.push(e.clone());
                self.hashes.push(h);
            }
        }
        self.checked[id as usize] = commit;
        Ok(())
    }

    /// Records when a request was first sent (for the real-time order check).
    pub fn on_first_send(&mut self, req: &Request, now: u64) {
        self.sent_at.entry((req.client_id, req.request_number)).or_insert(now);
    }

    pub fn on_ack(&mut self, req: &Request, reply: &Reply, now: u64) -> Result<(), String> {
        let key = (req.client_id, req.request_number);
        self.acked_at.entry(key).or_insert(now);
        if let Some(prev) = self.acks.get(&key) {
            if prev != reply {
                return Err(format!(
                    "request {key:?} acknowledged twice with different replies"
                ));
            }
        }
        self.acks.insert(key, reply.clone());
        Ok(())
    }

    /// Linearizability's real-time condition: if request A was acknowledged
    /// before request B was first sent, A must precede B in the log. (Every
    /// operation, including lookups, goes through the log, so log order is
    /// the linearization order.)
    fn check_real_time_order(&self, position: &BTreeMap<(u128, u64), (u64, usize)>) -> Result<(), String> {
        // Acknowledged requests sorted by ack time, with a running maximum of
        // their log positions.
        let mut acked: Vec<(u64, (u64, usize), (u128, u64))> = self
            .acked_at
            .iter()
            .filter_map(|(k, t)| position.get(k).map(|p| (*t, *p, *k)))
            .collect();
        acked.sort();
        let mut prefix_max: Vec<((u64, usize), (u128, u64))> = Vec::with_capacity(acked.len());
        for (_, pos, key) in &acked {
            let best = match prefix_max.last() {
                Some((p, k)) if *p > *pos => (*p, *k),
                _ => (*pos, *key),
            };
            prefix_max.push(best);
        }
        for (key, pos) in position {
            let Some(sent) = self.sent_at.get(key) else { continue };
            // Requests acknowledged strictly before `key` was first sent.
            let n = acked.partition_point(|(t, _, _)| t < sent);
            if n == 0 {
                continue;
            }
            let (max_pos, max_key) = prefix_max[n - 1];
            if max_pos > *pos {
                return Err(format!(
                    "real-time order violated: {max_key:?} was acknowledged before {key:?} was sent \
                     but is ordered after it in the log ({max_pos:?} > {pos:?})"
                ));
            }
        }
        Ok(())
    }

    /// Returns the digest of the reference state.
    pub fn final_check(&self, replicas: &[&SimReplica]) -> Result<u64, String> {
        let mut sm = StateMachine::new();
        let mut first: BTreeMap<(u128, u64), Reply> = BTreeMap::new();
        // Position of each request's (only) execution in the total order.
        let mut position: BTreeMap<(u128, u64), (u64, usize)> = BTreeMap::new();
        for e in &self.committed {
            for (slot, a) in sm.apply(e).into_iter().enumerate() {
                if let Applied::Executed(r) = a {
                    let key = (r.client_id, r.request_number);
                    position.insert(key, (e.index, slot));
                    if first.insert(key, r).is_some() {
                        return Err(format!(
                            "request {key:?} executed twice in the committed log"
                        ));
                    }
                }
            }
        }
        for (key, ack) in &self.acks {
            match first.get(key) {
                None => return Err(format!("acknowledged request {key:?} is not in the committed log (lost write)")),
                Some(r) if r != ack => {
                    return Err(format!(
                        "acknowledged reply for {key:?} differs from its committed execution:\n  acked:     {:?}\n  committed: {:?}",
                        ack.status, r.status
                    ))
                }
                Some(_) => {}
            }
        }
        self.check_real_time_order(&position)?;
        sm.ledger
            .check_invariants()
            .map_err(|e| format!("reference ledger invariant: {e}"))?;
        let digest = sm.digest();
        for r in replicas {
            if r.last_applied() != self.committed.len() as u64 {
                return Err(format!(
                    "replica {} applied {} of {} committed entries at end of run",
                    r.id(),
                    r.last_applied(),
                    self.committed.len()
                ));
            }
            r.state_machine()
                .ledger
                .check_invariants()
                .map_err(|e| format!("replica {} ledger invariant: {e}", r.id()))?;
            if r.state_machine().digest() != digest {
                return Err(format!(
                    "replica {} state diverges from reference replay",
                    r.id()
                ));
            }
        }
        Ok(digest)
    }
}
