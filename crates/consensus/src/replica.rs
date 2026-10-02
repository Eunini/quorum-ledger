//! A Raft replica as a deterministic, sans-IO state machine.
//!
//! The replica never touches sockets, threads or clocks. A driver (the TCP
//! server or the simulator) feeds it events:
//!
//! * [`Replica::step`]    – a message from another replica
//! * [`Replica::submit`]  – a client request
//! * [`Replica::tick`]    – a logical timer tick (election / heartbeat timers)
//! * [`Replica::prepare`] – cut pending client requests into a log entry
//! * [`Replica::sync`]    – make all buffered storage writes durable
//!
//! and collects [`Outgoing`] messages with [`Replica::drain_outgoing`].
//!
//! Durability rule: any message whose correctness depends on local state
//! being on disk (votes, append acknowledgements, anything sent while term or
//! vote are unsynced) is *held* until the next `sync`. Only the leader's
//! `AppendEntries` may be sent before its own log write is durable; the leader
//! then counts itself toward a quorum only up to its durable index.

use std::collections::VecDeque;

use rustc_hash::FxHashMap;

use crate::message::{Entry, Message, Payload, Reply, ReplyStatus, Request};
use crate::prng::Prng;
use crate::state_machine::{Applied, StateMachine};
use crate::storage::{HardState, Storage};

/// Deliberately wrong behaviours that the simulator must be able to detect.
/// All are `false` in normal operation; they exist to prove the checker has
/// teeth (see `sim --inject-bug`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InjectedBugs {
    /// Count replicas for entries from earlier terms (Raft paper, Figure 8).
    pub commit_old_term_entries: bool,
    /// Send votes and acknowledgements before the WAL is fsynced.
    pub ack_before_sync: bool,
    /// Follower sets commit index to `leader_commit` without bounding it by
    /// the last entry verified by the current AppendEntries.
    pub unbounded_follower_commit: bool,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub replica_count: u8,
    pub election_timeout_min_ticks: u32,
    pub election_timeout_max_ticks: u32,
    pub heartbeat_ticks: u32,
    /// Maximum client requests packed into one log entry.
    pub max_requests_per_entry: usize,
    /// Maximum entries sent in one AppendEntries message.
    pub max_entries_per_message: usize,
    pub bugs: InjectedBugs,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            replica_count: 3,
            election_timeout_min_ticks: 15,
            election_timeout_max_ticks: 30,
            heartbeat_ticks: 3,
            max_requests_per_entry: 64,
            max_entries_per_message: 64,
            bugs: InjectedBugs::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outgoing {
    Peer { to: u8, msg: Message },
    Reply(Reply),
}

pub struct Replica<S: Storage> {
    id: u8,
    cfg: Config,
    storage: S,
    rng: Prng,
    clock_ns: u64,

    // Persistent state (mirrored to storage before it is relied upon).
    term: u64,
    voted_for: Option<u8>,
    log: Vec<Entry>,

    // Volatile state.
    role: Role,
    leader_id: Option<u8>,
    commit_index: u64,
    last_applied: u64,
    sm: StateMachine,
    election_elapsed: u32,
    election_timeout: u32,
    heartbeat_elapsed: u32,
    votes: u64,

    // Leader state.
    next_index: Vec<u64>,
    match_index: Vec<u64>,
    probing: Vec<bool>,
    pending: VecDeque<Request>,
    /// Highest request number per client that is queued or in the log but not
    /// yet applied, so retries are not appended again by this leader.
    inflight: FxHashMap<u128, u64>,

    // Durability tracking.
    durable_index: u64,
    hs_dirty: bool,
    log_dirty: bool,

    outbox: Vec<Outgoing>,
    held: Vec<Outgoing>,
}

impl<S: Storage> Replica<S> {
    /// Opens a replica, recovering term, vote and log from `storage`. The state
    /// machine is rebuilt by re-applying the log as the commit index becomes
    /// known again (commit index is volatile in Raft).
    pub fn open(id: u8, cfg: Config, mut storage: S, seed: u64) -> std::io::Result<Self> {
        assert!(
            cfg.replica_count >= 1 && cfg.replica_count <= 16,
            "1..=16 replicas supported"
        );
        assert!(id < cfg.replica_count, "replica id out of range");
        assert!(cfg.election_timeout_min_ticks > cfg.heartbeat_ticks);
        assert!(cfg.election_timeout_min_ticks <= cfg.election_timeout_max_ticks);
        assert!(cfg.max_requests_per_entry > 0 && cfg.max_entries_per_message > 0);
        let rec = storage.recover()?;
        let n = cfg.replica_count as usize;
        let durable_index = rec.log.len() as u64;
        let mut r = Replica {
            id,
            storage,
            rng: Prng::new(seed ^ (u64::from(id) << 56)),
            clock_ns: 0,
            term: rec.hard_state.term,
            voted_for: rec.hard_state.voted_for,
            log: rec.log,
            role: Role::Follower,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
            sm: StateMachine::new(),
            election_elapsed: 0,
            election_timeout: 0,
            heartbeat_elapsed: 0,
            votes: 0,
            next_index: vec![1; n],
            match_index: vec![0; n],
            probing: vec![false; n],
            pending: VecDeque::new(),
            inflight: FxHashMap::default(),
            durable_index,
            hs_dirty: false,
            log_dirty: false,
            outbox: Vec::new(),
            held: Vec::new(),
            cfg,
        };
        r.reset_election_timer();
        Ok(r)
    }

    // ----- accessors -----------------------------------------------------

    pub fn id(&self) -> u8 {
        self.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn term(&self) -> u64 {
        self.term
    }
    pub fn voted_for(&self) -> Option<u8> {
        self.voted_for
    }
    pub fn leader_id(&self) -> Option<u8> {
        self.leader_id
    }
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }
    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }
    pub fn durable_index(&self) -> u64 {
        self.durable_index
    }
    pub fn log(&self) -> &[Entry] {
        &self.log
    }
    pub fn state_machine(&self) -> &StateMachine {
        &self.sm
    }
    pub fn last_index(&self) -> u64 {
        self.log.len() as u64
    }
    pub fn pending_requests(&self) -> usize {
        self.pending.len()
    }
    pub fn needs_sync(&self) -> bool {
        self.hs_dirty || self.log_dirty
    }

    fn last_term(&self) -> u64 {
        self.log.last().map_or(0, |e| e.term)
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            Some(0)
        } else {
            self.log.get(index as usize - 1).map(|e| e.term)
        }
    }

    fn majority(&self) -> usize {
        self.cfg.replica_count as usize / 2 + 1
    }

    fn peers(&self) -> impl Iterator<Item = u8> {
        let me = self.id;
        (0..self.cfg.replica_count).filter(move |p| *p != me)
    }

    /// Sets the wall-clock time used to timestamp new log entries.
    pub fn set_clock(&mut self, now_ns: u64) {
        self.clock_ns = now_ns;
    }

    pub fn drain_outgoing(&mut self) -> Vec<Outgoing> {
        std::mem::take(&mut self.outbox)
    }

    // ----- persistence helpers ------------------------------------------

    fn persist_hard_state(&mut self) {
        self.storage
            .set_hard_state(HardState {
                term: self.term,
                voted_for: self.voted_for,
            })
            .expect("storage write failed (fail-stop)");
        self.hs_dirty = true;
    }

    fn append_local(&mut self, entries: Vec<Entry>) {
        if entries.is_empty() {
            return;
        }
        debug_assert_eq!(entries[0].index, self.last_index() + 1);
        self.storage
            .append(&entries)
            .expect("storage write failed (fail-stop)");
        self.log.extend(entries);
        self.log_dirty = true;
    }

    fn truncate_local(&mut self, from: u64) {
        assert!(
            from > self.commit_index,
            "replica {} asked to truncate committed entry {} (commit index {})",
            self.id,
            from,
            self.commit_index
        );
        self.storage
            .truncate_from(from)
            .expect("storage write failed (fail-stop)");
        self.log.truncate(from as usize - 1);
        self.durable_index = self.durable_index.min(from - 1);
        self.log_dirty = true;
        // Acknowledgements for entries that no longer exist must never leave.
        self.held.retain(|m| {
            !matches!(
                m,
                Outgoing::Peer {
                    msg: Message::AppendEntriesResponse { .. },
                    ..
                }
            )
        });
    }

    /// Makes all buffered writes durable and releases held messages.
    pub fn sync(&mut self) {
        if !self.needs_sync() {
            return;
        }
        self.storage
            .sync()
            .expect("storage sync failed (fail-stop)");
        self.hs_dirty = false;
        self.log_dirty = false;
        self.durable_index = self.last_index();
        let term = self.term;
        for m in std::mem::take(&mut self.held) {
            match &m {
                // A message from an older term is useless and possibly stale.
                Outgoing::Peer { msg, .. } if msg.term() != term => {}
                _ => self.outbox.push(m),
            }
        }
        if self.role == Role::Leader {
            self.advance_commit();
        }
    }

    fn send(&mut self, to: u8, msg: Message) {
        let hold = !self.cfg.bugs.ack_before_sync
            && (self.hs_dirty || (self.log_dirty && !matches!(msg, Message::AppendEntries { .. })));
        let out = Outgoing::Peer { to, msg };
        if hold {
            self.held.push(out);
        } else {
            self.outbox.push(out);
        }
    }

    fn send_reply(&mut self, reply: Reply) {
        let out = Outgoing::Reply(reply);
        if self.needs_sync() && !self.cfg.bugs.ack_before_sync {
            self.held.push(out);
        } else {
            self.outbox.push(out);
        }
    }

    // ----- timers --------------------------------------------------------

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = self.rng.range(
            u64::from(self.cfg.election_timeout_min_ticks),
            u64::from(self.cfg.election_timeout_max_ticks),
        ) as u32;
    }

    pub fn tick(&mut self) {
        match self.role {
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks {
                    self.heartbeat_elapsed = 0;
                    for p in self.peers().collect::<Vec<_>>() {
                        self.send_append(p);
                    }
                }
            }
            Role::Follower | Role::Candidate => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.election_timeout {
                    self.start_election();
                }
            }
        }
    }

    // ----- role transitions ---------------------------------------------

    fn become_follower(&mut self, term: u64, leader: Option<u8>) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
            self.persist_hard_state();
        }
        if self.role == Role::Leader {
            // Clients will retry against the new leader; sessions dedupe.
            self.pending.clear();
            self.inflight.clear();
        }
        self.role = Role::Follower;
        self.leader_id = leader;
        self.votes = 0;
        self.reset_election_timer();
    }

    fn start_election(&mut self) {
        self.term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.persist_hard_state();
        self.votes = 1 << self.id;
        self.reset_election_timer();
        if self.majority() == 1 {
            self.become_leader();
            return;
        }
        let msg = Message::RequestVote {
            term: self.term,
            candidate: self.id,
            last_log_index: self.last_index(),
            last_log_term: self.last_term(),
        };
        for p in self.peers().collect::<Vec<_>>() {
            self.send(p, msg.clone());
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        let next = self.last_index() + 1;
        self.next_index.iter_mut().for_each(|n| *n = next);
        self.match_index.iter_mut().for_each(|m| *m = 0);
        self.probing.iter_mut().for_each(|p| *p = false);
        self.heartbeat_elapsed = 0;
        self.pending.clear();
        self.inflight.clear();
        // A no-op from the new term lets the leader commit (and therefore
        // apply and answer for) entries left over from earlier terms.
        let e = Entry {
            term: self.term,
            index: self.last_index() + 1,
            timestamp: self.next_timestamp(),
            payload: Payload::Noop,
        };
        self.append_local(vec![e]);
        for p in self.peers().collect::<Vec<_>>() {
            self.send_append(p);
        }
        self.advance_commit();
    }

    fn next_timestamp(&self) -> u64 {
        let last = self.log.last().map_or(0, |e| e.timestamp);
        self.clock_ns.max(last + 1)
    }

    // ----- message handling ---------------------------------------------

    pub fn step(&mut self, from: u8, msg: Message) {
        if from >= self.cfg.replica_count || from == self.id {
            return;
        }
        if msg.term() > self.term {
            let leader = match &msg {
                Message::AppendEntries { leader, .. } => Some(*leader),
                _ => None,
            };
            self.become_follower(msg.term(), leader);
        }
        match msg {
            Message::RequestVote {
                term,
                candidate,
                last_log_index,
                last_log_term,
            } => self.on_request_vote(from, term, candidate, last_log_index, last_log_term),
            Message::RequestVoteResponse { term, granted } => {
                self.on_vote_response(from, term, granted)
            }
            Message::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.on_append_entries(
                from,
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            ),
            Message::AppendEntriesResponse {
                term,
                success,
                match_index,
                conflict_index,
                conflict_term,
            } => self.on_append_response(
                from,
                term,
                success,
                match_index,
                conflict_index,
                conflict_term,
            ),
        }
    }

    fn on_request_vote(
        &mut self,
        from: u8,
        term: u64,
        candidate: u8,
        last_index: u64,
        last_term: u64,
    ) {
        let mut granted = false;
        if term == self.term && candidate == from {
            let up_to_date = last_term > self.last_term()
                || (last_term == self.last_term() && last_index >= self.last_index());
            let free = self.voted_for.is_none() || self.voted_for == Some(candidate);
            if up_to_date && free && self.role == Role::Follower {
                granted = true;
                if self.voted_for != Some(candidate) {
                    self.voted_for = Some(candidate);
                    self.persist_hard_state();
                }
                self.reset_election_timer();
            }
        }
        let msg = Message::RequestVoteResponse {
            term: self.term,
            granted,
        };
        self.send(from, msg);
    }

    fn on_vote_response(&mut self, from: u8, term: u64, granted: bool) {
        if self.role != Role::Candidate || term != self.term || !granted {
            return;
        }
        self.votes |= 1 << from;
        if self.votes.count_ones() as usize >= self.majority() {
            self.become_leader();
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append_entries(
        &mut self,
        from: u8,
        term: u64,
        leader: u8,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    ) {
        let fail = |r: &Replica<S>, conflict_index: u64, conflict_term: u64| {
            Message::AppendEntriesResponse {
                term: r.term,
                success: false,
                match_index: 0,
                conflict_index,
                conflict_term,
            }
        };
        if term < self.term || leader != from {
            let m = fail(self, 0, 0);
            self.send(from, m);
            return;
        }
        assert!(
            self.role != Role::Leader,
            "election safety violated: replicas {} and {} both lead term {}",
            self.id,
            from,
            term
        );
        if self.role == Role::Candidate {
            self.become_follower(term, Some(leader));
        }
        self.leader_id = Some(leader);
        self.reset_election_timer();

        if prev_index > self.last_index() {
            let m = fail(self, self.last_index() + 1, 0);
            self.send(from, m);
            return;
        }
        let local_prev_term = self.term_at(prev_index).expect("prev_index <= last_index");
        if local_prev_term != prev_term {
            // Fast backtracking: report the first index of the conflicting
            // term so the leader can skip the whole term in one round trip.
            let mut first = prev_index;
            while first > 1 && self.term_at(first - 1) == Some(local_prev_term) {
                first -= 1;
            }
            let m = fail(self, first, local_prev_term);
            self.send(from, m);
            return;
        }

        let match_index = prev_index + entries.len() as u64;
        let mut to_append = Vec::new();
        for (i, e) in entries.into_iter().enumerate() {
            let idx = prev_index + 1 + i as u64;
            if !to_append.is_empty() {
                to_append.push(e);
                continue;
            }
            match self.term_at(idx) {
                Some(t) if t == e.term => {} // already have it (duplicate/reordered message)
                Some(_) => {
                    self.truncate_local(idx);
                    to_append.push(e);
                }
                None => to_append.push(e),
            }
        }
        self.append_local(to_append);

        let bound = if self.cfg.bugs.unbounded_follower_commit {
            self.last_index()
        } else {
            match_index
        };
        let new_commit = leader_commit.min(bound);
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.apply_committed();
        }
        let m = Message::AppendEntriesResponse {
            term: self.term,
            success: true,
            match_index,
            conflict_index: 0,
            conflict_term: 0,
        };
        self.send(from, m);
    }

    fn on_append_response(
        &mut self,
        from: u8,
        term: u64,
        success: bool,
        match_index: u64,
        conflict_index: u64,
        conflict_term: u64,
    ) {
        if self.role != Role::Leader || term != self.term {
            return;
        }
        let p = from as usize;
        let last = self.last_index();
        if success {
            if match_index > last {
                return; // cannot be from this term's leader; ignore defensively
            }
            if match_index > self.match_index[p] {
                self.match_index[p] = match_index;
            }
            self.next_index[p] = self.next_index[p].max(self.match_index[p] + 1);
            self.probing[p] = false;
            self.advance_commit();
            if self.next_index[p] <= self.last_index() {
                // Follower is still behind (catch-up or after a probe).
                self.send_append(from);
            }
        } else {
            let mut next = if conflict_term != 0 {
                match self.last_index_of_term(conflict_term) {
                    Some(i) => i + 1,
                    None => conflict_index,
                }
            } else {
                conflict_index
            };
            next = next.max(self.match_index[p] + 1).min(last + 1).max(1);
            self.next_index[p] = next;
            self.probing[p] = true;
            self.send_append(from);
        }
    }

    fn last_index_of_term(&self, term: u64) -> Option<u64> {
        self.log
            .iter()
            .rev()
            .find(|e| e.term == term)
            .map(|e| e.index)
    }

    fn send_append(&mut self, to: u8) {
        let p = to as usize;
        let next = self.next_index[p].clamp(1, self.last_index() + 1);
        let prev = next - 1;
        let prev_term = self.term_at(prev).expect("prev within log");
        let end = (prev as usize + self.cfg.max_entries_per_message).min(self.log.len());
        let entries = self.log[prev as usize..end].to_vec();
        if !self.probing[p] {
            // Pipelining: assume delivery; a rejection resets next_index.
            self.next_index[p] = prev + entries.len() as u64 + 1;
        }
        let msg = Message::AppendEntries {
            term: self.term,
            leader: self.id,
            prev_log_index: prev,
            prev_log_term: prev_term,
            entries,
            leader_commit: self.commit_index,
        };
        self.send(to, msg);
    }

    fn advance_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let majority = self.majority();
        let mut idx = self.last_index();
        while idx > self.commit_index {
            let entry_term = self.log[idx as usize - 1].term;
            if entry_term != self.term && !self.cfg.bugs.commit_old_term_entries {
                // Raft only commits entries from the current term by counting
                // replicas; older entries are committed indirectly.
                break;
            }
            let mut count = usize::from(self.durable_index >= idx);
            for p in self.peers() {
                if self.match_index[p as usize] >= idx {
                    count += 1;
                }
            }
            if count >= majority {
                self.commit_index = idx;
                self.apply_committed();
                return;
            }
            idx -= 1;
        }
    }

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            let idx = self.last_applied as usize;
            let results = self.sm.apply(&self.log[idx]);
            self.last_applied += 1;
            if self.role != Role::Leader {
                continue;
            }
            for a in results {
                match a {
                    Applied::Executed(r) | Applied::Duplicate(r) => {
                        if self.inflight.get(&r.client_id) == Some(&r.request_number) {
                            self.inflight.remove(&r.client_id);
                        }
                        self.send_reply(r);
                    }
                    Applied::Stale => {}
                }
            }
        }
    }

    // ----- client requests -----------------------------------------------

    /// Accepts a client request. Returns an immediate reply when the request
    /// can be answered without replication (not leader, or a duplicate of an
    /// already-applied request). Otherwise the reply is emitted as an
    /// [`Outgoing::Reply`] once the request is committed and applied.
    pub fn submit(&mut self, req: Request) -> Option<Reply> {
        if self.role != Role::Leader {
            return Some(Reply {
                client_id: req.client_id,
                request_number: req.request_number,
                status: ReplyStatus::NotLeader {
                    leader_hint: self.leader_id,
                },
            });
        }
        if let Some(s) = self.sm.session(req.client_id) {
            if req.request_number < s.last_request_number {
                return None;
            }
            if req.request_number == s.last_request_number {
                return Some(Reply {
                    client_id: req.client_id,
                    request_number: req.request_number,
                    status: ReplyStatus::Ok(s.last_reply.clone()),
                });
            }
        }
        if let Some(n) = self.inflight.get(&req.client_id) {
            if *n >= req.request_number {
                return None; // already queued or replicating
            }
        }
        self.inflight.insert(req.client_id, req.request_number);
        self.pending.push_back(req);
        None
    }

    /// Leader only: packs queued client requests into log entries and starts
    /// replicating them. Drivers call this after draining a burst of input,
    /// which is what produces batching under load.
    pub fn prepare(&mut self) {
        if self.role != Role::Leader || self.pending.is_empty() {
            return;
        }
        let mut entries = Vec::new();
        let mut index = self.last_index();
        let mut ts = self.next_timestamp();
        while !self.pending.is_empty() {
            let n = self.pending.len().min(self.cfg.max_requests_per_entry);
            let batch: Vec<Request> = self.pending.drain(..n).collect();
            index += 1;
            entries.push(Entry {
                term: self.term,
                index,
                timestamp: ts,
                payload: Payload::Batch(batch),
            });
            ts += 1;
        }
        self.append_local(entries);
        for p in self.peers().collect::<Vec<_>>() {
            if !self.probing[p as usize] {
                self.send_append(p);
            }
        }
        self.advance_commit();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Operation, ReplyBody};
    use crate::storage::{MemDevice, Wal};
    use ledger::{NewAccount, ResultCode};

    type R = Replica<Wal<MemDevice>>;

    /// Delivers all messages synchronously until quiescent.
    fn pump(rs: &mut [R]) -> Vec<Reply> {
        let mut replies = Vec::new();
        loop {
            let mut any = false;
            for i in 0..rs.len() {
                rs[i].prepare();
                rs[i].sync();
                for o in rs[i].drain_outgoing() {
                    any = true;
                    match o {
                        Outgoing::Peer { to, msg } => rs[to as usize].step(i as u8, msg),
                        Outgoing::Reply(r) => replies.push(r),
                    }
                }
            }
            if !any {
                return replies;
            }
        }
    }

    fn cluster(n: u8) -> Vec<R> {
        let cfg = Config {
            replica_count: n,
            ..Config::default()
        };
        (0..n)
            .map(|i| Replica::open(i, cfg.clone(), Wal::new(MemDevice::default()), 1).unwrap())
            .collect()
    }

    fn elect(rs: &mut [R], who: usize) {
        for (i, r) in rs.iter_mut().enumerate() {
            r.set_clock(1_000 + i as u64);
        }
        while rs[who].role() != Role::Leader {
            rs[who].tick();
            pump(rs);
        }
    }

    fn create_account(client: u128, n: u64, id: u128) -> Request {
        Request {
            client_id: client,
            request_number: n,
            operation: Operation::CreateAccounts(vec![NewAccount {
                id,
                ledger: 1,
                code: 1,
                flags: 0,
            }]),
        }
    }

    #[test]
    fn elects_leader_and_replicates() {
        let mut rs = cluster(3);
        elect(&mut rs, 0);
        assert_eq!(rs[1].leader_id(), Some(0));
        assert!(rs[0].submit(create_account(9, 1, 1)).is_none());
        let replies = pump(&mut rs);
        assert_eq!(replies.len(), 1);
        assert_eq!(
            replies[0].status,
            ReplyStatus::Ok(ReplyBody::Results(vec![ResultCode::Ok]))
        );
        // Heartbeat propagates the commit index to followers.
        for _ in 0..3 {
            rs[0].tick();
        }
        pump(&mut rs);
        for r in &rs {
            assert_eq!(r.commit_index(), 2);
            assert_eq!(r.state_machine().ledger.account_count(), 1);
        }
    }

    #[test]
    fn followers_redirect_and_duplicates_get_cached_reply() {
        let mut rs = cluster(3);
        elect(&mut rs, 1);
        let r = rs[0].submit(create_account(9, 1, 1)).unwrap();
        assert_eq!(
            r.status,
            ReplyStatus::NotLeader {
                leader_hint: Some(1)
            }
        );
        rs[1].submit(create_account(9, 1, 1));
        // A retry while in flight is not appended twice.
        assert!(rs[1].submit(create_account(9, 1, 1)).is_none());
        assert_eq!(rs[1].pending_requests(), 1);
        pump(&mut rs);
        let cached = rs[1].submit(create_account(9, 1, 1)).unwrap();
        assert_eq!(
            cached.status,
            ReplyStatus::Ok(ReplyBody::Results(vec![ResultCode::Ok]))
        );
        // Older request numbers are ignored.
        assert!(rs[1].submit(create_account(9, 0, 1)).is_none());
    }

    #[test]
    fn votes_and_acks_are_held_until_sync() {
        let mut rs = cluster(3);
        for _ in 0..100 {
            rs[0].tick();
            if rs[0].role() == Role::Candidate {
                break;
            }
        }
        assert_eq!(rs[0].role(), Role::Candidate);
        assert!(rs[0].needs_sync());
        assert!(
            rs[0].drain_outgoing().is_empty(),
            "RequestVote must wait for term/vote fsync"
        );
        rs[0].sync();
        assert_eq!(rs[0].drain_outgoing().len(), 2);
    }

    #[test]
    fn restart_recovers_log_and_reapplies() {
        let cfg = Config::default();
        let mut rs = cluster(3);
        elect(&mut rs, 2);
        for n in 1..=5u64 {
            rs[2].submit(create_account(4, n, n as u128));
            pump(&mut rs);
        }
        for _ in 0..3 {
            rs[2].tick();
        }
        pump(&mut rs);
        // "Crash" replica 0 and reopen it from an image of what it had made durable.
        let mut image = Wal::new(MemDevice::default());
        image
            .set_hard_state(HardState {
                term: rs[0].term(),
                voted_for: rs[0].voted_for(),
            })
            .unwrap();
        image.append(rs[0].log()).unwrap();
        let mut r0 = Replica::open(0, cfg, Wal::new(image.device().clone()), 7).unwrap();
        assert_eq!(r0.last_index(), rs[2].last_index());
        assert_eq!(r0.commit_index(), 0);
        rs[0] = r0;
        for _ in 0..3 {
            rs[2].tick();
        }
        pump(&mut rs);
        r0 = rs.remove(0);
        assert_eq!(r0.last_applied(), rs[1].last_applied());
        assert_eq!(r0.state_machine().digest(), rs[1].state_machine().digest());
    }
}
