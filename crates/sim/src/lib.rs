//! Deterministic simulation of a full quorum-ledger cluster.
//!
//! One seed fully determines a run: cluster size, fault rates, workload, every
//! message delay, drop, duplicate, partition, crash, torn write and clock
//! jump. Everything runs on a single thread against a virtual clock, so a
//! failing seed replays bit-for-bit.
//!
//! A run has two phases:
//!
//! 1. **Fault phase** – clients issue requests while the network drops,
//!    duplicates, delays and reorders messages, replicas crash and restart
//!    (losing unsynced writes, sometimes with torn or corrupted tails),
//!    partitions come and go and replica clocks drift and jump.
//! 2. **Heal phase** – faults stop, every replica is restarted, the network
//!    becomes reliable. The cluster must then finish every client request and
//!    converge within a deadline (liveness).
//!
//! Safety checks run continuously; see [`checker`].

pub mod checker;
pub mod disk;
pub mod workload;

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;

use consensus::message::Message;
use consensus::prng::Prng;
use consensus::{Config, InjectedBugs, Outgoing, Replica, Reply, ReplyStatus, Request, Role, Wal};
use ledger::codec::Fnv64;

use checker::Checker;
use disk::{SimDevice, SimDisk};
use workload::Workload;

type SimReplica = Replica<Wal<SimDevice>>;

const MS: u64 = 1_000; // virtual time unit is the microsecond
/// A healthy run needs well under a million events; far more means the
/// cluster is in a message storm or livelock, which is itself a bug.
const MAX_EVENTS: u64 = 5_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Endpoint {
    Replica(u8),
    Client(u32),
}

#[derive(Debug, Clone)]
enum NetPayload {
    Peer(Message),
    Request(Request),
    Reply(Reply),
}

#[derive(Debug, Clone)]
enum Event {
    Deliver {
        from: Endpoint,
        to: Endpoint,
        payload: NetPayload,
    },
    Tick {
        replica: u8,
        epoch: u32,
    },
    Sync {
        replica: u8,
        epoch: u32,
    },
    Crash {
        replica: u8,
    },
    Restart {
        replica: u8,
    },
    ClientSend {
        client: u32,
    },
    ClientTimeout {
        client: u32,
        attempt: u64,
    },
    ClientRetry {
        client: u32,
        attempt: u64,
    },
    NetworkChange,
    ClockJump {
        replica: u8,
    },
    Heal,
}

struct Scheduled {
    time: u64,
    seq: u64,
    event: Event,
}

impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        (self.time, self.seq) == (other.time, other.seq)
    }
}
impl Eq for Scheduled {}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap on (time, seq).
        (other.time, other.seq).cmp(&(self.time, self.seq))
    }
}

/// Fault and workload parameters, all derived from the seed ("swarm testing":
/// each seed explores a different mix instead of one fixed average).
#[derive(Debug, Clone)]
pub struct Params {
    pub replica_count: u8,
    pub clients: u32,
    /// Clients issue requests continuously during the fault phase, then this
    /// many more after healing (which must all complete: liveness).
    pub requests_after_heal: u32,
    pub think_time_max_us: u64,
    pub max_events_per_request: u64,
    pub account_space: u128,
    pub drop_prob: f64,
    pub dup_prob: f64,
    pub delay_min_us: u64,
    pub delay_max_us: u64,
    pub network_change_mean_us: Option<u64>,
    pub crash_mean_us: Option<u64>,
    pub restart_max_us: u64,
    pub torn_write_prob: f64,
    pub corrupt_prob: f64,
    pub sync_latency_max_us: u64,
    /// Probability of crashing a replica right after it issued writes and
    /// before they were synced: the most interesting moment to crash.
    pub crash_on_write_prob: f64,
    pub tick_us: Vec<u64>,
    pub clock_offset_ns: Vec<i64>,
    pub clock_jump_mean_us: Option<u64>,
    pub fault_phase_us: u64,
    pub max_requests_per_entry: usize,
    pub max_events_per_entry: usize,
    pub max_entries_per_message: usize,
}

impl Params {
    pub fn from_seed(rng: &mut Prng) -> Self {
        let replica_count = [3u8, 3, 3, 3, 3, 3, 5, 5, 5, 1][rng.below(10) as usize];
        let n = replica_count as usize;
        let maybe = |rng: &mut Prng, p: f64, lo: u64, hi: u64| {
            if rng.chance(p) {
                Some(rng.range(lo, hi))
            } else {
                None
            }
        };
        Params {
            replica_count,
            clients: rng.range(1, 6) as u32,
            requests_after_heal: rng.range(1, 20) as u32,
            think_time_max_us: rng.range(MS, 100 * MS),
            max_events_per_request: rng.range(1, 8),
            account_space: u128::from(rng.range(4, 16)),
            drop_prob: if rng.chance(0.7) {
                rng.range(0, 200) as f64 / 1000.0
            } else {
                0.0
            },
            dup_prob: if rng.chance(0.5) {
                rng.range(0, 100) as f64 / 1000.0
            } else {
                0.0
            },
            delay_min_us: rng.range(50, 2 * MS),
            delay_max_us: [5 * MS, 20 * MS, 80 * MS, 300 * MS][rng.below(4) as usize],
            network_change_mean_us: maybe(rng, 0.7, 100 * MS, 3_000 * MS),
            crash_mean_us: maybe(rng, 0.8, 200 * MS, 5_000 * MS),
            restart_max_us: rng.range(10 * MS, 3_000 * MS),
            torn_write_prob: rng.range(0, 100) as f64 / 100.0,
            corrupt_prob: rng.range(0, 30) as f64 / 100.0,
            sync_latency_max_us: if rng.chance(0.3) {
                rng.range(5 * MS, 50 * MS)
            } else {
                rng.range(50, 5 * MS)
            },
            crash_on_write_prob: if rng.chance(0.5) {
                rng.range(1, 30) as f64 / 1000.0
            } else {
                0.0
            },
            tick_us: (0..n).map(|_| rng.range(9 * MS, 11 * MS)).collect(),
            clock_offset_ns: (0..n)
                .map(|_| rng.range(0, 4_000_000_000) as i64 - 2_000_000_000)
                .collect(),
            clock_jump_mean_us: maybe(rng, 0.3, 500 * MS, 5_000 * MS),
            fault_phase_us: rng.range(1_000 * MS, 20_000 * MS),
            max_requests_per_entry: rng.range(1, 64) as usize,
            max_events_per_entry: rng.range(1, 100) as usize,
            max_entries_per_message: if rng.chance(0.3) {
                rng.range(1, 2)
            } else {
                rng.range(1, 64)
            } as usize,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub events: u64,
    pub sim_time_us: u64,
    pub messages_sent: u64,
    pub messages_dropped: u64,
    pub messages_duplicated: u64,
    pub crashes: u64,
    pub torn_writes: u64,
    pub bit_flips: u64,
    pub lost_unsynced_writes: u64,
    pub network_changes: u64,
    pub clock_jumps: u64,
    pub max_term: u64,
    pub committed_entries: u64,
    pub requests_acked: u64,
    pub client_retries: u64,
    pub trace_hash: u64,
    pub final_digest: u64,
}

#[derive(Debug, Clone)]
pub struct Failure {
    pub seed: u64,
    pub time_us: u64,
    pub message: String,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {} failed at t={}ms: {}\nreproduce: cargo run --release -p sim -- --seed {}",
            self.seed,
            self.time_us / MS,
            self.message,
            self.seed
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct Options {
    pub bugs: InjectedBugs,
    pub verbose: bool,
}

struct SimClient {
    id: u128,
    next_request_number: u64,
    remaining: u32,
    current: Option<Request>,
    target: u8,
    attempt: u64,
}

struct Network {
    /// `blocked[from][to]`: replica-to-replica link is down.
    blocked: Vec<Vec<bool>>,
}

pub struct Simulation {
    seed: u64,
    params: Params,
    opts: Options,
    cfg: Config,
    rng: Prng,
    now: u64,
    seq: u64,
    queue: BinaryHeap<Scheduled>,
    replicas: Vec<Option<SimReplica>>,
    disks: Vec<SimDevice>,
    epochs: Vec<u32>,
    sync_scheduled: Vec<bool>,
    clock_jump_ns: Vec<i64>,
    clients: Vec<SimClient>,
    workload: Workload,
    net: Network,
    healed: bool,
    drop_prob: f64,
    dup_prob: f64,
    delay_max_us: u64,
    checker: Checker,
    stats: Stats,
    trace: Fnv64,
}

impl Simulation {
    pub fn new(seed: u64, opts: Options) -> Self {
        let mut rng = Prng::new(seed);
        let params = Params::from_seed(&mut rng);
        let n = params.replica_count as usize;
        let cfg = Config {
            replica_count: params.replica_count,
            election_timeout_min_ticks: 15,
            election_timeout_max_ticks: 30,
            heartbeat_ticks: 3,
            max_requests_per_entry: params.max_requests_per_entry,
            max_events_per_entry: params.max_events_per_entry,
            max_entries_per_message: params.max_entries_per_message,
            bugs: opts.bugs,
        };
        let workload = Workload::new(
            rng.fork(),
            params.account_space,
            params.max_events_per_request,
        );
        let clients = (0..params.clients)
            .map(|i| SimClient {
                id: (u128::from(seed) << 64) | u128::from(i + 1),
                next_request_number: 1,
                remaining: params.requests_after_heal,
                current: None,
                target: (i % u32::from(params.replica_count)) as u8,
                attempt: 0,
            })
            .collect();
        let mut sim = Simulation {
            seed,
            cfg,
            rng,
            now: 0,
            seq: 0,
            queue: BinaryHeap::new(),
            replicas: (0..n).map(|_| None).collect(),
            disks: (0..n).map(|_| SimDevice::default()).collect(),
            epochs: vec![0; n],
            sync_scheduled: vec![false; n],
            clock_jump_ns: vec![0; n],
            clients,
            workload,
            net: Network {
                blocked: vec![vec![false; n]; n],
            },
            healed: false,
            drop_prob: params.drop_prob,
            dup_prob: params.dup_prob,
            delay_max_us: params.delay_max_us,
            checker: Checker::new(n),
            stats: Stats::default(),
            trace: Fnv64::default(),
            opts,
            params,
        };
        for r in 0..n as u8 {
            sim.start_replica(r);
        }
        for c in 0..sim.clients.len() as u32 {
            let t = sim.rng.range(0, 50 * MS);
            sim.schedule(t, Event::ClientSend { client: c });
        }
        if let Some(mean) = sim.params.network_change_mean_us {
            let t = sim.rng.range(0, 2 * mean);
            sim.schedule(t, Event::NetworkChange);
        }
        if let Some(mean) = sim.params.crash_mean_us {
            let t = sim.rng.range(0, 2 * mean);
            let r = sim.rng.below(n as u64) as u8;
            sim.schedule(t, Event::Crash { replica: r });
        }
        if let Some(mean) = sim.params.clock_jump_mean_us {
            let t = sim.rng.range(0, 2 * mean);
            let r = sim.rng.below(n as u64) as u8;
            sim.schedule(t, Event::ClockJump { replica: r });
        }
        let heal_at = sim.params.fault_phase_us;
        sim.schedule(heal_at, Event::Heal);
        sim
    }

    pub fn params(&self) -> &Params {
        &self.params
    }

    fn schedule(&mut self, delay: u64, event: Event) {
        self.seq += 1;
        self.queue.push(Scheduled {
            time: self.now + delay,
            seq: self.seq,
            event,
        });
    }

    fn fail(&self, message: impl Into<String>) -> Failure {
        Failure {
            seed: self.seed,
            time_us: self.now,
            message: message.into(),
        }
    }

    fn log(&self, msg: impl FnOnce() -> String) {
        if self.opts.verbose {
            println!("[{:>9.3}ms] {}", self.now as f64 / MS as f64, msg());
        }
    }

    fn replica_clock_ns(&self, r: u8) -> u64 {
        let base = (self.now * 1_000) as i64;
        let t = base + self.params.clock_offset_ns[r as usize] + self.clock_jump_ns[r as usize];
        t.max(1) as u64
    }

    fn start_replica(&mut self, r: u8) {
        let seed = self.rng.next_u64();
        let replica = Replica::open(
            r,
            self.cfg.clone(),
            Wal::new(self.disks[r as usize].clone()),
            seed,
        )
        .expect("simulated storage never returns io errors");
        self.log(|| {
            format!(
                "replica {r} started: term {} log {} entries",
                replica.term(),
                replica.last_index()
            )
        });
        self.replicas[r as usize] = Some(replica);
        self.epochs[r as usize] += 1;
        self.sync_scheduled[r as usize] = false;
        self.checker.on_restart(r);
        let epoch = self.epochs[r as usize];
        let first_tick = self.rng.range(1, self.params.tick_us[r as usize]);
        self.schedule(first_tick, Event::Tick { replica: r, epoch });
    }

    fn send(&mut self, from: Endpoint, to: Endpoint, payload: NetPayload) {
        self.stats.messages_sent += 1;
        if self.rng.chance(self.drop_prob) {
            self.stats.messages_dropped += 1;
            return;
        }
        let copies = if self.rng.chance(self.dup_prob) {
            self.stats.messages_duplicated += 1;
            2
        } else {
            1
        };
        for _ in 0..copies {
            let delay = self.rng.range(
                self.params.delay_min_us,
                self.delay_max_us.max(self.params.delay_min_us),
            );
            self.schedule(
                delay,
                Event::Deliver {
                    from,
                    to,
                    payload: payload.clone(),
                },
            );
        }
    }

    /// Collects a replica's output, schedules its fsync, runs safety checks.
    fn after_replica(&mut self, r: u8) -> Result<(), Failure> {
        let Some(replica) = self.replicas[r as usize].as_mut() else {
            return Ok(());
        };
        let out = replica.drain_outgoing();
        let needs_sync = replica.needs_sync();
        if let Err(e) = self
            .checker
            .check_replica(self.replicas[r as usize].as_ref().unwrap())
        {
            return Err(self.fail(e));
        }
        for o in out {
            match o {
                Outgoing::Peer { to, msg } => self.send(
                    Endpoint::Replica(r),
                    Endpoint::Replica(to),
                    NetPayload::Peer(msg),
                ),
                Outgoing::Reply(reply) => {
                    if let Some(c) = self.client_index(reply.client_id) {
                        self.send(
                            Endpoint::Replica(r),
                            Endpoint::Client(c),
                            NetPayload::Reply(reply),
                        );
                    }
                }
            }
        }
        if needs_sync && !self.healed && self.rng.chance(self.params.crash_on_write_prob) {
            // Messages emitted above are already in flight; anything held for
            // fsync dies with the process.
            self.crash(r);
            let delay = self.rng.range(MS, self.params.restart_max_us);
            self.schedule(delay, Event::Restart { replica: r });
            return Ok(());
        }
        if needs_sync && !self.sync_scheduled[r as usize] {
            self.sync_scheduled[r as usize] = true;
            let epoch = self.epochs[r as usize];
            let latency = self.rng.range(10, self.params.sync_latency_max_us);
            self.schedule(latency, Event::Sync { replica: r, epoch });
        }
        Ok(())
    }

    fn client_index(&self, id: u128) -> Option<u32> {
        let i = (id & 0xFFFF_FFFF) as u32;
        (i >= 1 && (i as usize) <= self.clients.len() && self.clients[i as usize - 1].id == id)
            .then(|| i - 1)
    }

    fn client_send(&mut self, c: u32) {
        let client = &mut self.clients[c as usize];
        let Some(req) = client.current.clone() else {
            return;
        };
        client.attempt += 1;
        let attempt = client.attempt;
        let target = client.target;
        self.send(
            Endpoint::Client(c),
            Endpoint::Replica(target),
            NetPayload::Request(req),
        );
        self.schedule(500 * MS, Event::ClientTimeout { client: c, attempt });
    }

    fn rotate_target(&mut self, c: u32, hint: Option<u8>) {
        let n = self.params.replica_count;
        let client = &mut self.clients[c as usize];
        client.target = match hint {
            Some(h) if h < n && h != client.target => h,
            _ => (client.target + 1) % n,
        };
    }

    fn handle(&mut self, event: Event) -> Result<(), Failure> {
        match event {
            Event::Deliver { from, to, payload } => match to {
                Endpoint::Replica(r) => {
                    if let Endpoint::Replica(f) = from {
                        if self.net.blocked[f as usize][r as usize] {
                            self.stats.messages_dropped += 1;
                            return Ok(());
                        }
                    }
                    let clock = self.replica_clock_ns(r);
                    let Some(replica) = self.replicas[r as usize].as_mut() else {
                        return Ok(());
                    };
                    replica.set_clock(clock);
                    let mut immediate = None;
                    match payload {
                        NetPayload::Peer(msg) => {
                            let Endpoint::Replica(f) = from else {
                                unreachable!()
                            };
                            replica.step(f, msg);
                        }
                        NetPayload::Request(req) => immediate = replica.submit(req),
                        NetPayload::Reply(_) => unreachable!("replies go to clients"),
                    }
                    if let (Some(reply), Endpoint::Client(c)) = (immediate, from) {
                        self.send(
                            Endpoint::Replica(r),
                            Endpoint::Client(c),
                            NetPayload::Reply(reply),
                        );
                    }
                    self.after_replica(r)?;
                }
                Endpoint::Client(c) => {
                    let NetPayload::Reply(reply) = payload else {
                        unreachable!()
                    };
                    self.on_client_reply(c, reply)?;
                }
            },
            Event::Tick { replica: r, epoch } => {
                if self.epochs[r as usize] != epoch || self.replicas[r as usize].is_none() {
                    return Ok(());
                }
                let clock = self.replica_clock_ns(r);
                let replica = self.replicas[r as usize].as_mut().unwrap();
                replica.set_clock(clock);
                replica.tick();
                replica.prepare();
                self.after_replica(r)?;
                let period = self.params.tick_us[r as usize];
                self.schedule(period, Event::Tick { replica: r, epoch });
            }
            Event::Sync { replica: r, epoch } => {
                if self.epochs[r as usize] != epoch {
                    return Ok(());
                }
                self.sync_scheduled[r as usize] = false;
                let clock = self.replica_clock_ns(r);
                if let Some(replica) = self.replicas[r as usize].as_mut() {
                    replica.set_clock(clock);
                    replica.sync();
                    replica.prepare();
                    self.after_replica(r)?;
                }
            }
            Event::Crash { replica: r } => {
                if !self.healed {
                    if self.replicas[r as usize].is_some() {
                        self.crash(r);
                        let delay = self.rng.range(MS, self.params.restart_max_us);
                        self.schedule(delay, Event::Restart { replica: r });
                    }
                    let mean = self.params.crash_mean_us.unwrap();
                    let next = self.rng.range(1, 2 * mean);
                    let who = self.rng.below(self.params.replica_count as u64) as u8;
                    self.schedule(next, Event::Crash { replica: who });
                }
            }
            Event::Restart { replica: r } => {
                if self.replicas[r as usize].is_none() {
                    self.start_replica(r);
                }
            }
            Event::ClientSend { client: c } => self.client_start_next(c),
            Event::ClientTimeout { client: c, attempt } => {
                let client = &self.clients[c as usize];
                if client.attempt == attempt && client.current.is_some() {
                    self.stats.client_retries += 1;
                    self.rotate_target(c, None);
                    self.client_send(c);
                }
            }
            Event::ClientRetry { client: c, attempt } => {
                let client = &self.clients[c as usize];
                if client.attempt == attempt && client.current.is_some() {
                    self.client_send(c);
                }
            }
            Event::NetworkChange => {
                if !self.healed {
                    self.change_network();
                    let mean = self.params.network_change_mean_us.unwrap();
                    let next = self.rng.range(1, 2 * mean);
                    self.schedule(next, Event::NetworkChange);
                }
            }
            Event::ClockJump { replica: r } => {
                if !self.healed {
                    // Jump the wall clock forwards or backwards by up to 10 s.
                    let jump = self.rng.range(0, 20_000_000_000) as i64 - 10_000_000_000;
                    self.clock_jump_ns[r as usize] += jump;
                    self.stats.clock_jumps += 1;
                    self.log(|| format!("replica {r} clock jumps by {}ms", jump / 1_000_000));
                    let mean = self.params.clock_jump_mean_us.unwrap();
                    let next = self.rng.range(1, 2 * mean);
                    let who = self.rng.below(self.params.replica_count as u64) as u8;
                    self.schedule(next, Event::ClockJump { replica: who });
                }
            }
            Event::Heal => {
                self.healed = true;
                self.drop_prob = 0.0;
                self.dup_prob = 0.0;
                // Keep round trips well inside the election timeout.
                self.delay_max_us = self.delay_max_us.min(20 * MS);
                for row in &mut self.net.blocked {
                    row.iter_mut().for_each(|b| *b = false);
                }
                self.log(|| "heal: network reliable, all replicas restarting".into());
                for r in 0..self.params.replica_count {
                    if self.replicas[r as usize].is_none() {
                        self.start_replica(r);
                    }
                }
            }
        }
        Ok(())
    }

    fn crash(&mut self, r: u8) {
        self.replicas[r as usize] = None;
        self.epochs[r as usize] += 1;
        self.stats.crashes += 1;
        let damage = {
            let disk: &mut SimDisk = &mut self.disks[r as usize].0.borrow_mut();
            disk.crash(
                &mut self.rng,
                self.params.torn_write_prob,
                self.params.corrupt_prob,
            )
        };
        self.stats.lost_unsynced_writes += damage.lost_writes as u64;
        self.stats.torn_writes += u64::from(damage.torn);
        self.stats.bit_flips += u64::from(damage.bit_flipped);
        self.log(|| format!("replica {r} crashed: {damage:?}"));
    }

    fn change_network(&mut self) {
        self.stats.network_changes += 1;
        let n = self.params.replica_count as usize;
        for row in &mut self.net.blocked {
            row.iter_mut().for_each(|b| *b = false);
        }
        match self.rng.below(4) {
            0 => {} // fully connected
            1 => {
                // Isolate one replica.
                let x = self.rng.below(n as u64) as usize;
                for i in 0..n {
                    if i != x {
                        self.net.blocked[i][x] = true;
                        self.net.blocked[x][i] = true;
                    }
                }
            }
            2 => {
                // Random bipartition.
                let side: Vec<bool> = (0..n).map(|_| self.rng.chance(0.5)).collect();
                for i in 0..n {
                    for j in 0..n {
                        if side[i] != side[j] {
                            self.net.blocked[i][j] = true;
                        }
                    }
                }
            }
            _ => {
                // Some one-way link failures.
                for i in 0..n {
                    for j in 0..n {
                        if i != j && self.rng.chance(0.3) {
                            self.net.blocked[i][j] = true;
                        }
                    }
                }
            }
        }
        self.log(|| format!("network: blocked links {:?}", self.net.blocked));
    }

    fn client_start_next(&mut self, c: u32) {
        let healed = self.healed;
        let client = &self.clients[c as usize];
        if client.current.is_some() || (healed && client.remaining == 0) {
            return;
        }
        let operation = self.workload.next_operation();
        let client = &mut self.clients[c as usize];
        if healed {
            client.remaining -= 1;
        }
        let req = Request {
            client_id: client.id,
            request_number: client.next_request_number,
            operation,
        };
        client.next_request_number += 1;
        self.checker.on_first_send(&req, self.now);
        client.current = Some(req);
        self.client_send(c);
    }

    fn on_client_reply(&mut self, c: u32, reply: Reply) -> Result<(), Failure> {
        let client = &self.clients[c as usize];
        let Some(cur) = &client.current else {
            return Ok(());
        };
        if reply.request_number != cur.request_number || reply.client_id != client.id {
            return Ok(()); // late duplicate of an earlier reply
        }
        match &reply.status {
            ReplyStatus::NotLeader { leader_hint } => {
                let hint = *leader_hint;
                self.rotate_target(c, hint);
                // Back off briefly so a leaderless cluster is not hammered.
                let attempt = self.clients[c as usize].attempt;
                self.schedule(20 * MS, Event::ClientRetry { client: c, attempt });
            }
            ReplyStatus::Ok(_) => {
                let req = self.clients[c as usize].current.take().unwrap();
                self.stats.requests_acked += 1;
                if let Err(e) = self.checker.on_ack(&req, &reply, self.now) {
                    return Err(self.fail(e));
                }
                let think = self.rng.range(0, self.params.think_time_max_us);
                self.schedule(think, Event::ClientSend { client: c });
            }
        }
        Ok(())
    }

    fn workload_done(&self) -> bool {
        self.clients
            .iter()
            .all(|c| c.remaining == 0 && c.current.is_none())
    }

    /// All replicas up, one leader, everyone has committed and applied the
    /// leader's whole log and nothing is waiting for fsync.
    fn converged(&self) -> bool {
        let mut leader = None;
        for r in self.replicas.iter() {
            let Some(r) = r else { return false };
            if r.role() == Role::Leader {
                leader = Some(r);
            }
        }
        let Some(leader) = leader else { return false };
        let target = leader.last_index();
        self.replicas.iter().flatten().all(|r| {
            r.commit_index() == target
                && r.last_applied() == target
                && !r.needs_sync()
                && r.term() == leader.term()
        })
    }

    pub fn run(mut self) -> Result<Stats, Failure> {
        let deadline = self.params.fault_phase_us + 60_000 * MS;
        let mut since_invariants = 0u64;
        while let Some(s) = self.queue.pop() {
            self.now = s.time;
            if self.now > deadline {
                let state: Vec<String> = self
                    .replicas
                    .iter()
                    .map(|r| match r {
                        Some(r) => format!(
                            "{{role {:?} term {} last {} commit {} applied {}}}",
                            r.role(),
                            r.term(),
                            r.last_index(),
                            r.commit_index(),
                            r.last_applied()
                        ),
                        None => "down".into(),
                    })
                    .collect();
                let outstanding = self
                    .clients
                    .iter()
                    .filter(|c| c.current.is_some() || c.remaining > 0)
                    .count();
                return Err(self.fail(format!(
                    "liveness: cluster did not converge within 60s of healing; {outstanding} clients unfinished; replicas {}",
                    state.join(" ")
                )));
            }
            self.stats.events += 1;
            if self.stats.events > MAX_EVENTS {
                return Err(self.fail(format!(
                    "event budget of {MAX_EVENTS} exceeded (message storm or livelock); {} events queued",
                    self.queue.len()
                )));
            }
            self.trace.write(&s.time.to_le_bytes());
            self.trace.write(&s.seq.to_le_bytes());
            if self.opts.verbose {
                if let Event::Deliver { from, to, payload } = &s.event {
                    let desc = match payload {
                        NetPayload::Peer(m) => format!("{m:?}"),
                        NetPayload::Request(r) => {
                            format!("request c{} #{}", r.client_id & 0xFFFF, r.request_number)
                        }
                        NetPayload::Reply(r) => {
                            format!("reply #{} {:?}", r.request_number, r.status)
                        }
                    };
                    let desc: String = desc.chars().take(200).collect();
                    self.log(|| format!("{from:?} -> {to:?}: {desc}"));
                }
            }
            self.handle(s.event)?;

            since_invariants += 1;
            if since_invariants >= 2_000 {
                since_invariants = 0;
                for r in self.replicas.iter().flatten() {
                    if let Err(e) = r.state_machine().ledger.check_invariants() {
                        return Err(self.fail(format!("replica {} ledger invariant: {e}", r.id())));
                    }
                }
            }
            if self.healed && self.workload_done() && self.converged() {
                break;
            }
        }
        self.finish()
    }

    fn finish(mut self) -> Result<Stats, Failure> {
        let replicas: Vec<&SimReplica> = self.replicas.iter().flatten().collect();
        for r in &replicas {
            self.stats.max_term = self.stats.max_term.max(r.term());
        }
        let reference = self
            .checker
            .final_check(&replicas)
            .map_err(|e| self.fail(e))?;
        self.stats.committed_entries = self.checker.committed_len();
        self.stats.sim_time_us = self.now;
        self.stats.final_digest = reference;
        self.trace.write(&reference.to_le_bytes());
        self.stats.trace_hash = self.trace.finish();
        Ok(self.stats)
    }
}

/// Runs one seed, converting panics (failed assertions inside the replica or
/// state machine) into failures.
pub fn run_seed(seed: u64, opts: Options) -> Result<Stats, Failure> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        Simulation::new(seed, opts).run()
    }));
    match result {
        Ok(r) => r,
        Err(panic) => {
            let msg = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "unknown panic".into());
            Err(Failure {
                seed,
                time_us: 0,
                message: format!("panic: {msg}"),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_trace() {
        for seed in [1u64, 2, 3, 42, 1000] {
            let a = run_seed(seed, Options::default()).unwrap();
            let b = run_seed(seed, Options::default()).unwrap();
            assert_eq!(a.trace_hash, b.trace_hash, "seed {seed} not deterministic");
            assert_eq!(a.final_digest, b.final_digest);
        }
    }

    #[test]
    fn a_few_hundred_seeds_pass() {
        let mut failures = Vec::new();
        for seed in 0..200 {
            if let Err(f) = run_seed(seed, Options::default()) {
                failures.push(f.to_string());
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
}
