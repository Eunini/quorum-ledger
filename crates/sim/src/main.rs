//! Simulator CLI.
//!
//! ```text
//! sim --seed 1234 [-v]                 # replay one seed (verbose trace with -v)
//! sim --seeds 100000 [--start 0]       # VOPR: run a seed range across threads
//! sim --duration 60s                   # VOPR: run random seeds for a while
//!     [--threads 3] [--check-determinism] [--inject-bug NAME]
//! ```

use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use consensus::InjectedBugs;
use sim::{run_seed, Failure, Options, Simulation};

struct Args {
    seed: Option<u64>,
    seeds: Option<u64>,
    duration: Option<Duration>,
    start: Option<u64>,
    threads: usize,
    verbose: bool,
    check_determinism: bool,
    bugs: InjectedBugs,
}

fn usage() -> ! {
    eprintln!(
        "usage: sim (--seed N [-v] | --seeds N [--start S] | --duration 60s [--start S]) \
         [--threads T] [--check-determinism] \
         [--inject-bug commit-old-term|ack-before-sync|unbounded-follower-commit|vote-ignores-log]"
    );
    std::process::exit(2);
}

fn parse_duration(s: &str) -> Option<Duration> {
    let (num, mult) = if let Some(v) = s.strip_suffix("ms") {
        (v, 0.001)
    } else if let Some(v) = s.strip_suffix('s') {
        (v, 1.0)
    } else if let Some(v) = s.strip_suffix('m') {
        (v, 60.0)
    } else if let Some(v) = s.strip_suffix('h') {
        (v, 3600.0)
    } else {
        (s, 1.0)
    };
    num.parse::<f64>()
        .ok()
        .map(|n| Duration::from_secs_f64(n * mult))
}

fn parse_args() -> Args {
    let mut a = Args {
        seed: None,
        seeds: None,
        duration: None,
        start: None,
        threads: 3,
        verbose: false,
        check_determinism: false,
        bugs: InjectedBugs::default(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| usage());
        match arg.as_str() {
            "--seed" => a.seed = Some(val().parse().unwrap_or_else(|_| usage())),
            "--seeds" => a.seeds = Some(val().parse().unwrap_or_else(|_| usage())),
            "--start" => a.start = Some(val().parse().unwrap_or_else(|_| usage())),
            "--duration" => a.duration = Some(parse_duration(&val()).unwrap_or_else(|| usage())),
            "--threads" => a.threads = val().parse().unwrap_or_else(|_| usage()),
            "-v" | "--verbose" => a.verbose = true,
            "--check-determinism" => a.check_determinism = true,
            "--inject-bug" => match val().as_str() {
                "commit-old-term" => a.bugs.commit_old_term_entries = true,
                "ack-before-sync" => a.bugs.ack_before_sync = true,
                "unbounded-follower-commit" => a.bugs.unbounded_follower_commit = true,
                "vote-ignores-log" => a.bugs.vote_ignores_log = true,
                _ => usage(),
            },
            _ => usage(),
        }
    }
    a.threads = a.threads.max(1);
    a
}

fn run_one(seed: u64, args: &Args) -> ExitCode {
    let opts = Options {
        bugs: args.bugs,
        verbose: args.verbose,
    };
    println!("seed {seed}");
    println!("{:#?}", Simulation::new(seed, Options::default()).params());
    let started = Instant::now();
    match run_seed(seed, opts.clone()) {
        Ok(stats) => {
            println!("{stats:#?}");
            println!(
                "PASS seed {seed}: {} committed entries, {} acked requests, {:.1}s simulated in {:.2}s",
                stats.committed_entries,
                stats.requests_acked,
                stats.sim_time_us as f64 / 1e6,
                started.elapsed().as_secs_f64()
            );
            if args.check_determinism {
                let again = run_seed(seed, opts).expect("second run of a passing seed failed");
                if again.trace_hash != stats.trace_hash {
                    println!("NONDETERMINISM: trace hash differs between runs");
                    return ExitCode::FAILURE;
                }
                println!(
                    "determinism check passed (trace hash {:016x})",
                    stats.trace_hash
                );
            }
            ExitCode::SUCCESS
        }
        Err(f) => {
            println!("FAIL {f}");
            ExitCode::FAILURE
        }
    }
}

#[derive(Default)]
struct Totals {
    crashes: u64,
    lost_writes: u64,
    torn: u64,
    flips: u64,
    dropped: u64,
    duplicated: u64,
    network_changes: u64,
    clock_jumps: u64,
    committed: u64,
    acked: u64,
}

impl Totals {
    fn add(&mut self, s: &sim::Stats) {
        self.crashes += s.crashes;
        self.lost_writes += s.lost_unsynced_writes;
        self.torn += s.torn_writes;
        self.flips += s.bit_flips;
        self.dropped += s.messages_dropped;
        self.duplicated += s.messages_duplicated;
        self.network_changes += s.network_changes;
        self.clock_jumps += s.clock_jumps;
        self.committed += s.committed_entries;
        self.acked += s.requests_acked;
    }
}

fn run_many(args: &Args) -> ExitCode {
    let start = args.start.unwrap_or_else(|| {
        if args.duration.is_some() {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
                & 0xFFFF_FFFF_FFFF
        } else {
            0
        }
    });
    let limit = args.seeds.map(|n| start.saturating_add(n));
    let deadline = args.duration.map(|d| Instant::now() + d);
    // Panics are converted to failures; keep stderr quiet.
    std::panic::set_hook(Box::new(|_| {}));

    let next = AtomicU64::new(start);
    let passed = AtomicU64::new(0);
    let sim_us = AtomicU64::new(0);
    let totals = Mutex::new(Totals::default());
    let failures: Mutex<Vec<Failure>> = Mutex::new(Vec::new());
    let began = Instant::now();
    let opts = Options {
        bugs: args.bugs,
        verbose: false,
    };
    println!(
        "VOPR: start seed {start}, {} threads, {}",
        args.threads,
        match (limit, args.duration) {
            (Some(l), _) => format!("{} seeds", l - start),
            (None, Some(d)) => format!("for {:?}", d),
            _ => unreachable!(),
        }
    );
    std::thread::scope(|s| {
        for _ in 0..args.threads {
            s.spawn(|| loop {
                if deadline.is_some_and(|d| Instant::now() >= d) {
                    break;
                }
                let seed = next.fetch_add(1, Ordering::Relaxed);
                if limit.is_some_and(|l| seed >= l) {
                    break;
                }
                match run_seed(seed, opts.clone()) {
                    Ok(stats) => {
                        if args.check_determinism {
                            match run_seed(seed, opts.clone()) {
                                Ok(again) if again.trace_hash == stats.trace_hash => {}
                                _ => {
                                    failures.lock().unwrap().push(Failure {
                                        seed,
                                        time_us: 0,
                                        message: "nondeterministic: second run differs".into(),
                                    });
                                    continue;
                                }
                            }
                        }
                        passed.fetch_add(1, Ordering::Relaxed);
                        sim_us.fetch_add(stats.sim_time_us, Ordering::Relaxed);
                        totals.lock().unwrap().add(&stats);
                    }
                    Err(f) => {
                        println!("FAIL {f}");
                        failures.lock().unwrap().push(f);
                    }
                }
            });
        }
        s.spawn(|| {
            let mut last = Instant::now();
            loop {
                std::thread::sleep(Duration::from_millis(200));
                let done = limit.is_some_and(|l| next.load(Ordering::Relaxed) >= l)
                    || deadline.is_some_and(|d| Instant::now() >= d);
                if done {
                    break;
                }
                if last.elapsed() >= Duration::from_secs(10) {
                    last = Instant::now();
                    let p = passed.load(Ordering::Relaxed);
                    let f = failures.lock().unwrap().len();
                    println!(
                        "  {:>6.0}s  passed {p}  failed {f}  ({:.0} seeds/s)",
                        began.elapsed().as_secs_f64(),
                        (p as f64 + f as f64) / began.elapsed().as_secs_f64()
                    );
                }
            }
        });
    });
    let _ = std::panic::take_hook();
    let failures = failures.into_inner().unwrap();
    let p = passed.load(Ordering::Relaxed);
    let elapsed = began.elapsed().as_secs_f64();
    println!(
        "VOPR done: {} seeds run, {p} passed, {} failed in {elapsed:.1}s ({:.0} seeds/s, {:.1} hours of simulated cluster time)",
        p + failures.len() as u64,
        failures.len(),
        (p as f64 + failures.len() as f64) / elapsed,
        sim_us.load(Ordering::Relaxed) as f64 / 3.6e9
    );
    let t = totals.into_inner().unwrap();
    println!(
        "faults injected across passing seeds: {} crashes, {} unsynced writes lost, {} torn writes, {} bit flips, \
         {} messages dropped, {} duplicated, {} network changes, {} clock jumps; {} entries committed, {} requests acked",
        t.crashes, t.lost_writes, t.torn, t.flips, t.dropped, t.duplicated, t.network_changes, t.clock_jumps, t.committed, t.acked
    );
    if failures.is_empty() {
        ExitCode::SUCCESS
    } else {
        let mut seeds: Vec<u64> = failures.iter().map(|f| f.seed).collect();
        seeds.sort_unstable();
        println!("failing seeds: {:?}", &seeds[..seeds.len().min(50)]);
        ExitCode::FAILURE
    }
}

fn main() -> ExitCode {
    let args = parse_args();
    match (args.seed, args.seeds, args.duration) {
        (Some(seed), None, None) => run_one(seed, &args),
        (None, Some(_), _) | (None, None, Some(_)) => run_many(&args),
        _ => usage(),
    }
}
