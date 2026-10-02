//! Benchmarks.
//!
//! ```text
//! bench sm      [--transfers 2000000] [--accounts 10000]
//! bench fsync   [--dir .bench] [--count 2000]
//! bench cluster [--clients 8] [--batch 128] [--seconds 20] [--accounts 10000] [--dir .bench]
//! ```
//!
//! All numbers are printed as one line of `key=value` pairs per run so they can
//! be pasted into the README verbatim.

use std::io::Write;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use consensus::prng::Prng;
use consensus::{Entry, Operation, Payload, Request, StateMachine};
use ledger::{NewAccount, NewTransfer, ResultCode};
use server::client::Client;

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn us(d: Duration) -> String {
    format!("{:.1}", d.as_secs_f64() * 1e6)
}

struct Args(Vec<String>);

impl Args {
    fn get<T: std::str::FromStr>(&self, name: &str, default: T) -> T {
        self.0
            .iter()
            .position(|a| a == name)
            .and_then(|i| self.0.get(i + 1))
            .map(|v| v.parse().unwrap_or_else(|_| panic!("bad value for {name}")))
            .unwrap_or(default)
    }
}

fn accounts(n: u64) -> Vec<NewAccount> {
    (1..=n)
        .map(|id| NewAccount {
            id: u128::from(id),
            ledger: 1,
            code: 1,
            flags: 0,
        })
        .collect()
}

fn random_transfer(rng: &mut Prng, id: u128, accounts: u64) -> NewTransfer {
    let dr = 1 + rng.below(accounts);
    let mut cr = 1 + rng.below(accounts);
    if cr == dr {
        cr = dr % accounts + 1;
    }
    NewTransfer {
        id,
        debit_account_id: u128::from(dr),
        credit_account_id: u128::from(cr),
        amount: u128::from(1 + rng.below(1000)),
        ledger: 1,
        code: 1,
        ..Default::default()
    }
}

/// (a) The state machine alone: apply log entries holding one request of
/// `batch` transfers, no I/O, no replication.
fn bench_sm(args: &Args) {
    let total: u64 = args.get("--transfers", 2_000_000);
    let n_accounts: u64 = args.get("--accounts", 10_000);
    for batch in [1usize, 128, 8190] {
        let mut sm = StateMachine::new();
        let mut ts = 1u64;
        let mut index = 0u64;
        for chunk in accounts(n_accounts).chunks(8190) {
            index += 1;
            sm.apply(&Entry {
                term: 1,
                index,
                timestamp: ts,
                payload: Payload::Batch(vec![Request {
                    client_id: 1,
                    request_number: index,
                    operation: Operation::CreateAccounts(chunk.to_vec()),
                }]),
            });
            ts += 1;
        }
        let mut rng = Prng::new(7);
        let entries: Vec<Entry> = (0..total / batch as u64)
            .map(|i| {
                index += 1;
                ts += 1;
                let transfers = (0..batch)
                    .map(|j| random_transfer(&mut rng, u128::from(i) * 10_000 + j as u128 + 1, n_accounts))
                    .collect();
                Entry {
                    term: 1,
                    index,
                    timestamp: ts,
                    payload: Payload::Batch(vec![Request {
                        client_id: 1,
                        request_number: index,
                        operation: Operation::CreateTransfers(transfers),
                    }]),
                }
            })
            .collect();
        let mut lat = Vec::with_capacity(entries.len());
        let start = Instant::now();
        for e in &entries {
            let t = Instant::now();
            let out = sm.apply(e);
            lat.push(t.elapsed());
            std::hint::black_box(out);
        }
        let elapsed = start.elapsed();
        lat.sort_unstable();
        let applied = entries.len() * batch;
        sm.ledger.check_invariants().expect("ledger invariants after bench");
        println!(
            "bench=sm batch={batch} transfers={applied} seconds={:.3} transfers_per_sec={:.0} entry_p50_us={} entry_p99_us={}",
            elapsed.as_secs_f64(),
            applied as f64 / elapsed.as_secs_f64(),
            us(percentile(&lat, 0.50)),
            us(percentile(&lat, 0.99)),
        );
    }
}

/// Raw `write + fdatasync` latency of the disk the WAL lives on; it bounds
/// commit latency of the cluster.
fn bench_fsync(args: &Args) {
    let dir = PathBuf::from(args.get("--dir", ".bench".to_string()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("fsync-probe.bin");
    let count: usize = args.get("--count", 2000);
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap();
    let block = vec![0xA5u8; 4096];
    let mut lat = Vec::with_capacity(count);
    for _ in 0..count {
        let t = Instant::now();
        f.write_all(&block).unwrap();
        f.sync_data().unwrap();
        lat.push(t.elapsed());
    }
    std::fs::remove_file(&path).unwrap();
    lat.sort_unstable();
    println!(
        "bench=fsync count={count} p50_us={} p99_us={} max_us={}",
        us(percentile(&lat, 0.5)),
        us(percentile(&lat, 0.99)),
        us(*lat.last().unwrap())
    );
}

struct Servers {
    children: Vec<Child>,
    dir: PathBuf,
}

impl Drop for Servers {
    fn drop(&mut self) {
        for c in &mut self.children {
            let _ = c.kill();
            let _ = c.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn server_binary() -> PathBuf {
    let exe = std::env::current_exe().unwrap();
    let bin = exe.parent().unwrap().join("quorum-ledger-server");
    assert!(
        bin.exists(),
        "{} not found; run `cargo build --release -p server` first",
        bin.display()
    );
    bin
}

fn start_cluster(dir: &Path, n: usize) -> (Servers, Vec<SocketAddr>) {
    let addrs: Vec<SocketAddr> = (0..n)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap())
        .collect();
    let list: Vec<String> = addrs.iter().map(|a| a.to_string()).collect();
    std::fs::create_dir_all(dir).unwrap();
    let bin = server_binary();
    let children = (0..n)
        .map(|i| {
            Command::new(&bin)
                .args(["--id", &i.to_string(), "--cluster", &list.join(","), "--data"])
                .arg(dir)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap()
        })
        .collect();
    (
        Servers {
            children,
            dir: dir.to_path_buf(),
        },
        addrs,
    )
}

/// (b) A 3-process cluster on localhost: real TCP, real WAL with fdatasync.
fn bench_cluster(args: &Args) {
    let clients: usize = args.get("--clients", 8);
    let batch: usize = args.get("--batch", 128);
    let seconds: u64 = args.get("--seconds", 20);
    let warmup: u64 = args.get("--warmup", 3);
    let n_accounts: u64 = args.get("--accounts", 10_000);
    let replicas: usize = args.get("--replicas", 3);
    let base = PathBuf::from(args.get("--dir", ".bench".to_string()));
    let dir = base.join(format!("cluster-{}", std::process::id()));
    let (_servers, addrs) = start_cluster(&dir, replicas);

    let mut setup = Client::new(addrs.clone());
    for chunk in accounts(n_accounts).chunks(8190) {
        let r = setup.create_accounts(chunk.to_vec()).expect("create accounts");
        assert!(r.iter().all(|c| *c == ResultCode::Ok));
    }

    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let handles: Vec<_> = (0..clients)
        .map(|c| {
            let addrs = addrs.clone();
            let stop = stop.clone();
            let measuring = measuring.clone();
            std::thread::spawn(move || {
                let mut client = Client::new(addrs);
                let mut rng = Prng::new(c as u64 + 1);
                let mut next_id = 0u128;
                let mut lat = Vec::new();
                let mut transfers = 0u64;
                let mut failed = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let batch_transfers: Vec<NewTransfer> = (0..batch)
                        .map(|_| {
                            next_id += 1;
                            random_transfer(&mut rng, ((c as u128 + 1) << 64) | next_id, n_accounts)
                        })
                        .collect();
                    let t = Instant::now();
                    let results = client.create_transfers(batch_transfers).expect("request failed");
                    let d = t.elapsed();
                    if measuring.load(Ordering::Relaxed) {
                        lat.push(d);
                        transfers += results.len() as u64;
                        failed += results.iter().filter(|r| **r != ResultCode::Ok).count() as u64;
                    }
                }
                (lat, transfers, failed)
            })
        })
        .collect();
    std::thread::sleep(Duration::from_secs(warmup));
    measuring.store(true, Ordering::Relaxed);
    let t0 = Instant::now();
    std::thread::sleep(Duration::from_secs(seconds));
    measuring.store(false, Ordering::Relaxed);
    let elapsed = t0.elapsed();
    stop.store(true, Ordering::Relaxed);
    let mut lat = Vec::new();
    let mut transfers = 0;
    let mut failed = 0;
    for h in handles {
        let (l, t, f) = h.join().unwrap();
        lat.extend(l);
        transfers += t;
        failed += f;
    }
    lat.sort_unstable();
    let check = setup.lookup_accounts((1..=n_accounts.min(8190)).map(u128::from).collect()).unwrap();
    std::hint::black_box(check);
    println!(
        "bench=cluster replicas={replicas} clients={clients} batch={batch} seconds={:.1} transfers={transfers} failed={failed} \
         transfers_per_sec={:.0} requests_per_sec={:.0} request_p50_us={} request_p99_us={} request_p999_us={}",
        elapsed.as_secs_f64(),
        transfers as f64 / elapsed.as_secs_f64(),
        lat.len() as f64 / elapsed.as_secs_f64(),
        us(percentile(&lat, 0.50)),
        us(percentile(&lat, 0.99)),
        us(percentile(&lat, 0.999)),
    );
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some(cmd) = argv.first().cloned() else {
        eprintln!("usage: bench (sm|fsync|cluster) [options]");
        std::process::exit(2);
    };
    let args = Args(argv);
    match cmd.as_str() {
        "sm" => bench_sm(&args),
        "fsync" => bench_fsync(&args),
        "cluster" => bench_cluster(&args),
        _ => {
            eprintln!("unknown benchmark {cmd}");
            std::process::exit(2);
        }
    }
}
