//! quorum-ledger-server --id 0 --cluster 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --data ./data

use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use consensus::Config;
use server::node::{run, NodeConfig};

fn usage() -> ! {
    eprintln!(
        "usage: quorum-ledger-server --id N --cluster host:port,host:port,... --data DIR \
         [--tick-ms 10] [--max-requests-per-entry 64]"
    );
    std::process::exit(2);
}

fn main() -> ExitCode {
    let mut id: Option<u8> = None;
    let mut cluster: Vec<SocketAddr> = Vec::new();
    let mut data: Option<PathBuf> = None;
    let mut tick_ms = 10u64;
    let mut raft = Config {
        // Bounds an AppendEntries to ~4 x 8190 transfers (~3 MB).
        max_entries_per_message: 4,
        // With 10 ms ticks: heartbeat every 50 ms, election timeout 0.5-1 s.
        // The event loop fsyncs inline, so on a shared VPS a few hundred ms
        // of stall under load is normal and must not trigger elections.
        heartbeat_ticks: 5,
        election_timeout_min_ticks: 50,
        election_timeout_max_ticks: 100,
        ..Config::default()
    };
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = || args.next().unwrap_or_else(|| usage());
        match a.as_str() {
            "--id" => id = Some(val().parse().unwrap_or_else(|_| usage())),
            "--cluster" => {
                cluster = val()
                    .split(',')
                    .map(|s| {
                        s.to_socket_addrs()
                            .ok()
                            .and_then(|mut it| it.next())
                            .unwrap_or_else(|| usage())
                    })
                    .collect()
            }
            "--data" => data = Some(PathBuf::from(val())),
            "--tick-ms" => tick_ms = val().parse().unwrap_or_else(|_| usage()),
            "--max-requests-per-entry" => {
                raft.max_requests_per_entry = val().parse().unwrap_or_else(|_| usage())
            }
            _ => usage(),
        }
    }
    let (Some(id), Some(data)) = (id, data) else {
        usage()
    };
    if cluster.is_empty() || id as usize >= cluster.len() || cluster.len() > 16 {
        usage();
    }
    raft.replica_count = cluster.len() as u8;
    let cfg = NodeConfig {
        id,
        cluster,
        data_dir: data,
        tick: Duration::from_millis(tick_ms),
        raft,
    };
    match run(cfg) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("fatal: {e}");
            ExitCode::FAILURE
        }
    }
}
