//! One replica process.
//!
//! Threading model (deliberately simple, std only):
//!
//! * one **event-loop thread** owns the [`Replica`]; it is the only code that
//!   touches consensus or ledger state, so the replica stays single-threaded
//!   exactly as in the simulator;
//! * one **acceptor thread** plus one **reader thread per inbound
//!   connection** decode frames and forward them over a channel;
//! * one **writer thread per outbound peer** keeps a connection to that peer
//!   (reconnecting with backoff) and one writer thread per client connection.
//!
//! Every loop iteration drains all queued input, lets the leader cut a batch
//! (`prepare`), ships leader AppendEntries immediately, then performs a single
//! `fdatasync` for everything written in the iteration (group commit) and
//! finally releases the messages that were waiting for durability.

use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use consensus::{Config, FileDevice, Frame, Message, Outgoing, Replica, Reply, Request, Wal};

use crate::framing::{read_frame, write_frame};

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: u8,
    /// Addresses of all replicas, indexed by replica id.
    pub cluster: Vec<SocketAddr>,
    pub data_dir: PathBuf,
    pub tick: Duration,
    pub raft: Config,
}

enum Input {
    Peer { from: u8, msg: Message },
    ClientConnected { conn: u64, tx: Sender<Vec<u8>> },
    ClientRequest { conn: u64, req: Request },
    ClientGone { conn: u64 },
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Runs a replica until the process is killed. Returns only on startup errors.
pub fn run(cfg: NodeConfig) -> io::Result<()> {
    let wal_path = cfg.data_dir.join(format!("replica-{}.wal", cfg.id));
    let storage = Wal::new(FileDevice::open(&wal_path)?);
    let mut replica = Replica::open(cfg.id, cfg.raft.clone(), storage, now_ns())?;
    eprintln!(
        "replica {} recovered: term {}, {} log entries, wal {}",
        cfg.id,
        replica.term(),
        replica.last_index(),
        wal_path.display()
    );

    let listener = TcpListener::bind(cfg.cluster[cfg.id as usize])?;
    let (in_tx, in_rx) = mpsc::channel::<Input>();

    // Outbound peer links.
    let mut peer_tx: Vec<Option<Sender<Vec<u8>>>> = Vec::new();
    for (p, addr) in cfg.cluster.iter().enumerate() {
        if p == cfg.id as usize {
            peer_tx.push(None);
            continue;
        }
        let (tx, rx) = mpsc::channel::<Vec<u8>>();
        let addr = *addr;
        let me = cfg.id;
        thread::Builder::new()
            .name(format!("peer-out-{p}"))
            .spawn(move || peer_writer(me, addr, rx))?;
        peer_tx.push(Some(tx));
    }

    {
        let in_tx = in_tx.clone();
        let n = cfg.cluster.len() as u8;
        thread::Builder::new()
            .name("acceptor".into())
            .spawn(move || acceptor(listener, n, in_tx))?;
    }
    drop(in_tx);

    event_loop(&cfg, &mut replica, in_rx, &peer_tx);
    Ok(())
}

fn event_loop(
    cfg: &NodeConfig,
    replica: &mut Replica<Wal<FileDevice>>,
    rx: Receiver<Input>,
    peers: &[Option<Sender<Vec<u8>>>],
) {
    let mut clients: HashMap<u64, Sender<Vec<u8>>> = HashMap::new();
    let mut client_route: HashMap<u128, u64> = HashMap::new();
    let mut next_tick = Instant::now() + cfg.tick;

    let send_reply = |clients: &HashMap<u64, Sender<Vec<u8>>>, conn: u64, reply: Reply| {
        if let Some(tx) = clients.get(&conn) {
            let _ = tx.send(Frame::ClientReply(reply).to_wire());
        }
    };

    loop {
        let wait = next_tick.saturating_duration_since(Instant::now());
        let first = match rx.recv_timeout(wait) {
            Ok(i) => Some(i),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => return,
        };
        replica.set_clock(now_ns());
        let mut handled = 0;
        let mut input = first;
        while let Some(i) = input.take() {
            match i {
                Input::Peer { from, msg } => replica.step(from, msg),
                Input::ClientConnected { conn, tx } => {
                    clients.insert(conn, tx);
                }
                Input::ClientGone { conn } => {
                    clients.remove(&conn);
                }
                Input::ClientRequest { conn, req } => {
                    client_route.insert(req.client_id, conn);
                    if let Some(reply) = replica.submit(req) {
                        send_reply(&clients, conn, reply);
                    }
                }
            }
            handled += 1;
            if handled < 4096 {
                input = rx.try_recv().ok();
            }
        }
        let now = Instant::now();
        while now >= next_tick {
            replica.tick();
            next_tick += cfg.tick;
        }
        replica.prepare();
        // Leader AppendEntries leave before the local fsync (they are not
        // held), so followers write in parallel with the leader.
        dispatch(replica, peers, &clients, &client_route);
        replica.sync();
        dispatch(replica, peers, &clients, &client_route);
    }
}

fn dispatch(
    replica: &mut Replica<Wal<FileDevice>>,
    peers: &[Option<Sender<Vec<u8>>>],
    clients: &HashMap<u64, Sender<Vec<u8>>>,
    route: &HashMap<u128, u64>,
) {
    for o in replica.drain_outgoing() {
        match o {
            Outgoing::Peer { to, msg } => {
                if let Some(Some(tx)) = peers.get(to as usize) {
                    let _ = tx.send(Frame::Peer(msg).to_wire());
                }
            }
            Outgoing::Reply(reply) => {
                if let Some(tx) = route.get(&reply.client_id).and_then(|c| clients.get(c)) {
                    let _ = tx.send(Frame::ClientReply(reply).to_wire());
                }
            }
        }
    }
}

fn acceptor(listener: TcpListener, replica_count: u8, tx: Sender<Input>) {
    let mut next_conn = 1u64;
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let _ = stream.set_nodelay(true);
        let conn = next_conn;
        next_conn += 1;
        let tx = tx.clone();
        let _ = thread::Builder::new()
            .name(format!("conn-{conn}"))
            .spawn(move || {
                if let Err(e) = connection_reader(stream, conn, replica_count, tx) {
                    if e.kind() != io::ErrorKind::UnexpectedEof {
                        eprintln!("connection {conn} closed: {e}");
                    }
                }
            });
    }
}

fn connection_reader(
    stream: TcpStream,
    conn: u64,
    replica_count: u8,
    tx: Sender<Input>,
) -> io::Result<()> {
    let mut reader = BufReader::with_capacity(1 << 16, stream.try_clone()?);
    let hello = read_frame(&mut reader)?;
    match hello {
        Frame::Hello {
            is_peer: true,
            replica_id,
        } if replica_id < replica_count => loop {
            match read_frame(&mut reader)? {
                Frame::Peer(msg) => {
                    if tx
                        .send(Input::Peer {
                            from: replica_id,
                            msg,
                        })
                        .is_err()
                    {
                        return Ok(());
                    }
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "peer sent non-peer frame",
                    ))
                }
            }
        },
        Frame::Hello { is_peer: false, .. } => {
            let (out_tx, out_rx) = mpsc::channel::<Vec<u8>>();
            let write_half = stream.try_clone()?;
            thread::Builder::new()
                .name(format!("client-out-{conn}"))
                .spawn(move || frame_writer(write_half, out_rx))?;
            if tx
                .send(Input::ClientConnected { conn, tx: out_tx })
                .is_err()
            {
                return Ok(());
            }
            let result = loop {
                match read_frame(&mut reader) {
                    Ok(Frame::ClientRequest(req)) => {
                        if tx.send(Input::ClientRequest { conn, req }).is_err() {
                            break Ok(());
                        }
                    }
                    Ok(_) => {
                        break Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "client sent unexpected frame",
                        ))
                    }
                    Err(e) => break Err(e),
                }
            };
            let _ = tx.send(Input::ClientGone { conn });
            let _ = stream.shutdown(std::net::Shutdown::Both);
            result
        }
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "expected hello")),
    }
}

/// Writes queued frames, coalescing everything already queued into one flush.
fn frame_writer(stream: TcpStream, rx: Receiver<Vec<u8>>) {
    let mut w = BufWriter::with_capacity(1 << 16, stream);
    while let Ok(buf) = rx.recv() {
        if w.write_all(&buf).is_err() {
            return;
        }
        while let Ok(buf) = rx.try_recv() {
            if w.write_all(&buf).is_err() {
                return;
            }
        }
        if w.flush().is_err() {
            return;
        }
    }
}

/// Maintains the outbound connection to one peer. Messages queued while the
/// peer is unreachable are dropped; Raft retransmits on its own.
fn peer_writer(me: u8, addr: SocketAddr, rx: Receiver<Vec<u8>>) {
    let mut backoff = Duration::from_millis(20);
    loop {
        let stream = match TcpStream::connect_timeout(&addr, Duration::from_millis(500)) {
            Ok(s) => s,
            Err(_) => {
                // Drain (drop) whatever piled up while disconnected.
                while rx.try_recv().is_ok() {}
                thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_millis(500));
                if matches!(rx.try_recv(), Err(mpsc::TryRecvError::Disconnected)) {
                    return;
                }
                continue;
            }
        };
        backoff = Duration::from_millis(20);
        let _ = stream.set_nodelay(true);
        let mut w = BufWriter::with_capacity(1 << 16, stream);
        if write_frame(
            &mut w,
            &Frame::Hello {
                is_peer: true,
                replica_id: me,
            },
        )
        .is_err()
            || w.flush().is_err()
        {
            continue;
        }
        'conn: while let Ok(buf) = rx.recv() {
            if w.write_all(&buf).is_err() {
                break 'conn;
            }
            while let Ok(buf) = rx.try_recv() {
                if w.write_all(&buf).is_err() {
                    break 'conn;
                }
            }
            if w.flush().is_err() {
                break 'conn;
            }
        }
    }
}
